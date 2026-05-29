"""
HTTP client for the Zoho People API.

Zoho People has the tightest documented rate limits in the Zoho stack:
30 requests per 5-minute window for most read endpoints (forms, holidays,
attendance entries), 100 requests per 5 minutes for the bulk
`getUserReport` / `gettimelogs`, 300 / 5 min for `leavetracker/leaves/records`,
and 20 / 5 min for `getjobs` / `gettimesheetdetails`.

Our limiter uses a per-endpoint sliding window. We pick a conservative
PER_ENDPOINT_LIMIT of 25 in a 5-minute window (under the strictest
documented 30/5min), which keeps every endpoint safe without needing
per-endpoint overrides. A global 200/5min ceiling guards against bursts
when many endpoints fire concurrently.

Endpoint paths in Zoho People mix two prefixes:
  * `/people/api/...`   — legacy v1/v2 surface (forms list, attendance,
                          leave, timesheet, files, holidays)
  * `/api/...`          — newer "Forms API" surface (`/api/forms/{view}/records`,
                          `/api/forms/{form}/views`, `/api/views`)

Both share the same host (`people.zoho.{dc}`) and the same Authorization
header, so this module treats them uniformly — callers just hand in the
full URL.
"""

import re as _re
import time

import requests
from fivetran_connector_sdk import Logging as log

from auth import api_host, get_access_token, get_headers


class DailyLimitExceeded(Exception):
    """Raised when Zoho indicates the per-day API quota is exhausted, or
    when Retry-After implies a many-hour wait. Caught by `update()` to
    checkpoint gracefully and exit; Fivetran will resume from the last
    checkpoint on the next scheduled run."""


class ScopeMissing(Exception):
    """Raised when Zoho rejects the request with an oauthscope error.
    Token refresh won't help — caller decides what to do (typically:
    skip the endpoint and continue)."""


class ZohoPeopleApiError(Exception):
    """Raised on logical (non-HTTP) failures returned in the response
    envelope (`response.status != 0` or `errorcode` set)."""


# Permanent client-side error codes that Zoho People occasionally wraps in
# HTTP 5xx responses. Retrying these wastes the (very tight) rate budget.
# Source: per-endpoint docs (timesheet, leavetracker, attendance modules).
_PERMANENT_ERROR_CODES = {
    9001,  # Date period > 1 year (or similar window-too-wide)
    9002,  # No parameters / required param missing
    9004,  # Date format error
    9006,  # toDate before fromDate
    9007,  # Month limit exceeded
    9009,  # Permission denied
    7012,  # Invalid view name
    7042,  # Invalid search value
    7150,  # Endpoint not supported for this resource
}


def _detect_permanent_error(body_text: str) -> tuple:
    """Return `(code, message)` if the body contains a permanent error
    code from `_PERMANENT_ERROR_CODES`, otherwise `(None, None)`."""
    if not body_text:
        return (None, None)
    for code in _PERMANENT_ERROR_CODES:
        needle = f'"code":{code}'
        if needle in body_text or f'"errorcode":{code}' in body_text:
            return (code, body_text[:300])
    return (None, None)


# ── Rate limiter ─────────────────────────────────────────────────────────────
# Zoho People documents most limits as "X requests | 5 minute lock period".
# We treat that as a sliding 5-minute window with a small safety buffer.
PER_ENDPOINT_LIMIT = 25            # <30/5min documented floor
GLOBAL_LIMIT_PER_5MIN = 200        # soft IP-wide ceiling
WINDOW_SECONDS = 300               # 5 minutes


_per_endpoint_timestamps: dict = {}
_global_timestamps: list = []
_token_endpoint_rate_limited: list = [False]


def reset_rate_limiter():
    global _per_endpoint_timestamps, _global_timestamps
    _per_endpoint_timestamps = {}
    _global_timestamps = []
    _token_endpoint_rate_limited[0] = False


def token_endpoint_rate_limited() -> bool:
    return _token_endpoint_rate_limited[0]


def mark_token_endpoint_rate_limited():
    _token_endpoint_rate_limited[0] = True


# Strip numeric path components so /forms/abc/records and /forms/def/records
# share the same bucket only when {abc, def} differ — we want PER FORM
# buckets because each one gets its own 30/5min quota. So we DON'T normalise
# alphanumeric path segments. Only normalise long numeric IDs (job_id, leave_id).
_PATH_NORMALIZE_RE = _re.compile(r"/\d{6,}")


def _endpoint_key(url: str) -> str:
    path = url.split("?", 1)[0]
    if "://" in path:
        path = path.split("/", 3)[-1]
    path = "/" + path if not path.startswith("/") else path
    return _PATH_NORMALIZE_RE.sub("/{id}", path)


def _wait_for_rate_limit(url: str):
    global _per_endpoint_timestamps, _global_timestamps
    now = time.time()
    key = _endpoint_key(url)

    bucket = _per_endpoint_timestamps.get(key, [])
    bucket = [t for t in bucket if now - t < WINDOW_SECONDS]
    if len(bucket) >= PER_ENDPOINT_LIMIT:
        sleep_time = WINDOW_SECONDS - (now - bucket[0]) + 1
        log.info(f"Per-endpoint limit ({PER_ENDPOINT_LIMIT}/{WINDOW_SECONDS}s) "
                 f"reached for {key} — sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
        now = time.time()
        bucket = [t for t in bucket if now - t < WINDOW_SECONDS]
    bucket.append(now)
    _per_endpoint_timestamps[key] = bucket

    _global_timestamps = [t for t in _global_timestamps if now - t < WINDOW_SECONDS]
    if len(_global_timestamps) >= GLOBAL_LIMIT_PER_5MIN:
        sleep_time = WINDOW_SECONDS - (now - _global_timestamps[0]) + 1
        log.info(f"Global limit ({GLOBAL_LIMIT_PER_5MIN}/{WINDOW_SECONDS}s) "
                 f"reached — sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
        now = time.time()
        _global_timestamps = [t for t in _global_timestamps if now - t < WINDOW_SECONDS]
    _global_timestamps.append(now)


# ── Single request ──────────────────────────────────────────────────────────
def api_request(config: dict, url: str, params: dict = None,
                form_data: dict = None, json_body: dict = None,
                headers_extra: dict = None, max_retries: int = 5,
                method: str = "GET"):
    """Single HTTP request with rate limiting and retry on 429/5xx.

    Returns parsed JSON, or `{}` on 404. A 401 triggers exactly one token
    refresh + retry before bubbling up.

    Zoho People wraps successful responses in different envelopes per
    module — this method does NOT unwrap. Callers know the expected shape
    and dig into it themselves (e.g. `result = body['response']['result']`
    for the bulk endpoints, plain dict for the forms records endpoint).
    """
    refreshed_once = False

    for attempt in range(max_retries):
        _wait_for_rate_limit(url)
        headers = get_headers(config, extra=headers_extra)

        try:
            response = requests.request(
                method, url,
                headers=headers,
                params=params,
                data=form_data,
                json=json_body,
                timeout=60,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt * 2
                log.warning(f"Network error for {url}: {exc}. Retry in {wait}s")
                time.sleep(wait)
                continue
            raise

        sc = response.status_code

        if sc in (200, 201):
            try:
                return response.json()
            except ValueError:
                log.severe(f"Non-JSON 200 from {url} (len={len(response.content)})")
                raise

        if sc == 204:
            return {}

        if sc == 404:
            log.fine(f"404 for {url} — returning empty")
            return {}

        if sc in (401, 403):
            body_text = response.text or ""
            body_lower = body_text.lower()
            # Scope-error fingerprints across Zoho's various error envelopes.
            # 7103 = generic Zoho oauthscope; 7218 = Zoho People-specific
            # "Invalid OAuth Scope" returned for endpoints whose scope is
            # missing from the refresh_token (files, dashboard, etc.).
            scope_error = (
                "oauthscope" in body_lower
                or "invalid oauth scope" in body_lower
                or "invalid scope" in body_lower
                or '"code":7103' in body_text
                or '"errorcode":7103' in body_text
                or '"code":7218' in body_text
                or '"errorcode":7218' in body_text
            )
            if scope_error:
                log.warning(
                    f"Scope error for {url}: {body_text[:200]}. "
                    f"Token refresh would not help — surfacing as ScopeMissing."
                )
                raise ScopeMissing(body_text[:300])
            if sc == 401 and not refreshed_once:
                log.warning(f"401 for {url} — refreshing token and retrying once")
                from auth import reset_caches as _reset_auth
                _reset_auth()
                get_access_token(config)
                refreshed_once = True
                continue
            log.severe(f"Auth error {sc} for {url}: {body_text[:300]}")
            response.raise_for_status()

        if sc == 429:
            # Zoho People's 5-minute lockout sometimes returns Retry-After in
            # seconds, sometimes doesn't. Default to a full 5-min wait.
            MAX_RETRY_AFTER = 600  # 10 min — anything longer = treat as daily exhaustion
            retry_after = int(response.headers.get("Retry-After") or "300")
            if retry_after > MAX_RETRY_AFTER:
                hours = retry_after / 3600
                raise DailyLimitExceeded(
                    f"Zoho daily API quota exhausted. "
                    f"Retry-After={retry_after}s (~{hours:.1f} hours). "
                    f"Checkpointing and aborting — Fivetran will resume on "
                    f"the next scheduled run. URL: {url}"
                )
            log.warning(f"429 for {url}. Sleeping {retry_after}s")
            time.sleep(retry_after)
            continue

        if sc >= 500:
            # Some Zoho People endpoints wrap permanent client errors
            # (date-window too wide, bad format, etc.) inside HTTP 5xx.
            # Don't waste retries on those — raise immediately so the
            # caller can react (e.g. by chunking the window).
            body_text = response.text or ""
            perm_code, perm_msg = _detect_permanent_error(body_text)
            if perm_code is not None:
                log.severe(
                    f"Permanent error code {perm_code} on {url} "
                    f"(HTTP {sc}, not retrying): {perm_msg}"
                )
                raise ZohoPeopleApiError(
                    f"{url}: permanent error {perm_code} — {perm_msg}"
                )
            wait = 2 ** attempt * 2
            log.warning(f"Server error {sc} for {url}. Retry in {wait}s. "
                        f"Body: {body_text[:200]}")
            time.sleep(wait)
            continue

        # Unexpected status — log and raise
        log.severe(f"API error {sc} for {url}: {response.text[:500]}")
        response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


def unwrap_envelope(body: dict, url: str) -> dict:
    """Unwrap a Zoho People legacy envelope of shape:

        {"response": {"result": ..., "message": ..., "status": 0|N, "errorcode": ...}}

    Returns the inner content. Raises ZohoPeopleApiError on a non-zero
    status with an actionable code. Returns `{}` (or `[]`) if the envelope
    indicates empty.

    Newer `/api/forms/{view}/records` endpoint returns a raw list at the
    top level — callers should bypass this helper for that shape.
    """
    if not isinstance(body, dict):
        return body
    inner = body.get("response") if "response" in body else body
    if not isinstance(inner, dict):
        return body
    status = inner.get("status")
    err_code = inner.get("errorcode")
    if status not in (None, 0, "0") or err_code not in (None, 0, "0"):
        msg = inner.get("message") or inner.get("errormessage") or inner
        # 7103 = unauthorized / oauth scope
        # 7401 = no permission
        # 9000 = generic permission denied
        if str(err_code) in ("7103",) or "oauthscope" in str(msg).lower():
            raise ScopeMissing(f"{url}: {msg}")
        raise ZohoPeopleApiError(
            f"Zoho People API rejected {url}: status={status}, "
            f"errorcode={err_code}, message={msg}"
        )
    return inner


# ── Generic offset-paginated reader ─────────────────────────────────────────
def paginate(config: dict, url: str, page_size: int,
             extract: callable,
             start_at: int = 0,
             page_param: str = "sIndex",
             limit_param: str = "limit",
             extra_params: dict = None,
             max_pages: int = 10_000,
             method: str = "GET",
             form_data_template: dict = None):
    """Generic offset/limit pagination.

    `extract(body) -> (records_list, is_last_page_flag_or_none)`. Returning
    `(None, _)` is treated as a permanent stop; returning `([], _)` is
    treated as "no records on this page, stop".

    Iteration stops when:
      - extract returns None or [],
      - extract returns fewer rows than page_size,
      - extract returns is_last_page == True,
      - max_pages reached.

    The legacy bulk endpoints use `sIndex=1` (1-indexed) with `limit`; the
    `attendance/getUserReport` endpoint uses `startIndex=0` (0-indexed) with
    100-row pages; the leave-records endpoint uses `startIndex=0` with
    `limit`. Callers configure these via the `*_param` kwargs and
    `start_at`.
    """
    extras = dict(extra_params or {})
    cur = start_at
    page = 0
    yielded = 0
    while page < max_pages:
        params = dict(extras)
        params[page_param] = cur
        params[limit_param] = page_size

        if method == "POST":
            body = api_request(config, url, form_data=dict(form_data_template or {}, **params),
                               method="POST")
        else:
            body = api_request(config, url, params=params, method="GET")

        try:
            records, is_last = extract(body)
        except ScopeMissing:
            raise
        if records is None:
            return
        for r in records:
            yield r
            yielded += 1
        if not records or len(records) < page_size or is_last:
            log.fine(f"  page {page+1}: yielded {len(records)} (total {yielded}) — stop")
            return
        log.fine(f"  page {page+1}: yielded {len(records)} (total {yielded}) — next")
        cur += page_size
        page += 1
    log.warning(f"paginate({url}): hit max_pages={max_pages} cap at offset {cur}; "
                f"there may be more data — open an issue if so")

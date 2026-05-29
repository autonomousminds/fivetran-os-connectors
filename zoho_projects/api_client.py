"""
HTTP client for the Zoho Projects API.

Zoho Projects documents its rate limit as **200 requests per 2-minute rolling
window, per endpoint, per organisation** (https://projects.zoho.com/api-docs).
Counters are independent per endpoint, so a heavy fan-out on task comments
does not affect the projects-list throughput. Our limiter mirrors that
shape with a small safety buffer.

There are two coexisting URL prefixes, both rooted at the same host:
  * V2 (legacy):  `{host}/restapi/portal/{portal_id}/...`
  * V3 (current): `{host}/api/v3/portal/{portal_id}/...`

`build_url()` is the single helper that builds the right URL for a path +
version combination. Pagination shapes differ between the two:
  * V2: `index` (1-based offset) + `range` (page size, max 200). Stop when
        the returned record count is less than `range`.
  * V3: `page` (1-based) + `per_page`. The response carries a `page_info`
        block with `has_next_page` — use that to terminate.

Both share Authorization (`Zoho-oauthtoken ...`) and the same retry rules.
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


class ZohoProjectsApiError(Exception):
    """Raised on logical (non-HTTP) failures returned in the response
    envelope (Zoho-side rejection on a 200 response)."""


# ── Rate limiter ─────────────────────────────────────────────────────────────
# Zoho Projects publishes 200 reqs / 2-min per endpoint, but several
# high-traffic endpoints (task detail, bug detail, task attachments) enforce
# a tighter 100/2-min ceiling that emits `URL_ROLLING_THROTTLES_LIMIT_EXCEEDED`
# with a multi-minute cooldown. We hold the per-endpoint limit at 90 to stay
# safely under that ceiling; the global cap to 400/2min protects bursts when
# many endpoints fire concurrently.
PER_ENDPOINT_LIMIT = 90
GLOBAL_LIMIT_PER_WINDOW = 400
WINDOW_SECONDS = 120


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


# Normalise long numeric path components (portal IDs, project IDs, task IDs)
# so distinct calls to the same endpoint shape share a rate-limit bucket.
_PATH_NORMALIZE_RE = _re.compile(r"/\d{6,}")

_THROTTLE_WAIT_RE = _re.compile(r"Try again after (\d+) (minute|second)s?",
                                _re.IGNORECASE)


def _parse_throttle_wait(body_text: str):
    """Extract the cooldown duration from a
    URL_ROLLING_THROTTLES_LIMIT_EXCEEDED body. Returns seconds (int) or
    None if the text doesn't carry a parseable duration."""
    m = _THROTTLE_WAIT_RE.search(body_text or "")
    if not m:
        return None
    value = int(m.group(1))
    unit = m.group(2).lower()
    return value * 60 if unit.startswith("minute") else value


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
    if len(_global_timestamps) >= GLOBAL_LIMIT_PER_WINDOW:
        sleep_time = WINDOW_SECONDS - (now - _global_timestamps[0]) + 1
        log.info(f"Global limit ({GLOBAL_LIMIT_PER_WINDOW}/{WINDOW_SECONDS}s) "
                 f"reached — sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
        now = time.time()
        _global_timestamps = [t for t in _global_timestamps if now - t < WINDOW_SECONDS]
    _global_timestamps.append(now)


# ── URL builder ──────────────────────────────────────────────────────────────
def build_url(config: dict, portal_id, path: str, version: str = "v2") -> str:
    """Build a fully-qualified Zoho Projects API URL.

    `version` chooses the prefix:
      * "v2" → `{host}/restapi/portal/{portal_id}{path}` (legacy)
      * "v3" → `{host}/api/v3/portal/{portal_id}{path}` (newer)

    For portal-free endpoints (`/restapi/portals/`) pass `portal_id=None`
    and a `path` already containing the full root (e.g. `/restapi/portals/`).
    """
    host = api_host(config)
    if portal_id is None:
        # Caller knows the full path including the version prefix.
        return f"{host}{path}"
    if version == "v3":
        return f"{host}/api/v3/portal/{portal_id}{path}"
    return f"{host}/restapi/portal/{portal_id}{path}"


# ── Single request ──────────────────────────────────────────────────────────
def api_request(config: dict, url: str, params: dict = None,
                form_data: dict = None, json_body: dict = None,
                headers_extra: dict = None, max_retries: int = 5,
                method: str = "GET"):
    """Single HTTP request with rate limiting and retry on 429/5xx.

    Returns parsed JSON, or `{}` on 404. A 401 triggers exactly one token
    refresh + retry before bubbling up.

    Does NOT unwrap response envelopes — callers know the expected shape for
    each endpoint and dig into it themselves (Zoho Projects uses different
    wrappers per resource; some return bare arrays, some wrap in `{projects:
    [...]}`, some return `{page_info: ..., data: [...]}`).
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
            # Some Zoho Projects endpoints (documents/folders on empty
            # projects) return 200 with a zero-byte body. Treat that as "no
            # rows" instead of a JSON-decode failure.
            if not response.content:
                return {}
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

        # Several Zoho Projects "this is not enabled" cases come back as HTTP
        # 400 with a JSON envelope rather than 403/404. Detect them here so
        # callers can skip the endpoint cleanly instead of crashing.
        # - 6500 (also returned as HTTP 403): "module not available in your
        #   current plan or it has been disabled"
        # - 6891 / "Given URL is wrong": endpoint not supported on this portal
        # - URL_RULE_NOT_CONFIGURED: V3 path not provisioned for this account
        # - URL_ROLLING_THROTTLES_LIMIT_EXCEEDED: per-endpoint rolling lockout
        #   (separate code path, see below)
        if sc == 400:
            body_text = response.text or ""
            if ('"code":6891' in body_text
                    or '"code":6500' in body_text
                    or "URL_RULE_NOT_CONFIGURED" in body_text
                    or "Given URL is wrong" in body_text
                    # 6832: "Input Parameter Does not Match the Pattern
                    # Specified" — `/logs` notoriously rejects valid-looking
                    # param combos depending on plan/portal config (time
                    # tracking module may not be enabled). Treat as
                    # scope-missing so the connector skips silently.
                    or '"code":6832' in body_text):
                log.fine(f"Endpoint not provisioned for this portal "
                         f"({url}) — treating as scope-missing.")
                raise ScopeMissing(body_text[:300])
            if "URL_ROLLING_THROTTLES_LIMIT_EXCEEDED" in body_text:
                # Zoho enforces a tighter per-endpoint rolling lockout
                # (typically 100 reqs / 2 min) on some endpoints. The body
                # carries "Try again after X minutes" — we parse it and
                # either sleep (for short cooldowns) or raise
                # DailyLimitExceeded so the connector checkpoints and the
                # next scheduled run resumes.
                wait_seconds = _parse_throttle_wait(body_text)
                MAX_INLINE_WAIT = 300  # 5 min — anything longer = bail out
                if wait_seconds and wait_seconds <= MAX_INLINE_WAIT:
                    log.warning(f"Rolling throttle on {url}. Sleeping "
                                f"{wait_seconds}s before retry.")
                    time.sleep(wait_seconds)
                    continue
                raise DailyLimitExceeded(
                    f"Zoho per-endpoint rolling-throttle lockout on {url}. "
                    f"Cooldown ~{wait_seconds or 'unknown'}s. Checkpointing "
                    f"and aborting — next scheduled run will resume."
                )

        if sc in (401, 403):
            body_text = response.text or ""
            body_lower = body_text.lower()
            # Scope-error fingerprints across Zoho's various error envelopes.
            # 7103 / 7218 / 2945 are the codes Zoho returns when the
            # refresh_token doesn't include the right scope.
            scope_error = (
                "oauthscope" in body_lower
                or "invalid oauth scope" in body_lower
                or "invalid scope" in body_lower
                or '"code":7103' in body_text
                or '"errorcode":7103' in body_text
                or '"code":7218' in body_text
                or '"errorcode":7218' in body_text
                or '"code":2945' in body_text
                or '"errorcode":2945' in body_text
                # 6500: "module not available in your current plan or it has
                # been disabled" — entitlement, not scope, but the effect is
                # the same (skip endpoint, don't retry).
                or '"code":6500' in body_text
                or '"errorcode":6500' in body_text
                # V3 entitlement envelope — different shape, same meaning:
                #   {"error":{"status_code":"403","title":"FORBIDDEN",
                #             "error_type":"OPERATIONAL_VALIDATION_ERROR",
                #             "details":[{"message":"Enable the leave module to access the APIs."}]}}
                or ('"title":"FORBIDDEN"' in body_text
                    and 'OPERATIONAL_VALIDATION_ERROR' in body_text)
                or ("enable the" in body_lower
                    and "module to access" in body_lower)
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
            MAX_RETRY_AFTER = 600   # >10 min ⇒ treat as quota exhaustion
            retry_after = int(response.headers.get("Retry-After") or "60")
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
            wait = 2 ** attempt * 2
            log.warning(f"Server error {sc} for {url}. Retry in {wait}s. "
                        f"Body: {response.text[:200]}")
            time.sleep(wait)
            continue

        log.severe(f"API error {sc} for {url}: {response.text[:500]}")
        response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


# ── V2 paginator (index/range, offset-based, 1-indexed) ─────────────────────
def paginate_v2(config: dict, url: str, extract_key: str,
                params: dict = None, page_size: int = 200,
                max_pages: int = 10_000):
    """Iterate a V2 list endpoint that uses `index/range` pagination.

    `extract_key` is the JSON key inside the response that holds the list of
    records (e.g. "projects", "tasks", "bugs", "milestones"). If the response
    is already a bare list, pass `extract_key=None`.

    Stops when:
      - the response has no records under the extract key,
      - the returned record count is less than `page_size`,
      - `max_pages` is exhausted.
    """
    extras = dict(params or {})
    index = 1
    page = 0
    yielded = 0
    while page < max_pages:
        call_params = dict(extras)
        call_params["index"] = index
        call_params["range"] = page_size

        body = api_request(config, url, params=call_params)

        if extract_key is None:
            records = body if isinstance(body, list) else []
        elif isinstance(body, dict):
            records = body.get(extract_key) or []
        else:
            records = []

        if not isinstance(records, list):
            log.warning(f"paginate_v2({url}): expected list at "
                        f"`{extract_key}`, got {type(records).__name__}; stopping")
            return

        for r in records:
            yield r
            yielded += 1

        if len(records) < page_size:
            log.fine(f"  v2 page {page + 1}: {len(records)} record(s) "
                     f"(total {yielded}) — last page")
            return

        log.fine(f"  v2 page {page + 1}: {len(records)} record(s) "
                 f"(total {yielded}) — next")
        index += page_size
        page += 1
    log.warning(f"paginate_v2({url}): hit max_pages={max_pages} at index={index}")


# ── V3 paginator (page/per_page + page_info.has_next_page) ──────────────────
def paginate_v3(config: dict, url: str, extract_key: str,
                params: dict = None, per_page: int = 200,
                max_pages: int = 10_000):
    """Iterate a V3 list endpoint that uses `page/per_page` pagination plus
    a `page_info` block with `has_next_page`.

    Same extraction rules as `paginate_v2`. Stops when `page_info.has_next_page`
    is false OR (defensively) when a page returns no records.
    """
    extras = dict(params or {})
    page_num = 1
    yielded = 0
    while page_num <= max_pages:
        call_params = dict(extras)
        call_params["page"] = page_num
        call_params["per_page"] = per_page

        body = api_request(config, url, params=call_params)

        if extract_key is None:
            records = body if isinstance(body, list) else []
            page_info = {}
        elif isinstance(body, dict):
            records = body.get(extract_key) or body.get("data") or []
            page_info = body.get("page_info") or {}
        else:
            records = []
            page_info = {}

        if not isinstance(records, list):
            log.warning(f"paginate_v3({url}): expected list at "
                        f"`{extract_key}`, got {type(records).__name__}; stopping")
            return

        for r in records:
            yield r
            yielded += 1

        has_next = bool(page_info.get("has_next_page"))
        log.fine(f"  v3 page {page_num}: {len(records)} record(s) "
                 f"(total {yielded}) — has_next_page={has_next}")

        if not has_next or not records:
            return
        page_num += 1
    log.warning(f"paginate_v3({url}): hit max_pages={max_pages} at page={page_num}")

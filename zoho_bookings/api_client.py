"""
HTTP client for the Zoho Bookings API v1.

Two request paths:
  - `api_request`                — single GET or POST (form-data) with rate limit
                                   + retry, returns parsed JSON
  - `fetch_paginated_appointments` — generator over `/fetchappointment` pages

All requests share one process-wide sliding-window rate limiter. Zoho Bookings
documents only a per-day quota (250–3000 calls/day depending on plan) and no
per-minute ceiling, but we keep the same 45/min per-endpoint + 250/min global
buffer used by the Creator connector — Zoho's underlying infra is shared and
silently throttles bursts.

Response envelope (all Bookings endpoints):
    {"response": {"returnvalue": {...}, "status": "success" | "failure"}}

`_unwrap_response()` raises `BookingsApiError` on failure status (either outer
or inner) and returns the `returnvalue` payload.

`/fetchappointment` quirks (undocumented by Zoho, confirmed via activepieces'
production zoho-bookings piece):
  - Request body is a SINGLE form field named `data` whose value is a JSON
    string containing all parameters. Sending parameters as separate form
    fields yields {"returnvalue":{"status":"failure"}} with no error message.
  - Response appointments live at `returnvalue.response` (array), NOT
    `returnvalue.data` — different from every other Bookings list endpoint.
  - Default page size 50, max 100 (60 with custom fields).
"""

import re as _re
import time

import requests
from fivetran_connector_sdk import Logging as log

from auth import api_host, get_access_token, get_headers


class DailyLimitExceeded(Exception):
    """Raised when Zoho indicates the per-day API quota is exhausted.

    Caught by `update()` to checkpoint gracefully and exit; Fivetran will
    resume from the last checkpoint on the next scheduled run."""


class BookingsApiError(Exception):
    """Raised when Bookings returns `status != "success"` in its envelope.
    HTTP was 200 but the API logically rejected the call."""


class ScopeMissing(Exception):
    """Raised when Zoho rejects the request with code 2945 (invalid oauthscope).
    Token refresh won't help — caller decides what to do."""


# ── Rate limiter ─────────────────────────────────────────────────────────────
_per_endpoint_timestamps: dict = {}
_global_timestamps: list = []

PER_ENDPOINT_LIMIT = 45
GLOBAL_LIMIT_PER_MINUTE = 250

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
    bucket = [t for t in bucket if now - t < 60]
    if len(bucket) >= PER_ENDPOINT_LIMIT:
        sleep_time = 60 - (now - bucket[0]) + 0.3
        log.info(f"Per-endpoint limit ({PER_ENDPOINT_LIMIT}/min) reached "
                 f"for {key} — sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
        now = time.time()
        bucket = [t for t in bucket if now - t < 60]
    bucket.append(now)
    _per_endpoint_timestamps[key] = bucket

    _global_timestamps = [t for t in _global_timestamps if now - t < 60]
    if len(_global_timestamps) >= GLOBAL_LIMIT_PER_MINUTE:
        sleep_time = 60 - (now - _global_timestamps[0]) + 0.3
        log.info(f"Global IP limit ({GLOBAL_LIMIT_PER_MINUTE}/min) reached "
                 f"— sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
        now = time.time()
        _global_timestamps = [t for t in _global_timestamps if now - t < 60]
    _global_timestamps.append(now)


# ── Single request ──────────────────────────────────────────────────────────
def api_request(config: dict, url: str, params: dict = None,
                form_data: dict = None, headers_extra: dict = None,
                max_retries: int = 5, method: str = "GET"):
    """Single HTTP request with rate limiting and retry on 429/5xx.

    - `method="GET"`: use `params=` for query string.
    - `method="POST"`: use `form_data=` for application/x-www-form-urlencoded
      body (Zoho Bookings `/fetchappointment` uses form encoding, not JSON).

    Returns parsed JSON (after unwrapping the Bookings response envelope), or
    `{}` on 404. A 401 triggers exactly one token refresh + retry before
    bubbling up.
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
                body = response.json()
            except ValueError:
                log.severe(f"Non-JSON 200 from {url} (len={len(response.content)})")
                raise
            return _unwrap_response(body, url)

        if sc == 204:
            return {}

        if sc == 404:
            log.fine(f"404 for {url} — returning empty")
            return {}

        if sc in (401, 403):
            body_text = response.text or ""
            if '"code":2945' in body_text or "invalid oauthscope" in body_text.lower():
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
            MAX_RETRY_AFTER = 300
            retry_after = int(response.headers.get("Retry-After", 60))
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


def _unwrap_response(body: dict, url: str) -> dict:
    """Unwrap the Bookings response envelope.

    Shape: `{"response": {"returnvalue": {...}, "status": "success"|"failure"}}`

    On `status == "success"`, return `returnvalue` (always a dict — for list
    endpoints it has a `"data"` key carrying the list).

    On `status == "failure"`, raise `BookingsApiError` with the
    `errormessage` text the API provided.

    On responses that don't match the envelope (unlikely but possible for
    future-added endpoints), pass through unchanged.
    """
    if not isinstance(body, dict) or "response" not in body:
        return body
    inner = body["response"]
    if not isinstance(inner, dict):
        return body
    outer_status = (inner.get("status") or "").lower()
    if outer_status and outer_status != "success":
        msg = (inner.get("errormessage")
               or inner.get("logMessage")
               or inner.get("returnvalue")
               or inner)
        raise BookingsApiError(
            f"Bookings API rejected {url}: outer status={outer_status}, body={msg}"
        )
    rv = inner.get("returnvalue") if isinstance(inner.get("returnvalue"), dict) else inner
    # Inner `returnvalue.status` can also be "failure", but Zoho overloads
    # that to mean both (a) genuine request rejection and (b) "no records
    # match" — e.g. /resources returns
    # `{"mesage": "There is no active resource for the criteria", "status": "failure"}`
    # (note the typo) when the workspace has no resources configured.
    # Log a warning and return an empty-but-valid payload so callers see
    # zero rows. If you hit a *real* failure, the warning will tell you.
    inner_status = (rv.get("status") if isinstance(rv, dict) else "") or ""
    if inner_status.lower() == "failure":
        msg = (rv.get("errormessage")
               or rv.get("message")
               or rv.get("mesage")  # Zoho typo seen on /resources
               or rv.get("logMessage")
               or rv)
        log.warning(
            f"Bookings inner failure for {url}: {msg}. "
            f"Treating as empty result. If this is unexpected, check "
            f"request shape (e.g. /fetchappointment needs body `data=<JSON>`)."
        )
        return {"data": [], "response": [], "_failure_message": str(msg)}
    return rv


# ── Pagination over /fetchappointment ───────────────────────────────────────
def fetch_paginated_appointments(config: dict, from_time: str, to_time: str,
                                 per_page: int = 100,
                                 extra_filters: dict = None):
    """Generator yielding individual appointment dicts from /fetchappointment.

    Body shape: a single form field `data=<JSON-string>` where the JSON
    contains the actual params (from_time, to_time, page, per_page, status,
    service_id, staff_id, etc.). Sending the params as separate form fields
    causes Zoho to return {"returnvalue":{"status":"failure"}} with no
    diagnostics — confirmed via the activepieces production integration.

    Date strings must be `dd-MMM-yyyy HH:mm:ss` (e.g. `01-Jan-2025 00:00:00`).
    Caller is responsible for formatting.

    Pagination: pass `page=1,2,...` until `next_page_available` is false.
    Per-page max is 100 (60 if custom fields are enabled).

    `workspace_id` is intentionally NOT sent — Zoho's /fetchappointment doesn't
    honour it as a filter param. The response includes `workspace_id` on each
    appointment, so callers can filter client-side if they need per-workspace
    scoping.
    """
    import json as _json

    url = f"{api_host(config)}/bookings/v1/json/fetchappointment"
    base_payload = {
        "from_time": from_time,
        "to_time":   to_time,
        "per_page":  int(per_page),
    }
    if extra_filters:
        for k, v in extra_filters.items():
            if v is not None:
                base_payload[k] = v

    page = 1
    total_yielded = 0
    while True:
        payload = dict(base_payload, page=page)
        # The single `data=<JSON>` form field is the magic — undocumented but
        # required. Without it Zoho rejects every variant with inner failure.
        form = {"data": _json.dumps(payload)}
        rv = api_request(config, url, form_data=form, method="POST")

        # /fetchappointment is the one endpoint where appointments live at
        # `returnvalue.response` instead of `returnvalue.data`. Keep the
        # `data` fallback just in case Zoho ever normalises the shape.
        records = rv.get("response") or rv.get("data") or []
        if isinstance(records, dict):
            records = []

        for record in records:
            yield record
            total_yielded += 1

        next_available = bool(rv.get("next_page_available"))
        log.fine(f"appointments page {page}: {len(records)} records, "
                 f"next_page_available={next_available}")
        if not next_available or not records:
            log.info(f"appointments: yielded {total_yielded} record(s) across "
                     f"{page} page(s)")
            return
        page += 1

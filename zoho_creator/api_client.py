"""
HTTP client for the Zoho Creator API v2.1.

Three distinct request paths:
  - `api_request`            — single GET with rate limit + retry, returns parsed JSON
  - `fetch_records`          — cursor-paginated record reader for the Data API
  - `bulk_read`              — async Bulk Read job: POST → poll → download ZIP-CSV

All three share one process-wide sliding-window rate limiter, set conservatively
at 45 calls/minute. Zoho's documented limit is 50 req/min per endpoint per IP,
but multiple endpoints share the same IP budget in practice, so a single global
counter is safer than per-endpoint counters.
"""

import csv
import io
import time
import zipfile

import requests
from fivetran_connector_sdk import Logging as log

from auth import api_host, get_access_token, get_headers


class DailyLimitExceeded(Exception):
    """Raised when Zoho indicates the per-day API quota is exhausted.

    Caught by `update()` to checkpoint gracefully and exit; Fivetran will
    resume from the last checkpoint on the next scheduled run.
    """


class BulkReadFailed(Exception):
    """Raised when a Bulk Read job ends in a FAILED state. The caller
    (tables_data) catches this and falls back to the standard Data API."""


class ScopeMissing(Exception):
    """Raised when Zoho rejects the request with code 2945 (invalid oauthscope).

    Zoho's Bulk Read API requires `ZohoCreator.bulk.CREATE`, but Zoho's OAuth
    overview page does not list that scope — so a perfectly normal OAuth grant
    misses it. Caller falls back to the Data API for that report.
    """


# ── Rate limiter ─────────────────────────────────────────────────────────────
# Zoho's documented limit is "50 per minute per endpoint per IP". An
# endpoint is the path template, not the host — so /data/.../report/A and
# /data/.../report/B have separate 50/min buckets, but every call to A
# competes for A's budget.
#
# We use per-endpoint sliding-window timestamps. A single soft IP-wide
# safety cap stops us from ever hammering the whole API.
import re as _re

# Per-endpoint timestamps: {endpoint_key: [t1, t2, ...]}
_per_endpoint_timestamps: dict = {}
# Global IP-level safety net — loose, only catches the case of many
# endpoints all firing concurrently.
_global_timestamps: list = []

PER_ENDPOINT_LIMIT = 45        # Zoho says 50/min/endpoint — buffer of 5
GLOBAL_LIMIT_PER_MINUTE = 250  # soft IP-wide ceiling

# Soft circuit-breaker: once the accounts.zoho.* token endpoint says
# "too many requests", every subsequent token refresh in this run also fails.
# This flag short-circuits further refreshes for the rest of the run.
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


# Strip the host + querystring; collapse numeric IDs so /bulk/.../{job_id}
# variants share an endpoint bucket.
_PATH_NORMALIZE_RE = _re.compile(r"/\d{6,}")


def _endpoint_key(url: str) -> str:
    path = url.split("?", 1)[0]
    # Strip leading scheme://host
    if "://" in path:
        path = path.split("/", 3)[-1]
    path = "/" + path if not path.startswith("/") else path
    # Replace long numeric IDs (Zoho job_ids, record_ids) with {id}
    return _PATH_NORMALIZE_RE.sub("/{id}", path)


def _wait_for_rate_limit(url: str):
    """Sliding-window rate limiter with two windows:

      - per-endpoint (45/min) — Zoho's documented 50/min/endpoint with a
        small safety buffer. Different reports/endpoints don't share this
        budget, so the small-tables phase can fire as fast as the API
        accepts.
      - global IP-wide (250/min) — soft ceiling so we never hammer Zoho
        across many endpoints concurrently. Only kicks in during heavy
        bursts.
    """
    global _per_endpoint_timestamps, _global_timestamps
    now = time.time()
    key = _endpoint_key(url)

    # Per-endpoint window
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

    # Global IP-wide window
    _global_timestamps = [t for t in _global_timestamps if now - t < 60]
    if len(_global_timestamps) >= GLOBAL_LIMIT_PER_MINUTE:
        sleep_time = 60 - (now - _global_timestamps[0]) + 0.3
        log.info(f"Global IP limit ({GLOBAL_LIMIT_PER_MINUTE}/min) reached "
                 f"— sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
        now = time.time()
        _global_timestamps = [t for t in _global_timestamps if now - t < 60]
    _global_timestamps.append(now)


# ── Single GET ───────────────────────────────────────────────────────────────
def api_request(config: dict, url: str, params: dict = None,
                headers_extra: dict = None, max_retries: int = 5,
                method: str = "GET", json_body: dict = None,
                return_response: bool = False):
    """Single HTTP request with rate limiting and retry on 429/5xx.

    `return_response=True` returns the raw `requests.Response` (used by
    `bulk_read` so it can grab the ZIP body). Otherwise parsed JSON is
    returned, or `{}` on 404.

    A 401 triggers exactly one token refresh + retry before bubbling up.
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

        # Happy paths
        if sc in (200, 201):
            if return_response:
                return response
            try:
                return response.json()
            except ValueError:
                # Some bulk endpoints return non-JSON bodies (e.g. ZIP).
                # Without return_response=True the caller didn't expect that — log and bail.
                log.severe(f"Non-JSON 200 from {url} (len={len(response.content)})")
                raise

        if sc == 204:
            return {} if not return_response else response

        if sc == 404:
            log.fine(f"404 for {url} — returning empty")
            return {} if not return_response else response

        # Zoho returns HTTP 400 with code 9220 for "report is empty" — which is
        # bizarre (an empty result set is not a client error). Treat it the
        # same as a 200 with no records.
        if sc == 400:
            body_text = response.text or ""
            if '"code":9220' in body_text or "no records exist in this report" in body_text.lower():
                log.fine(f"9220 (empty report) for {url} — returning empty")
                return {} if not return_response else response

        # Auth — most 401s mean the access token expired; refresh once and retry.
        # BUT: if Zoho returns code 2945 (invalid oauthscope), refreshing won't
        # help — the new token has the same scope set. Surface as ScopeMissing
        # so the caller can fall back to a different API.
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
                get_access_token(config)  # warm cache
                refreshed_once = True
                continue
            log.severe(f"Auth error {sc} for {url}: {body_text[:300]}")
            response.raise_for_status()

        # Rate limit
        if sc == 429:
            MAX_RETRY_AFTER = 300
            retry_after = int(response.headers.get("Retry-After", 60))
            # No formal X-Rate-Limit-Problem header on Zoho — we infer the
            # daily-vs-minute distinction by Retry-After magnitude.
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

        # 5xx
        if sc >= 500:
            wait = 2 ** attempt * 2
            log.warning(f"Server error {sc} for {url}. Retry in {wait}s. Body: {response.text[:200]}")
            time.sleep(wait)
            continue

        # Anything else — log and raise
        log.severe(f"API error {sc} for {url}: {response.text[:500]}")
        response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


# ── Data API: cursor-paginated record reader ─────────────────────────────────
def fetch_records(config: dict, owner: str, app: str, report: str,
                  criteria: str = None, max_records: int = 1000):
    """Generator yielding individual record dicts from a Zoho Creator report.

    Pagination model (Zoho v2.1):
      - First call: no `record_cursor` header → response carries one.
      - Subsequent calls: pass `record_cursor` back as a request header.
      - Stop when the response no longer carries the header (or `data` is empty).

    `max_records` must be 200, 500, or 1000 (Zoho's allowed values). We
    default to 1000 — the largest page — because the per-endpoint rate
    limit is fixed at 50/min, and bigger pages let us cover huge tables
    in fewer round-trips. For 73k notes: 73 calls @ page=1000 vs 365
    calls @ page=200, i.e. ~90s vs ~7m at 50 req/min.

    `field_config=all` is hard-coded because the default `quick_view`
    omits roughly half the fields; we always want the full record.
    """
    if max_records not in (200, 500, 1000):
        max_records = 1000
    url = f"{api_host(config)}/creator/v2.1/data/{owner}/{app}/report/{report}"
    params = {"field_config": "all", "max_records": max_records}
    if criteria:
        params["criteria"] = criteria

    cursor = None
    page = 0
    while True:
        headers_extra = {"record_cursor": cursor} if cursor else None
        # We need the response to read the record_cursor header — call
        # api_request with return_response=True.
        resp = api_request(config, url, params=params,
                           headers_extra=headers_extra,
                           return_response=True)
        if resp.status_code == 204 or not getattr(resp, "content", None):
            return
        try:
            data = resp.json()
        except ValueError:
            log.severe(f"Non-JSON response from {url}: {resp.content[:200]}")
            return

        records = data.get("data") or []
        page += 1
        log.fine(f"{app}/{report} page {page}: {len(records)} records")
        for record in records:
            yield record

        # Drop the params on subsequent calls — the cursor encodes them server-side.
        cursor = resp.headers.get("record_cursor")
        if not cursor or not records:
            return
        params = {}


# ── Bulk Read API: async CSV export ──────────────────────────────────────────
def bulk_read(config: dict, owner: str, app: str, report: str,
              criteria: str = None,
              poll_interval_initial: int = 5,
              poll_interval_max: int = 30,
              max_wait_seconds: int = 1800):
    """Generator yielding individual record dicts from a Bulk Read job.

    Lifecycle:
      1. POST  /creator/v2.1/bulk/{owner}/{app}/report/{report}/read       → job_id
      2. GET   /creator/v2.1/bulk/{owner}/{app}/report/{report}/read/{id}  → poll until COMPLETED
      3. GET   /creator/v2.1/bulk/{owner}/{app}/report/{report}/read/{id}/result → ZIP body

    The ZIP contains a single CSV. We stream-extract it via `zipfile.ZipFile`
    and `csv.DictReader`. Up to 200,000 rows per job.

    On FAILED status → raises `BulkReadFailed` so the caller can fall back
    to the standard Data API.
    """
    base = f"{api_host(config)}/creator/v2.1/bulk/{owner}/{app}/report/{report}/read"

    # 1) Create the job
    body = {"query": {"criteria": criteria}} if criteria else {}
    log.info(f"Bulk Read: creating job for {app}/{report}"
             + (f" (criteria={criteria})" if criteria else ""))
    create = api_request(config, base, method="POST",
                         json_body=body,
                         headers_extra={"Content-Type": "application/json"})

    # Zoho responses are wrapped: {"code": 3000, "details": {"id": "...", ...}}
    details = create.get("details") or create
    job_id = details.get("id")
    if not job_id:
        # Bulk Read returns code 7150 ("Bulk APIs are currently not supported
        # for pivot charts, pivot tables and third-party reports") for
        # report types that simply can't be bulk-exported. Surface as
        # BulkReadFailed so the caller falls back to the Data API gracefully.
        ccode = create.get("code")
        if ccode == 7150 or "not supported" in str(create).lower():
            raise BulkReadFailed(
                f"Bulk Read not supported for {app}/{report} "
                f"(chart/pivot/3rd-party report). Body: {create}"
            )
        raise RuntimeError(f"Bulk Read create: no job id in response. Body: {create}")

    # 2) Poll. The CREATE POST already succeeded, so we KNOW the user has
    # `ZohoCreator.bulk.CREATE`. If the poll/download path returns a fresh
    # ScopeMissing (some per-app permission quirk we've seen on Category-2
    # shared apps), we re-raise as BulkReadFailed so the caller falls back
    # to the Data API for THIS report only — not for the whole run.
    poll_url = f"{base}/{job_id}"
    waited = 0
    interval = poll_interval_initial
    try:
        while True:
            status_resp = api_request(config, poll_url)
            status_details = status_resp.get("details") or status_resp
            status = (status_details.get("status") or "").upper()
            if status == "COMPLETED":
                log.info(f"Bulk Read {app}/{report} completed (job_id={job_id}, "
                         f"records={status_details.get('record_count', '?')}).")
                break
            if status == "FAILED":
                raise BulkReadFailed(
                    f"Bulk Read job failed for {app}/{report}: {status_details}"
                )
            if waited >= max_wait_seconds:
                raise BulkReadFailed(
                    f"Bulk Read job {job_id} for {app}/{report} did not "
                    f"complete within {max_wait_seconds}s (last status={status})."
                )
            log.fine(f"Bulk Read {app}/{report}: status={status}, waited={waited}s")
            time.sleep(interval)
            waited += interval
            # Linear ramp toward poll_interval_max so we don't hammer early
            # but don't wait forever near the end.
            if interval < poll_interval_max:
                interval = min(poll_interval_max, interval + 5)

        # 3) Download result
        result_url = f"{poll_url}/result"
        resp = api_request(config, result_url, return_response=True)
    except ScopeMissing as e:
        # Post-create scope rejection — keep bulk enabled globally, just
        # surface as a per-report failure.
        raise BulkReadFailed(
            f"Bulk Read poll/download for {app}/{report} rejected with "
            f"missing scope (probably an app-level permission on this "
            f"Category-2 shared app). Falling back to Data API for this "
            f"report only. Underlying: {e!s}"
        )
    if not resp.content:
        log.warning(f"Bulk Read {app}/{report}: result endpoint returned empty body")
        return

    # The result is a ZIP containing exactly one CSV (per Zoho docs).
    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        # Some bulk responses come back as plain JSON when no records matched.
        try:
            j = resp.json()
            log.info(f"Bulk Read {app}/{report}: result was JSON (no rows). Body: {j}")
        except ValueError:
            log.severe(
                f"Bulk Read {app}/{report}: result was neither ZIP nor JSON. "
                f"First bytes: {resp.content[:100]!r}"
            )
        return

    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_names:
        log.warning(f"Bulk Read {app}/{report}: ZIP contained no CSV (entries={zf.namelist()})")
        return

    with zf.open(csv_names[0]) as fh:
        # Bulk Read CSVs are UTF-8. Wrap to a text stream so DictReader can parse.
        text_stream = io.TextIOWrapper(fh, encoding="utf-8", newline="")
        reader = csv.DictReader(text_stream)
        n = 0
        for row in reader:
            n += 1
            yield row
        log.info(f"Bulk Read {app}/{report}: yielded {n} CSV rows from {csv_names[0]}")

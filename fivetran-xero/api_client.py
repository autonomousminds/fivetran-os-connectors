"""
HTTP client for Xero APIs with rate limiting, pagination, and retry logic.

All API communication flows through this module.
"""

import time

import requests
from fivetran_connector_sdk import Logging as log

from auth import get_headers

class DailyRateLimitExceeded(Exception):
    """Raised when Xero's daily API call quota is exhausted."""
    pass


ACCOUNTING_BASE = "https://api.xero.com/api.xro/2.0"
PAYROLL_BASE = "https://api.xero.com/payroll.xro/2.0"

# Rate limiter: sliding window
_call_timestamps = []
MAX_CALLS_PER_MINUTE = 55  # conservative buffer under Xero's 60/min limit


def reset_rate_limiter():
    """Clear rate limiter state. Call at the start of each sync run
    to prevent stale timestamps from carrying over if the process is reused."""
    global _call_timestamps
    _call_timestamps = []


def _wait_for_rate_limit():
    """Sliding window rate limiter: max 55 calls per 60-second window."""
    global _call_timestamps
    now = time.time()
    _call_timestamps = [t for t in _call_timestamps if now - t < 60]
    if len(_call_timestamps) >= MAX_CALLS_PER_MINUTE:
        sleep_time = 60 - (now - _call_timestamps[0]) + 0.5
        log.info(f"Rate limit approaching, sleeping {sleep_time:.1f}s")
        time.sleep(sleep_time)
    _call_timestamps.append(time.time())


def _log_rate_limit_headers(response, url):
    """Log all Xero rate limit headers for diagnostics."""
    rate_headers = {
        k: v for k, v in response.headers.items()
        if any(x in k.lower() for x in ["rate", "limit", "retry", "remaining"])
    }
    if rate_headers:
        level = log.warning if response.status_code == 429 else log.fine
        level(f"Rate limit headers for {url.split('/')[-1]}: {rate_headers}")


def api_request(config: dict, url: str, params: dict = None,
                headers_extra: dict = None, max_retries: int = 3,
                scope_group: str = "accounting") -> dict:
    """
    Single API request with rate limiting, retry on 429/5xx, and error handling.
    Returns parsed JSON response. Returns empty dict on 404.
    """
    headers = get_headers(config, scope_group=scope_group)
    if headers_extra:
        headers.update(headers_extra)

    for attempt in range(max_retries):
        _wait_for_rate_limit()

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt * 5
                log.warning(f"Request error for {url}: {exc}. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise

        # Log Xero rate limit headers on every response
        _log_rate_limit_headers(response, url)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            log.warning(f"404 for {url} - resource not found, returning empty")
            return {}
        elif response.status_code == 429:
            MAX_RETRY_AFTER = 120  # never sleep more than 2 minutes
            raw_retry = int(response.headers.get("Retry-After", 60))
            rate_problem = response.headers.get("X-Rate-Limit-Problem", "")
            if rate_problem == "day" or raw_retry > MAX_RETRY_AFTER:
                raise DailyRateLimitExceeded(
                    f"Xero daily API limit exhausted "
                    f"(Retry-After={raw_retry}s, problem={rate_problem}). "
                    f"Aborting sync — will resume from last checkpoint."
                )
            log.warning(f"Rate limited (429). Sleeping {raw_retry}s")
            time.sleep(raw_retry)
        elif response.status_code >= 500:
            wait = 2 ** attempt * 5
            log.warning(f"Server error {response.status_code} for {url}. Retrying in {wait}s")
            time.sleep(wait)
        else:
            log.severe(f"API error {response.status_code} for {url}: {response.text}")
            response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


def fetch_all_pages(config: dict, endpoint: str, entity_key: str,
                    base_url: str = ACCOUNTING_BASE,
                    modified_since: str = None,
                    page_size: int = 500):
    """
    Generator: iterate all records across pages without buffering the full dataset.
    Yields individual records one at a time; each page is freed after processing.

    Xero Accounting API supports pageSize up to 1000 (default 100).
    Only a few Accounting endpoints support pagination: Invoices, Contacts,
    BankTransactions, ManualJournals. All Payroll endpoints support it.
    Callers must ensure this function is only used for paginated endpoints.

    Termination: stop when a page returns fewer than page_size records.
    """
    url = f"{base_url}{endpoint}"
    scope_group = "payroll" if base_url == PAYROLL_BASE else "accounting"
    page = 1
    headers_extra = {}
    if modified_since:
        headers_extra["If-Modified-Since"] = modified_since

    while True:
        data = api_request(
            config, url,
            params={"page": page, "pageSize": page_size},
            headers_extra=headers_extra if headers_extra else None,
            scope_group=scope_group,
        )
        records = data.get(entity_key, [])
        yield from records

        if len(records) < page_size:
            break
        page += 1


def fetch_all_no_pagination(config: dict, endpoint: str, entity_key: str,
                            base_url: str = ACCOUNTING_BASE,
                            modified_since: str = None) -> list:
    """Fetch all records from a non-paginated endpoint (single GET)."""
    url = f"{base_url}{endpoint}"
    scope_group = "payroll" if base_url == PAYROLL_BASE else "accounting"
    headers_extra = {}
    if modified_since:
        headers_extra["If-Modified-Since"] = modified_since
    data = api_request(
        config, url,
        headers_extra=headers_extra if headers_extra else None,
        scope_group=scope_group,
    )
    return data.get(entity_key, [])


def fetch_single(config: dict, endpoint: str,
                 base_url: str = ACCOUNTING_BASE) -> dict:
    """Fetch a single resource (no pagination)."""
    url = f"{base_url}{endpoint}"
    scope_group = "payroll" if base_url == PAYROLL_BASE else "accounting"
    return api_request(config, url, scope_group=scope_group)


def fetch_journals(config: dict, offset: int = 0):
    """
    Generator for Journals (offset-based pagination).
    Yields (page_records, new_offset) tuples per page to allow per-page checkpointing.
    """
    url = f"{ACCOUNTING_BASE}/Journals"

    while True:
        data = api_request(config, url, params={"offset": offset})
        records = data.get("Journals", [])
        offset += len(records)
        yield records, offset

        if len(records) < 100:
            break

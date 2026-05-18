"""
HTTP client for the Breww public API.

Handles Bearer auth, DRF PageNumberPagination (`?page=N`, response shape
`{count, next, previous, results}`), 429 + 5xx retries with backoff,
and yields flattened records ready for op.upsert.
"""

import time

import requests
from fivetran_connector_sdk import Logging as log

from auth import get_headers

BASE_URL = "https://breww.com/api"


class RateLimitExceeded(Exception):
    """Raised when Breww's API rate limit window is exhausted beyond a reasonable wait."""
    pass


# Breww doesn't publish a documented rate limit. Stay conservative.
# A small inter-request delay prevents bursts that could trip an undocumented limiter.
_MIN_INTERVAL = 0.05  # 20 req/s ceiling — well below any plausible Django/DRF default
_last_request_ts = [0.0]


def _throttle():
    elapsed = time.time() - _last_request_ts[0]
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_request_ts[0] = time.time()


def api_request(config: dict, url: str, params: dict = None,
                max_retries: int = 5) -> dict:
    """Single GET with retry on 429/5xx. Returns parsed JSON, or {} on 404."""
    headers = get_headers(config)

    for attempt in range(max_retries):
        _throttle()
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt * 2
                log.warning(f"Network error for {url}: {exc}. Retry in {wait}s")
                time.sleep(wait)
                continue
            raise

        sc = response.status_code
        if sc == 200:
            return response.json()
        if sc == 404:
            log.fine(f"404 for {url} — skipping")
            return {}
        if sc == 401 or sc == 403:
            log.severe(f"Auth error {sc} for {url}: {response.text[:300]}")
            response.raise_for_status()
        if sc == 429:
            MAX_RETRY_AFTER = 300
            retry_after = int(response.headers.get("Retry-After", 30))
            if retry_after > MAX_RETRY_AFTER:
                raise RateLimitExceeded(
                    f"Rate-limited with Retry-After={retry_after}s. "
                    f"Aborting — will resume from checkpoint."
                )
            log.warning(f"429 for {url}. Sleeping {retry_after}s")
            time.sleep(retry_after)
            continue
        if sc >= 500:
            wait = 2 ** attempt * 2
            log.warning(f"Server error {sc} for {url}. Retry in {wait}s")
            time.sleep(wait)
            continue
        log.severe(f"API error {sc} for {url}: {response.text[:500]}")
        response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


def fetch_all_pages(config: dict, endpoint: str, params: dict = None,
                    page_size: int = 100):
    """
    Generator yielding raw API records across all pages.

    `endpoint` is the path after BASE_URL — e.g. "/orders/" or "/customers-suppliers/".
    Pagination follows `response["next"]` until null. Records are yielded as the
    server returned them (unflattened) — the caller (sync_table) is responsible
    for child-table extraction and flatten_record on the parent.
    """
    url = f"{BASE_URL}{endpoint}"
    request_params = dict(params or {})
    request_params.setdefault("page_size", page_size)

    page = 1
    while True:
        data = api_request(config, url, params=request_params)
        if not data:
            return

        results = data.get("results", [])
        count = data.get("count", 0)
        next_url = data.get("next")
        log.fine(f"{endpoint} page {page}: {len(results)} records (count={count})")

        for record in results:
            yield record

        del data, results

        if not next_url:
            break
        # DRF supplies the full next URL — switch to it and drop the original params,
        # because `next` already encodes them (including any filters we sent on page 1).
        url = next_url
        request_params = {}
        page += 1

"""
HTTP client for Productive API with dual rate limiting, pagination, and retry logic.

All API communication flows through this module.
"""

import time
from collections import deque

import requests
from fivetran_connector_sdk import Logging as log

from auth import get_headers
from helpers import flatten_jsonapi_record

BASE_URL = "https://api.productive.io/api/v2"


class RateLimitExceeded(Exception):
    """Raised when Productive's API rate limit window is exhausted."""
    pass


# Dual sliding-window rate limiter
_short_timestamps = deque()   # 100 per 10 seconds
_long_timestamps = deque()    # 4000 per 30 minutes

SHORT_WINDOW = 10        # seconds
SHORT_MAX = 90            # conservative buffer (limit is 100)
LONG_WINDOW = 1800        # 30 minutes in seconds
LONG_MAX = 3800           # conservative buffer (limit is 4000)


def _wait_for_rate_limit():
    """Dual sliding-window rate limiter: 90/10s and 3800/30min."""
    now = time.time()

    # Prune expired timestamps
    while _short_timestamps and now - _short_timestamps[0] > SHORT_WINDOW:
        _short_timestamps.popleft()
    while _long_timestamps and now - _long_timestamps[0] > LONG_WINDOW:
        _long_timestamps.popleft()

    # Check short window
    if len(_short_timestamps) >= SHORT_MAX:
        sleep_time = SHORT_WINDOW - (now - _short_timestamps[0]) + 0.5
        if sleep_time > 0:
            log.info(f"Short rate limit approaching ({len(_short_timestamps)}/{SHORT_MAX}), sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)

    # Check long window
    if len(_long_timestamps) >= LONG_MAX:
        sleep_time = LONG_WINDOW - (now - _long_timestamps[0]) + 1.0
        if sleep_time > 120:
            raise RateLimitExceeded(
                f"30-min rate limit exhausted ({len(_long_timestamps)}/{LONG_MAX}), "
                f"need {sleep_time:.0f}s wait. Aborting — will resume from last checkpoint."
            )
        if sleep_time > 0:
            log.warning(f"Long rate limit approaching, sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)

    _short_timestamps.append(time.time())
    _long_timestamps.append(time.time())


def api_request(config: dict, url: str, params: dict = None,
                max_retries: int = 3) -> dict:
    """
    Single API request with rate limiting, retry on 429/5xx, and error handling.
    Returns parsed JSON response. Returns empty dict on 404.
    """
    headers = get_headers(config)

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

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            log.fine(f"404 for {url} — endpoint not available, skipping")
            return {}
        elif response.status_code == 429:
            MAX_RETRY_AFTER = 120
            raw_retry = int(response.headers.get("Retry-After", 60))
            if raw_retry > MAX_RETRY_AFTER:
                raise RateLimitExceeded(
                    f"Rate limit exceeded (Retry-After={raw_retry}s). "
                    f"Aborting sync — will resume from last checkpoint."
                )
            log.warning(f"Rate limited (429). Sleeping {raw_retry}s")
            time.sleep(raw_retry)
        elif response.status_code >= 500:
            wait = 2 ** attempt * 5
            log.warning(f"Server error {response.status_code} for {url}. Retrying in {wait}s")
            time.sleep(wait)
        else:
            log.severe(f"API error {response.status_code} for {url}: {response.text[:500]}")
            response.raise_for_status()

    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


def fetch_all_pages(config: dict, endpoint: str, params: dict = None,
                    page_size: int = 100):
    """
    Generator: iterate all records across pages, yielding flattened dicts.

    Uses JSON:API pagination with page[number] and page[size].
    Records are flattened from JSON:API format before yielding.
    """
    url = f"{BASE_URL}{endpoint}"
    page = 1
    base_params = {"page[size]": page_size}
    if params:
        base_params.update(params)

    while True:
        request_params = {**base_params, "page[number]": page}
        data = api_request(config, url, params=request_params)

        records = data.get("data", [])
        meta = data.get("meta", {})
        total_pages = meta.get("total_pages", 0)
        num_records = len(records)

        for record in records:
            yield flatten_jsonapi_record(record)

        # Free response memory before fetching next page
        del data, records

        if page >= total_pages or num_records < page_size:
            break
        page += 1


def fetch_single(config: dict, endpoint: str) -> dict:
    """Fetch a single resource and flatten it."""
    url = f"{BASE_URL}{endpoint}"
    data = api_request(config, url)
    record = data.get("data")
    if record and isinstance(record, dict):
        return flatten_jsonapi_record(record)
    return {}

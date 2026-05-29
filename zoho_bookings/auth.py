"""
OAuth token management for Zoho Bookings API v1.

Uses the refresh-token grant from Zoho's Self-Client flow: the user generates
a long-lived refresh_token once at api-console.zoho.{dc} and supplies it via
configuration.json. This module exchanges it for short-lived access tokens
(~1 hour) and caches them with a 60-second safety margin.

The refresh_token itself does NOT rotate on Zoho's Self-Client flow, so we
never need to persist a new one back to state.

OAuth scope required: `zohobookings.data.CREATE`. That is the only documented
scope for Zoho Bookings — read endpoints accept it as well as the write ones.
"""

import time

import requests
from fivetran_connector_sdk import Logging as log

# Zoho identity hosts per data center. CA is the odd one out (zohocloud.ca).
ACCOUNTS_URLS = {
    "com":    "https://accounts.zoho.com",
    "eu":     "https://accounts.zoho.eu",
    "in":     "https://accounts.zoho.in",
    "com.au": "https://accounts.zoho.com.au",
    "com.cn": "https://accounts.zoho.com.cn",
    "jp":     "https://accounts.zoho.jp",
    "sa":     "https://accounts.zoho.sa",
    "ca":     "https://accounts.zohocloud.ca",
}

# API hosts per data center.
API_HOSTS = {
    "com":    "https://www.zohoapis.com",
    "eu":     "https://www.zohoapis.eu",
    "in":     "https://www.zohoapis.in",
    "com.au": "https://www.zohoapis.com.au",
    "com.cn": "https://www.zohoapis.com.cn",
    "jp":     "https://www.zohoapis.jp",
    "sa":     "https://www.zohoapis.sa",
    "ca":     "https://www.zohoapis.ca",
}

# (access_token, expiry_epoch_seconds) — single tuple, only one scope group needed.
_token: tuple = ()


def reset_caches():
    """Clear cached token. Call at the start of each sync run so a reused
    Python process does not serve a stale token."""
    global _token
    _token = ()


def _accounts_url(config: dict) -> str:
    dc = config.get("data_center", "com")
    if dc not in ACCOUNTS_URLS:
        raise ValueError(
            f"Unknown data_center '{dc}'. Expected one of: {sorted(ACCOUNTS_URLS)}"
        )
    return ACCOUNTS_URLS[dc]


def api_host(config: dict) -> str:
    """Public — used by api_client to build request URLs."""
    dc = config.get("data_center", "com")
    if dc not in API_HOSTS:
        raise ValueError(
            f"Unknown data_center '{dc}'. Expected one of: {sorted(API_HOSTS)}"
        )
    return API_HOSTS[dc]


def get_access_token(config: dict) -> str:
    """Return a valid Zoho access token, refreshing via the refresh_token grant
    when the cached one is within 60 seconds of expiry (or absent)."""
    global _token

    if _token:
        token, expiry = _token
        if time.time() < (expiry - 60):
            return token

    # Circuit breaker: if Zoho's token endpoint just told us "too many
    # requests", another refresh inside the same sync run will get the same
    # answer. Re-use the (now-stale) cached token if we have one; otherwise
    # raise so the caller can stop the loop.
    from api_client import token_endpoint_rate_limited, mark_token_endpoint_rate_limited
    if token_endpoint_rate_limited():
        if _token:
            return _token[0]
        raise RuntimeError(
            "Zoho token endpoint is rate-limited and no access token is cached. "
            "Aborting — Fivetran will retry on the next scheduled run."
        )

    token_url = f"{_accounts_url(config)}/oauth/v2/token"
    log.info("Refreshing Zoho access token...")
    resp = requests.post(
        token_url,
        data={
            "grant_type":    "refresh_token",
            "refresh_token": config["refresh_token"],
            "client_id":     config["client_id"],
            "client_secret": config["client_secret"],
        },
        timeout=30,
    )

    if not resp.ok:
        body_text = (resp.text or "")[:300]
        if "too many requests" in body_text.lower():
            mark_token_endpoint_rate_limited()
            log.severe(
                "Zoho token endpoint is rate-limited (too many continuous "
                "refreshes in a short window). Tripping the circuit breaker "
                "so the rest of this run doesn't make it worse."
            )
        log.severe(
            f"Token refresh failed: {resp.status_code} {body_text}. "
            f"If this is a 'INVALID_TOKEN' or 'invalid_code' error, your "
            f"refresh_token has been revoked — regenerate it in the Zoho "
            f"API Console for data center '{config.get('data_center')}'."
        )
        resp.raise_for_status()

    data = resp.json()
    if "access_token" not in data:
        err = data.get("error") or data
        raise RuntimeError(
            f"Zoho token endpoint returned no access_token. Body: {err}. "
            f"Check that client_id/client_secret/refresh_token were all "
            f"generated in the same data center "
            f"({config.get('data_center')}.{_accounts_url(config)})."
        )

    token = data["access_token"]
    expires_in = int(data.get("expires_in", 3600))
    _token = (token, time.time() + expires_in)
    log.info(f"Zoho access token obtained (expires in {expires_in}s).")
    return token


def get_headers(config: dict, extra: dict = None) -> dict:
    """Standard headers for Zoho Bookings API requests."""
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_access_token(config)}",
        "Accept":        "application/json",
    }
    if extra:
        headers.update(extra)
    return headers

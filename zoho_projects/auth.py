"""
OAuth token management for the Zoho Projects API.

Uses the refresh-token grant from Zoho's Self-Client flow: the user generates
a long-lived refresh_token once at api-console.zoho.{dc} and supplies it via
configuration.json. This module exchanges it for short-lived access tokens
(~1 hour) and caches them with a 60-second safety margin.

The refresh_token itself does NOT rotate on Zoho's Self-Client flow, so we
never need to persist a new one back to state.

Zoho Projects has two coexisting URL prefixes that share the same host:
  * V2 (legacy):  `{host}/restapi/portal/{portal_id}/...`
  * V3 (current): `{host}/api/v3/portal/{portal_id}/...`

The host itself is `projectsapi.zoho.{dc}` (the formally documented host for
V3), or equivalently `projects.zoho.{dc}` — both route to the same backend.
We default to `projectsapi.zoho.{dc}` but always honour `api_domain` returned
in the OAuth token response if Zoho gives us one.

Required OAuth scopes (read-only sync):
  ZohoProjects.portals.READ      portal list + portal details
  ZohoProjects.projects.READ     projects, groups, layouts, custom statuses
  ZohoProjects.milestones.READ   milestones (per-project + portal-wide)
  ZohoProjects.tasklists.READ    tasklists (incl. template tasklists)
  ZohoProjects.tasks.READ        tasks, subtasks, comments, attachments,
                                 dependencies, followers, custom views,
                                 status history
  ZohoProjects.bugs.READ         bugs, comments, attachments, resolution,
                                 timer, followers, default + renamed +
                                 custom fields
  ZohoProjects.timesheets.READ   time logs (per-task, per-bug, general),
                                 timesheet layouts + custom fields
  ZohoProjects.events.READ       meetings
  ZohoProjects.forums.READ       forums, categories, comments
  ZohoProjects.users.READ        portal + project users, client users
  ZohoProjects.clients.READ      client companies + project clients
  ZohoProjects.documents.READ    documents + folders
  ZohoProjects.search.READ       portal- and project-scoped search
  ZohoProjects.activities.READ   project activity feed
  ZohoProjects.status.READ       project status (team-post) feed
  ZohoProjects.tags.READ         portal tags
  ZohoProjects.leave.READ        leaves (V3)
  ZohoPC.files.READ              attachments + document binaries
  ZohoSearch.securesearch.READ   global tag search (V3)

These 19 scopes are all the connector needs. V3 tables (profiles, roles,
phases) populate under existing scope buckets. The V3 `/teams` endpoint
returns 401 INVALID_OAUTHSCOPE for every standard scope combination — Zoho
has not publicly documented a scope that unlocks it. The connector skips
it gracefully; the rest of the sync continues unaffected.
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

# Zoho Projects API hosts per data center. The `projectsapi.zoho.{dc}` form is
# the formally documented host for V3 and the only one that serves the
# per-portal Projects endpoints.
#
# IMPORTANT: do NOT use the generic `api_domain` returned in the OAuth token
# response. Zoho gives back `https://www.zohoapis.{dc}` which fronts CRM,
# Books, and the cross-product portal-list endpoint — but the per-portal
# Projects endpoints there return CRM's "Zoho CRM - Error" 400 HTML page.
# The dedicated `projectsapi.zoho.{dc}` host is the only one that routes
# every Projects path correctly.
API_HOSTS = {
    "com":    "https://projectsapi.zoho.com",
    "eu":     "https://projectsapi.zoho.eu",
    "in":     "https://projectsapi.zoho.in",
    "com.au": "https://projectsapi.zoho.com.au",
    "com.cn": "https://projectsapi.zoho.com.cn",
    "jp":     "https://projectsapi.zoho.jp",
    "sa":     "https://projectsapi.zoho.sa",
    "ca":     "https://projectsapi.zohocloud.ca",
}

# (access_token, expiry_epoch_seconds, api_domain_or_None)
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
    """Public — used by api_client to build request URLs. Returns the
    Projects API host (no trailing slash, no path).

    Deliberately ignores the `api_domain` from the OAuth token response
    because Zoho returns the cross-product `zohoapis.{dc}` host there — and
    that host's per-portal paths route to CRM, not Projects. See the
    comment on API_HOSTS above."""
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
        token, expiry = _token[0], _token[1]
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
            f"If this is an 'INVALID_TOKEN' or 'invalid_code' error, your "
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
    api_domain = data.get("api_domain")
    _token = (token, time.time() + expires_in, api_domain)
    log.info(f"Zoho access token obtained (expires in {expires_in}s"
             f"{', api_domain=' + api_domain if api_domain else ''}).")
    return token


def get_headers(config: dict, extra: dict = None) -> dict:
    """Standard headers for Zoho Projects API requests.

    Zoho's auth scheme is `Zoho-oauthtoken <token>` — NOT the standard
    `Bearer` prefix. Wrong header = 401 even with a valid token."""
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_access_token(config)}",
        "Accept":        "application/json",
    }
    if extra:
        headers.update(extra)
    return headers

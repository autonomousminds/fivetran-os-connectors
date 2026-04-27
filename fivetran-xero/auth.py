"""
OAuth2 token management for Xero Custom Connection apps.

Uses client_credentials grant type — no user authorization or
refresh tokens needed. The app is pre-authorized to a single org.

On first token request per scope group, tries all scopes at once.
If Xero returns invalid_scope, probes each scope individually and
uses only the granted ones. Missing scopes are logged as warnings.
"""

import time

import requests
from fivetran_connector_sdk import Logging as log

TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"

ACCOUNTING_SCOPES = [
    "accounting.transactions.read",
    "accounting.settings.read",
    "accounting.contacts.read",
    "accounting.journals.read",
    "accounting.attachments.read",
    "assets.read",
]

PAYROLL_SCOPES = [
    "payroll.employees.read",
    "payroll.settings.read",
    "payroll.timesheets.read",
    "payroll.payruns.read",
    "payroll.payslip.read",
]

# Per-scope-group token cache: {"accounting": (token, expiry), "payroll": (token, expiry)}
_tokens = {}
_tenant_id = None
# Resolved scope strings after probing: {"accounting": "scope1 scope2", "payroll": "scope1 scope2"}
_resolved_scopes = {}


def reset_caches():
    """Clear all module-level caches. Call at the start of each sync run
    to prevent stale tokens/scopes from carrying over if the process is reused."""
    global _tokens, _tenant_id, _resolved_scopes
    _tokens = {}
    _tenant_id = None
    _resolved_scopes = {}


def _try_token(config: dict, scope: str):
    """Try to get a token for the given scope string. Returns response object."""
    return requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "scope": scope,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )


def _resolve_scopes(config: dict, scope_group: str) -> str:
    """
    Resolve which scopes are actually granted for a scope group.
    Tries all scopes at once first; if that fails with invalid_scope,
    probes each scope individually to find which ones work.
    Returns the space-joined string of granted scopes.
    """
    if scope_group in _resolved_scopes:
        return _resolved_scopes[scope_group]

    all_scopes = PAYROLL_SCOPES if scope_group == "payroll" else ACCOUNTING_SCOPES
    full_scope_str = " ".join(all_scopes)

    # Fast path: try all scopes at once
    resp = _try_token(config, full_scope_str)
    if resp.ok:
        _resolved_scopes[scope_group] = full_scope_str
        return full_scope_str

    # If the error isn't invalid_scope, it's a real auth problem — raise
    error_body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if error_body.get("error") != "invalid_scope":
        log.severe(f"Token request failed for {scope_group}: {resp.status_code} {resp.text}")
        raise Exception(f"Token request failed: {resp.status_code}")

    # Slow path: probe each scope individually
    log.warning(f"Some {scope_group} scopes are not authorised — probing individually...")
    granted = []
    denied = []
    for scope in all_scopes:
        probe = _try_token(config, scope)
        if probe.ok:
            granted.append(scope)
        else:
            denied.append(scope)

    if denied:
        log.warning(f"Scopes NOT authorised for this Xero app: {', '.join(denied)}")
    if granted:
        log.info(f"Granted {scope_group} scopes: {', '.join(granted)}")

    granted_str = " ".join(granted)
    _resolved_scopes[scope_group] = granted_str
    return granted_str


def get_access_token(config: dict, scope_group: str = "accounting") -> str:
    """Return a valid access token for the given scope group ('accounting' or 'payroll')."""
    global _tokens

    cached = _tokens.get(scope_group)
    if cached:
        token, expiry = cached
        if time.time() < (expiry - 60):
            return token

    scope_str = _resolve_scopes(config, scope_group)
    if not scope_str:
        raise Exception(f"No {scope_group} scopes are authorised for this Xero app")

    log.info(f"Requesting Xero access token for {scope_group} scopes...")
    resp = _try_token(config, scope_str)

    if not resp.ok:
        log.severe(f"Failed to get access token: {resp.status_code} {resp.text}")
        raise Exception(f"Token request failed: {resp.status_code}")

    data = resp.json()
    token = data["access_token"]
    expiry = time.time() + data.get("expires_in", 1800)
    _tokens[scope_group] = (token, expiry)

    log.info(f"Access token obtained for {scope_group}.")
    return token


def is_payroll_available(config: dict) -> bool:
    """
    Test whether the Xero app has any payroll scopes authorised AND
    the org's Custom Connection actually grants payroll API access.

    Xero can issue a valid OAuth token with payroll scopes even when
    the Custom Connection hasn't been re-authorized to include payroll.
    In that case the token works but every API call returns 403.
    We probe /Settings once to detect this before trying all endpoints.
    """
    scope_str = _resolve_scopes(config, "payroll")
    if not scope_str:
        log.warning(
            "No payroll scopes authorised for this Xero app — "
            "skipping all payroll tables. To enable payroll sync, "
            "add payroll scopes in the Xero developer portal."
        )
        return False

    # Probe actual API access with a lightweight call
    try:
        headers = get_headers(config, scope_group="payroll")
        resp = requests.get(
            "https://api.xero.com/payroll.xro/2.0/Settings",
            headers=headers, timeout=15,
        )
        if resp.status_code == 403:
            log.warning(
                "Payroll token obtained but API returned 403 Forbidden. "
                "The Custom Connection likely needs re-authorization with "
                "payroll scopes in the Xero developer portal "
                "(My Apps → select app → re-authorize). "
                "Skipping all payroll tables."
            )
            return False
        return True
    except Exception as e:
        log.warning(f"Could not verify payroll access: {e}. Skipping payroll tables.")
        return False


def get_tenant_id(config: dict) -> str:
    """
    Get the tenant ID. Custom Connection apps are bound to a single org,
    so we fetch it from the /connections endpoint.
    """
    global _tenant_id

    if _tenant_id:
        return _tenant_id

    # If explicitly provided in config, use that
    if config.get("tenant_id"):
        _tenant_id = config["tenant_id"]
        return _tenant_id

    # Otherwise, fetch from connections endpoint (accounting token is sufficient)
    token = get_access_token(config, scope_group="accounting")
    response = requests.get(
        CONNECTIONS_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    response.raise_for_status()
    connections = response.json()

    if not connections:
        raise ValueError("No Xero tenants found. Ensure your custom app is connected to an organisation.")

    _tenant_id = connections[0]["tenantId"]
    log.info(f"Resolved tenant ID: {_tenant_id}")
    return _tenant_id


def get_headers(config: dict, scope_group: str = "accounting") -> dict:
    """Return standard headers for Xero API calls including auth and tenant ID."""
    return {
        "Authorization": f"Bearer {get_access_token(config, scope_group=scope_group)}",
        "xero-tenant-id": get_tenant_id(config),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

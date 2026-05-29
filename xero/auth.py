"""
OAuth2 token management for Xero Custom Connection apps.

Uses client_credentials grant type — no user authorization or
refresh tokens needed. The app is pre-authorized to a single org.

Xero migrated from broad scopes (accounting.transactions,
accounting.reports.read) to granular ones effective 2 March 2026 (new apps)
and rolled out to all apps by end of April 2026. This module uses the
new granular scope names exclusively.

Scope-group token cache: one token per (scope_group) keyed by the resolved
scope string. Scope groups:
  - "accounting" — bundle of all accounting + reports + assets scopes
                   (single token covers Accounting + Assets + Reports
                   endpoints — they share api.xero.com auth).
  - "journals"   — accounting.journals.read only. Premium-gated (Advanced
                   tier + security review + use-case approval). Probed
                   separately so the connector gracefully skips Journals
                   sync when the scope isn't authorised.
  - "files"      — files.read for the Files API.
  - "projects"   — projects.read for the Projects API.
  - "payroll"    — payroll.* read scopes (UK Payroll).

On first token request per scope group, tries all scopes at once.
If Xero returns invalid_scope, probes each scope individually and
uses only the granted ones. Missing scopes are logged as warnings.
"""

import time

import requests
from fivetran_connector_sdk import Logging as log

from exceptions import DailyRateLimitExceeded

TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"

# Granular Accounting API scopes (replaces deprecated accounting.transactions.read
# and accounting.reports.read umbrella scopes). Bundled into one token because
# they all hit api.xero.com.
ACCOUNTING_SCOPES = [
    # Core data
    "accounting.banktransactions.read",   # BankTransactions, BankTransfers
    "accounting.invoices.read",           # Invoices, CreditNotes, LinkedTransactions,
                                          # Quotes, PurchaseOrders, RepeatingInvoices, Items
    "accounting.payments.read",           # Payments, BatchPayments, Overpayments, Prepayments
    "accounting.manualjournals.read",     # ManualJournals
    "accounting.contacts.read",           # Contacts, ContactGroups
    "accounting.settings.read",           # Accounts, BrandingThemes, Currencies,
                                          # Items, Organisation, TaxRates,
                                          # TrackingCategories, Users
    "accounting.attachments.read",        # Attachments
    "accounting.budgets.read",            # Budgets
    # Reports (granular — one scope per report)
    "accounting.reports.aged.read",            # AgedPayablesByContact, AgedReceivablesByContact
    "accounting.reports.balancesheet.read",    # BalanceSheet
    "accounting.reports.banksummary.read",     # BankSummary
    "accounting.reports.budgetsummary.read",   # BudgetSummary
    "accounting.reports.executivesummary.read",  # ExecutiveSummary
    "accounting.reports.profitandloss.read",   # ProfitAndLoss
    "accounting.reports.trialbalance.read",    # TrialBalance
    "accounting.reports.taxreports.read",      # GST/BAS reports
    "accounting.reports.tenninetynine.read",   # 1099 (US)
    # Assets API — shares auth.xero.com host, can be bundled
    "assets.read",
]

# accounting.journals.read is premium-gated post-April 2026
# (Advanced pricing tier + security assessment + use-case approval).
# Kept separate so we can probe-and-skip without blocking the rest.
JOURNALS_SCOPES = ["accounting.journals.read"]

FILES_SCOPES = ["files.read"]

PROJECTS_SCOPES = ["projects.read"]

PAYROLL_SCOPES = [
    "payroll.employees.read",
    "payroll.settings.read",
    "payroll.timesheets.read",
    "payroll.payruns.read",
    "payroll.payslip.read",
]

_SCOPE_LISTS = {
    "accounting": ACCOUNTING_SCOPES,
    "journals": JOURNALS_SCOPES,
    "files": FILES_SCOPES,
    "projects": PROJECTS_SCOPES,
    "payroll": PAYROLL_SCOPES,
}

# Per-scope-group token cache: {scope_group: (token, expiry_epoch)}
_tokens = {}
_tenant_id = None
# Resolved scope strings after probing: {scope_group: "scope1 scope2 ..."}
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
    Returns the space-joined string of granted scopes (empty if none).
    """
    if scope_group in _resolved_scopes:
        return _resolved_scopes[scope_group]

    all_scopes = _SCOPE_LISTS.get(scope_group)
    if not all_scopes:
        raise ValueError(f"Unknown scope_group: {scope_group}")

    full_scope_str = " ".join(all_scopes)

    # Fast path: try all scopes at once
    resp = _try_token(config, full_scope_str)
    if resp.ok:
        _resolved_scopes[scope_group] = full_scope_str
        return full_scope_str

    error_body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    if error_body.get("error") != "invalid_scope":
        log.severe(f"Token request failed for {scope_group}: {resp.status_code} {resp.text}")
        raise Exception(f"Token request failed: {resp.status_code}")

    # Slow path: probe each scope individually
    log.warning(f"Some {scope_group} scopes are not authorised — probing individually...")
    granted, denied = [], []
    for scope in all_scopes:
        if _try_token(config, scope).ok:
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
    """Return a valid access token for the given scope group."""
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


def _probe_endpoint(config: dict, scope_group: str, probe_url: str,
                    feature_name: str, denial_hints: dict) -> bool:
    """
    Generic probe: resolve scopes for a group, then GET a lightweight endpoint
    to confirm the org actually grants API access (not just the OAuth scope).

    Xero can issue a valid token whose scopes the org doesn't accept (e.g.
    payroll token to a non-payroll org, or journals token to a non-Advanced-tier
    customer). The token works but the API returns 401/403.
    denial_hints maps status_code → log message to use on that specific denial.
    """
    scope_str = _resolve_scopes(config, scope_group)
    if not scope_str:
        log.warning(
            f"No {scope_group} scopes authorised for this Xero app — "
            f"skipping all {feature_name} tables."
        )
        return False
    try:
        headers = get_headers(config, scope_group=scope_group)
        resp = requests.get(probe_url, headers=headers, timeout=15)

        # 429 is a rate-limit signal, NOT a "feature unavailable" signal —
        # callers must not silently disable the feature. If the day budget
        # is exhausted, abort the whole sync so the connector can resume
        # next day with all features intact. If just the per-minute window
        # is full, sleep through it and probe once more.
        if resp.status_code == 429:
            raw_retry = int(resp.headers.get("Retry-After", 60))
            rate_problem = resp.headers.get("X-Rate-Limit-Problem", "")
            if rate_problem == "day" or raw_retry > 120:
                raise DailyRateLimitExceeded(
                    f"Day limit exhausted during {feature_name} probe "
                    f"(Retry-After={raw_retry}s, problem={rate_problem})."
                )
            log.warning(
                f"{feature_name.title()} probe rate-limited (429). "
                f"Sleeping {raw_retry}s and retrying once."
            )
            time.sleep(raw_retry)
            resp = requests.get(probe_url, headers=headers, timeout=15)

        if resp.status_code == 200:
            return True
        hint = denial_hints.get(resp.status_code)
        if hint:
            log.warning(f"{hint} Detail: {resp.text[:300]}")
        else:
            log.warning(
                f"{feature_name.title()} probe returned unexpected status "
                f"{resp.status_code}. Skipping all {feature_name} tables. "
                f"Detail: {resp.text[:300]}"
            )
        return False
    except DailyRateLimitExceeded:
        raise
    except Exception as e:
        log.warning(f"Could not verify {feature_name} access: {e}. Skipping {feature_name} tables.")
        return False


def is_payroll_available(config: dict) -> bool:
    """True iff Xero Payroll is provisioned for the connected org AND
    the Custom Connection has payroll scopes authorised."""
    return _probe_endpoint(
        config,
        scope_group="payroll",
        probe_url="https://api.xero.com/payroll.xro/2.0/Settings",
        feature_name="payroll",
        denial_hints={
            401: ("Payroll token obtained but API returned 401 Unauthorized. "
                  "The connected Xero organisation does not have Payroll "
                  "provisioned, or the connection lacks Payroll Administrator "
                  "permissions. Skipping all payroll tables."),
            403: ("Payroll token obtained but API returned 403 Forbidden. "
                  "The Custom Connection likely needs re-authorization with "
                  "payroll scopes in the Xero developer portal. "
                  "Skipping all payroll tables."),
        },
    )


def is_journals_available(config: dict) -> bool:
    """True iff the Custom Connection has accounting.journals.read authorised
    AND the Xero org has Journals API access (Advanced pricing tier + security
    review + use-case approval, per Xero policy effective April 2026)."""
    return _probe_endpoint(
        config,
        scope_group="journals",
        probe_url="https://api.xero.com/api.xro/2.0/Journals",
        feature_name="journals",
        denial_hints={
            401: ("Journals token obtained but API returned 401 Unauthorized. "
                  "Skipping accounting_journal* tables."),
            403: ("Journals API returned 403 Forbidden. From April 2026 the "
                  "/Journals endpoint requires the Advanced pricing tier "
                  "($1,445 AUD/month), a security assessment, and use-case "
                  "approval from Xero. Skipping accounting_journal* tables."),
        },
    )


def is_assets_available(config: dict) -> bool:
    """True iff the Xero org has Fixed Assets enabled.
    The `assets.read` scope is bundled into the 'accounting' token, so we
    probe the API directly — 403 here means the org-level feature is off."""
    return _probe_endpoint(
        config,
        scope_group="accounting",
        probe_url="https://api.xero.com/assets.xro/1.0/Settings",
        feature_name="assets",
        denial_hints={
            403: ("Assets API returned 403 Forbidden. The connected Xero "
                  "organisation does not have Fixed Assets enabled. "
                  "Skipping assets tables."),
            401: ("Assets API returned 401 Unauthorized. Skipping assets tables."),
        },
    )


def is_files_available(config: dict) -> bool:
    """True iff files.read is authorised for this Custom Connection."""
    return _probe_endpoint(
        config,
        scope_group="files",
        probe_url="https://api.xero.com/files.xro/1.0/Files?pagesize=1",
        feature_name="files",
        denial_hints={
            401: "Files token obtained but API returned 401 Unauthorized. Skipping files tables.",
            403: "Files API returned 403 Forbidden. Skipping files tables.",
        },
    )


def is_projects_available(config: dict) -> bool:
    """True iff projects.read is authorised for this Custom Connection."""
    return _probe_endpoint(
        config,
        scope_group="projects",
        probe_url="https://api.xero.com/projects.xro/2.0/Projects?pagesize=1",
        feature_name="projects",
        denial_hints={
            401: "Projects token obtained but API returned 401 Unauthorized. Skipping projects tables.",
            403: "Projects API returned 403 Forbidden. Skipping projects tables.",
        },
    )


def get_tenant_id(config: dict) -> str:
    """
    Get the tenant ID. Custom Connection apps are bound to a single org,
    so we fetch it from the /connections endpoint.
    """
    global _tenant_id

    if _tenant_id:
        return _tenant_id

    if config.get("tenant_id"):
        _tenant_id = config["tenant_id"]
        return _tenant_id

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

"""Connector-wide exceptions.

Lives in its own module so both `auth.py` (probes) and `api_client.py`
(main request loop) can import it without a circular dependency.
"""


class DailyRateLimitExceeded(Exception):
    """Raised when Xero's daily API call quota is exhausted."""
    pass

"""Authentication header builder for Productive API."""


def get_headers(config: dict) -> dict:
    """Build request headers with API token and organization context."""
    return {
        "X-Auth-Token": config["api_token"],
        "X-Organization-Id": config["organization_id"],
        "Content-Type": "application/vnd.api+json",
    }

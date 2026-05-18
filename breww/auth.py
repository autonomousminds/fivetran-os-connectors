"""Authentication header builder for Breww API."""


def get_headers(config: dict) -> dict:
    """Build request headers with Bearer API key."""
    return {
        "Authorization": f"Bearer {config['api_key']}",
        "Accept": "application/json",
    }

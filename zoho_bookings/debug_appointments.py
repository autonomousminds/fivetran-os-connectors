"""
Confirmed via activepieces source: /fetchappointment expects a single
form field `data` containing all params as a JSON string. Test that.
"""

import json

from fivetran_connector_sdk import Logging
Logging.LOG_LEVEL = Logging.Level.INFO

from auth import api_host, get_access_token, reset_caches
import requests


def call_data_field(label: str, host: str, token: str, payload: dict):
    """POST with body `data=<json-encoded payload>` (urlencoded form)."""
    url = f"{host}/bookings/v1/json/fetchappointment"
    print(f"\n── {label} ──")
    print(f"   POST {url}")
    print(f"   data payload (JSON): {json.dumps(payload)}")
    r = requests.post(
        url,
        headers={"Authorization": f"Zoho-oauthtoken {token}",
                 "Accept": "application/json"},
        data={"data": json.dumps(payload)},
        timeout=30,
    )
    print(f"   HTTP {r.status_code}")
    try:
        body = r.json()
    except ValueError:
        body = r.text
    text = json.dumps(body, indent=2, default=str) if isinstance(body, (dict, list)) else str(body)
    if len(text) > 4000:
        text = text[:4000] + " ...(truncated)"
    print(f"   body:\n{text}")


def main():
    with open("configuration.json") as f:
        config = json.load(f)

    reset_caches()
    token = get_access_token(config)
    host = api_host(config)

    # I. Empty JSON payload — docs say no params = today's appointments.
    call_data_field("I. data={}  (should return today's appointments)",
                    host, token, {})

    # J. Tight May 2026 window — the window your UI shows appointments.
    call_data_field("J. data={from_time, to_time} for May 2026",
                    host, token, {
                        "from_time": "01-May-2026 00:00:00",
                        "to_time":   "31-May-2026 23:59:59",
                    })

    # K. Wide window.
    call_data_field("K. data={from_time, to_time} for 2020-2030",
                    host, token, {
                        "from_time": "01-Jan-2020 00:00:00",
                        "to_time":   "31-Dec-2030 23:59:59",
                        "page":      1,
                        "per_page":  50,
                    })


if __name__ == "__main__":
    main()

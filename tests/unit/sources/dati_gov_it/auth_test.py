"""
Auth verification test for the dati.gov.it (CKAN) connector.

Confirms that the credentials in dev_config.json can reach the CKAN Action API
and that the portal responds with a successful status.

dati.gov.it is a fully public read-only portal — no API key is required for
read operations. If an api_key is present in dev_config.json it is passed as
an Authorization header (CKAN convention). If base_url is empty or missing the
default https://www.dati.gov.it/opendata/api/3/action is used.

Run with:
    .venv/bin/python -m pytest tests/unit/sources/dati_gov_it/auth_test.py -v

Or directly:
    .venv/bin/python tests/unit/sources/dati_gov_it/auth_test.py
"""

from __future__ import annotations

import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

import pytest

# Allow running as a plain script from the project root.
_ROOT = pathlib.Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.unit.sources.test_utils import load_config  # noqa: E402

CONFIG_PATH = pathlib.Path(__file__).parent / "configs" / "dev_config.json"

_DEFAULT_BASE_URL = "https://www.dati.gov.it/opendata/api/3/action"

pytestmark = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="dev_config.json not found — skipping live auth tests",
)


def _base_url(config: dict) -> str:
    """Return the effective base URL, falling back to the default."""
    url = config.get("base_url", "").rstrip("/")
    return url if url else _DEFAULT_BASE_URL


def _get_json(url: str, config: dict) -> dict:
    """Make a GET request and return the parsed JSON body."""
    req = urllib.request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    api_key = config.get("api_key", "")
    if api_key:
        req.add_header("Authorization", api_key)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def test_site_read():
    """
    Call GET /site_read to confirm the CKAN portal is reachable and returns
    {"success": true}.  This endpoint requires no arguments and no auth,
    making it the ideal lightweight connectivity probe.
    """
    config = load_config(CONFIG_PATH)
    base_url = _base_url(config)
    url = f"{base_url}/site_read"

    try:
        body = _get_json(url, config)
    except urllib.error.HTTPError as exc:
        pytest.fail(
            f"HTTP {exc.code} from {url}. "
            f"Check base_url and api_key in dev_config.json.\n"
            f"Response: {exc.read().decode('utf-8', errors='replace')[:500]}"
        )

    assert body.get("success") is True, (
        f"CKAN returned success=false.\nFull response: {body}"
    )

    print(f"\nAuthentication successful. Portal is reachable at {base_url}")
    print(f"   site_read response: {body}")


def test_organization_list_returns_results():
    """
    Call GET /organization_list as a secondary connectivity check.
    A healthy dati.gov.it instance should return a non-empty list of
    organization slugs.
    """
    config = load_config(CONFIG_PATH)
    base_url = _base_url(config)
    url = f"{base_url}/organization_list?limit=5&offset=0"

    try:
        body = _get_json(url, config)
    except urllib.error.HTTPError as exc:
        pytest.fail(
            f"HTTP {exc.code} from {url}.\n"
            f"Response: {exc.read().decode('utf-8', errors='replace')[:500]}"
        )

    assert body.get("success") is True, (
        f"organization_list returned success=false: {body}"
    )

    orgs = body.get("result", [])
    assert isinstance(orgs, list) and len(orgs) > 0, (
        "organization_list returned an empty result — portal may be down or misconfigured."
    )

    print(f"\nOrganization list check passed. First {len(orgs)} org slugs: {orgs}")


if __name__ == "__main__":
    config = load_config(CONFIG_PATH)
    base_url = _base_url(config)
    url = f"{base_url}/site_read"
    print(f"Calling {url} ...")

    try:
        body = _get_json(url, config)
    except urllib.error.HTTPError as exc:
        print(f"Authentication failed: HTTP {exc.code}")
        print(f"   Body: {exc.read().decode('utf-8', errors='replace')[:500]}")
        sys.exit(1)

    if body.get("success"):
        print(f"Authentication successful! Portal is reachable at {base_url}")
        print(f"   Response: {body}")
        sys.exit(0)
    else:
        print(f"Authentication failed: success=false in response")
        print(f"   Body: {body}")
        sys.exit(1)

"""OAuth2 password-grant client for the immobiliare.it Insights API.

The Market Explorer service authenticates with the Resource Owner Password
Grant flow: the connector POSTs ``grant_type=password`` to ``/oauth/token``
with HTTP Basic ``client_id:client_secret`` and form-encoded ``username``,
``password``. Tokens expire after ~14400 seconds.

The auth docs explicitly warn that re-authenticating before every API call
"can be flagged by our protection systems and may lead to a temporary ban,"
so this module caches the token in memory and only re-grants when:

* the cached token is missing,
* the cached token is past its expiry minus a safety buffer, or
* the caller signals a 401 by calling :meth:`force_refresh`.

The cache is keyed by ``(base_url, client_id, username)`` so multiple
connector instances sharing the same credentials reuse the same token even
though each has its own :class:`OAuthTokenManager` instance — keeping this
module self-contained avoids cross-instance state leakage.
"""

from __future__ import annotations

import base64
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from databricks.labs.community_connector.sources.immobiliare_it.immobiliare_it_schemas import (
    INITIAL_BACKOFF,
    MAX_BACKOFF,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    TOKEN_REFRESH_BUFFER_SECONDS,
    TOKEN_REQUEST_TIMEOUT,
    USER_AGENT,
)


class OAuthTokenManager:
    """Per-connector token cache and refresher.

    Thread-safe — the lock guards ``_access_token`` / ``_expires_at`` so that
    concurrent driver-side calls (e.g. parallel tests) cannot trigger more
    than one ``/oauth/token`` request at once. Executors typically build
    their own manager (see :func:`create_executor_manager`) so contention
    on this lock is rare in practice.
    """

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
    ) -> None:
        if not all([base_url, client_id, client_secret, username, password]):
            missing = [
                name
                for name, val in [
                    ("base_url", base_url),
                    ("client_id", client_id),
                    ("client_secret", client_secret),
                    ("username", username),
                    ("password", password),
                ]
                if not val
            ]
            raise ValueError(
                f"OAuth credentials missing required fields: {', '.join(missing)}"
            )
        self._base_url = base_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._username = username
        self._password = password

        self._access_token: str | None = None
        self._expires_at: float = 0.0  # epoch seconds
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_token(self) -> str:
        """Return a valid access token, fetching one if necessary."""
        with self._lock:
            if self._is_token_valid():
                return self._access_token  # type: ignore[return-value]
            self._refresh_locked()
            return self._access_token  # type: ignore[return-value]

    def force_refresh(self) -> str:
        """Discard the cached token and fetch a fresh one (call on 401)."""
        with self._lock:
            self._access_token = None
            self._expires_at = 0.0
            self._refresh_locked()
            return self._access_token  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        if not self._access_token:
            return False
        return time.time() + TOKEN_REFRESH_BUFFER_SECONDS < self._expires_at

    def _refresh_locked(self) -> None:
        """Fetch a new token via password grant. Caller must hold ``_lock``."""
        body = self._password_grant()
        token = body.get("access_token")
        if not isinstance(token, str) or not token:
            raise RuntimeError(
                "OAuth token endpoint returned no access_token: "
                f"{json.dumps(body)[:300]}"
            )
        # ``expires_in`` is documented as 14399. Be defensive in case the
        # field is missing or malformed.
        try:
            expires_in = int(body.get("expires_in", 14400))
        except (TypeError, ValueError):
            expires_in = 14400
        self._access_token = token
        self._expires_at = time.time() + expires_in

    def _password_grant(self) -> dict[str, Any]:
        url = f"{self._base_url}/oauth/token"
        basic = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode("utf-8")
        ).decode("ascii")
        form = urllib.parse.urlencode(
            {
                "grant_type": "password",
                "username": self._username,
                "password": self._password,
            }
        ).encode("ascii")

        backoff = INITIAL_BACKOFF
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                req = urllib.request.Request(url, data=form, method="POST")
                req.add_header("Authorization", f"Basic {basic}")
                req.add_header("Content-Type", "application/x-www-form-urlencoded")
                req.add_header("Accept", "application/json")
                req.add_header("User-Agent", USER_AGENT)
                with urllib.request.urlopen(
                    req, timeout=TOKEN_REQUEST_TIMEOUT
                ) as resp:
                    raw = resp.read().decode("utf-8")
                return json.loads(raw)
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code not in RETRIABLE_STATUS_CODES or attempt == MAX_RETRIES - 1:
                    detail = ""
                    try:
                        detail = exc.read().decode("utf-8", errors="replace")[:500]
                    except Exception:
                        pass
                    raise RuntimeError(
                        f"OAuth token request failed: HTTP {exc.code} {detail}"
                    ) from exc
            except urllib.error.URLError as exc:
                last_exc = exc
                if attempt == MAX_RETRIES - 1:
                    raise RuntimeError(
                        f"OAuth token network error: {exc}"
                    ) from exc
            time.sleep(min(backoff, MAX_BACKOFF))
            backoff *= 2

        # Defensive — loop should have returned or raised.
        raise RuntimeError(
            f"Exhausted retries reaching {url}: {last_exc}"
        )


def create_token_manager_from_options(
    options: dict[str, str], default_base_url: str
) -> OAuthTokenManager:
    """Factory used by both driver and executors.

    Reads connection options the same way regardless of where it's called,
    so :meth:`read_partition` on a Spark executor produces the same token
    cache shape as the driver. ``options`` must contain ``client_id``,
    ``client_secret``, ``username``, ``password``; ``base_url`` is optional.
    """
    base_url = (options.get("base_url") or "").strip().rstrip("/") or default_base_url
    return OAuthTokenManager(
        base_url=base_url,
        client_id=options.get("client_id", ""),
        client_secret=options.get("client_secret", ""),
        username=options.get("username", ""),
        password=options.get("password", ""),
    )

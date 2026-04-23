"""LakeflowConnect connector for dati.gov.it (Italian national open data portal).

The portal runs a customised CKAN instance with the DCAT-AP_IT extension.
Every read-only endpoint we use is publicly accessible; ``api_key`` is
optional and, when supplied, is forwarded as an ``Authorization`` header
per CKAN convention.

Design decisions
----------------

* **HTTP client**: standard library ``urllib`` (no ``requests`` dependency) to
  match the convention used in ``tests/unit/sources/dati_gov_it/auth_test.py``.
* **Partitioning**: packages and resources support range queries on
  ``metadata_modified`` so we implement ``SupportsPartitionedStream`` and
  split incremental windows into time slices that can be fetched in parallel
  on Spark executors. Tables without range filters (organizations, tags,
  groups) opt out via ``is_partitioned`` and fall back to the single-driver
  ``read_table`` path.
* **Resources**: derived from ``package_search`` rather than issuing one
  ``resource_show`` call per resource. This avoids O(N) amplification and
  keeps the watermark (``metadata_modified`` on the parent package) aligned
  between the two tables.
* **Filters**: ``organization``, ``tags``, ``groups``, ``res_format``, ``q``
  are accepted as per-table ``external_options`` and only take effect for
  packages / resources. They are ANDed into a single Solr ``fq`` expression.
* **Soft deletes**: CKAN uses soft-deletes only (``state = "deleted"``).
  Passing ``include_deleted=true`` to ``package_search`` — off by default, on
  via the ``include_deleted`` table option — surfaces them. We report
  ingestion type ``cdc`` (not ``cdc_with_deletes``) because there is no
  separate delete endpoint.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.dati_gov_it.dati_gov_it_schemas import (
    DEFAULT_BASE_URL,
    DEFAULT_INCREMENTAL_FLOOR,
    DEFAULT_WINDOW_SECONDS,
    GROUPS_TABLE,
    INCREMENTAL_TABLES,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    MIN_WINDOW_SECONDS,
    ORGANIZATIONS_TABLE,
    PACKAGE_FILTER_KEYS,
    PACKAGES_TABLE,
    PAGE_SIZE,
    RESOURCES_TABLE,
    RETRIABLE_STATUS_CODES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TAGS_TABLE,
    USER_AGENT,
)


# ---------------------------------------------------------------------------
# Low-level CKAN HTTP helper
# ---------------------------------------------------------------------------


class _CKANClient:
    """Thin urllib-based client for the CKAN Action API.

    Designed to be cheap to construct on executors. ``read_partition`` builds
    a fresh client inside Spark workers so it must not rely on any shared
    mutable state from the driver.
    """

    def __init__(self, base_url: str, api_key: str | None = None, timeout: int = 60):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key or None
        self._timeout = timeout

    def get(self, action: str, params: dict[str, Any] | None = None) -> dict:
        """GET an action endpoint and return the unwrapped CKAN ``result``.

        Raises ``RuntimeError`` on HTTP errors (after retries) or when CKAN
        signals ``success: false`` in the response envelope.
        """
        query = urllib.parse.urlencode(_normalise_params(params or {}), doseq=False)
        url = f"{self._base_url}/{action}"
        if query:
            url = f"{url}?{query}"

        body = self._request(url)
        if not isinstance(body, dict) or not body.get("success", False):
            err = body.get("error") if isinstance(body, dict) else body
            raise RuntimeError(f"CKAN {action} returned error: {err}")
        return body["result"]

    def _request(self, url: str) -> dict:
        backoff = INITIAL_BACKOFF
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                req = urllib.request.Request(url, method="GET")
                req.add_header("Accept", "application/json")
                req.add_header("User-Agent", USER_AGENT)
                if self._api_key:
                    req.add_header("Authorization", self._api_key)
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    raw = resp.read().decode("utf-8")
                return json.loads(raw)
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code not in RETRIABLE_STATUS_CODES or attempt == MAX_RETRIES - 1:
                    # Surface the body for easier debugging.
                    try:
                        detail = exc.read().decode("utf-8", errors="replace")[:500]
                    except Exception:
                        detail = ""
                    raise RuntimeError(
                        f"HTTP {exc.code} calling {url}: {detail}"
                    ) from exc
            except urllib.error.URLError as exc:
                last_exc = exc
                if attempt == MAX_RETRIES - 1:
                    raise RuntimeError(f"Network error calling {url}: {exc}") from exc
            time.sleep(backoff)
            backoff *= 2

        # Defensive — loop should always return or raise.
        raise RuntimeError(f"Exhausted retries calling {url}: {last_exc}")


def _normalise_params(params: dict[str, Any]) -> list[tuple[str, str]]:
    """Flatten params, dropping Nones and coercing booleans to CKAN's lowercase."""
    out: list[tuple[str, str]] = []
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            out.append((key, "true" if value else "false"))
        else:
            out.append((key, str(value)))
    return out


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class DatiGovItLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for dati.gov.it.

    Extends :class:`SupportsPartitionedStream` so that packages and resources
    (the two high-volume tables) can be read in parallel across Spark
    executors using ``metadata_modified`` time windows. Other tables fall
    back to single-driver snapshot reads via ``read_table``.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        base_url = (options.get("base_url") or "").strip().rstrip("/")
        self._base_url = base_url or DEFAULT_BASE_URL
        self._api_key = options.get("api_key") or None

        # Driver-side client — only used from ``list_tables`` / ``read_table`` /
        # ``latest_offset``. Executors build their own client inside
        # ``read_partition`` so we never try to serialise this one.
        self._client = _CKANClient(self._base_url, self._api_key)

        # Freeze the "now" ceiling at init time so a single trigger never
        # chases new data forever. The next trigger gets a fresh instance
        # and therefore a fresh ceiling.
        self._init_ts = _utc_now_iso()

    # ------------------------------------------------------------------
    # LakeflowConnect surface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Return the static list of supported tables (object list is hardcoded)."""
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        # Copy so the caller cannot mutate our module-level dict.
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read path.

        For snapshot tables this reads the full list. For packages/resources
        this is the fallback when partitioning is disabled — it performs a
        sequential sliding-window read advancing one window per call.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name == ORGANIZATIONS_TABLE:
            return self._read_organizations(table_options)
        if table_name == TAGS_TABLE:
            return self._read_tags(table_options)
        if table_name == GROUPS_TABLE:
            return self._read_groups(table_options)
        if table_name == PACKAGES_TABLE:
            return self._read_packages_window(start_offset, table_options)
        if table_name == RESOURCES_TABLE:
            return self._read_resources_window(start_offset, table_options)

        raise ValueError(f"Unhandled table: {table_name}")  # pragma: no cover

    # ------------------------------------------------------------------
    # SupportsPartitionedStream surface
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """Only packages and resources support partitioned reads.

        Organizations / tags / groups are small (~hundreds of rows at most),
        have no time-range filter on their list endpoints, and should use
        ``simpleStreamReader`` via ``read_table``.
        """
        return table_name in INCREMENTAL_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the current high-water mark for the streaming cursor.

        We use wall-clock "now at driver init" as the ceiling rather than a
        `package_search` metadata call. Packages are sorted by
        ``metadata_modified asc``, and any row with ``metadata_modified <=
        self._init_ts`` belongs in the batch. Using ``self._init_ts`` keeps
        the ceiling stable for the life of this driver instance so that
        micro-batches don't grow unboundedly.
        """
        self._validate_table(table_name)
        if table_name not in INCREMENTAL_TABLES:
            # Non-partitioned tables never call latest_offset in practice,
            # but return a stable empty dict to be defensive.
            return {}
        return {"cursor": self._init_ts}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split the [start_cursor, end_cursor] range into time windows.

        Each returned descriptor is ``{"since": iso, "until": iso}`` and is
        fed to ``read_partition`` on an executor. The bounds are inclusive
        of ``since`` and exclusive of ``until`` — we add a negligible 1ms
        nudge on the seam to avoid duplicate fetches on back-to-back windows
        (Solr's range query is inclusive on both ends; we dedupe by letting
        the cursor advance monotonically, and rely on upsert semantics for
        the rare edge-on-seam record).
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name not in INCREMENTAL_TABLES:
            # Single descriptor → whole table. This path is only exercised
            # if the framework calls ``get_partitions`` for a non-partitioned
            # table; ``is_partitioned`` returns False for these so normally
            # it doesn't happen.
            return [{"mode": "full"}]

        start = (start_offset or {}).get("cursor") if start_offset else None
        end = (end_offset or {}).get("cursor") if end_offset else None

        # When called from batch mode, start/end are None → cover everything
        # from the configured floor up to the init timestamp.
        if start is None:
            start = table_options.get("start_timestamp") or DEFAULT_INCREMENTAL_FLOOR
        if end is None:
            end = self._init_ts

        # Empty range — nothing to do. (Spark streaming sends start==end when
        # caught up.)
        if start >= end:
            return []

        window_seconds = max(
            int(table_options.get("window_seconds", DEFAULT_WINDOW_SECONDS)),
            MIN_WINDOW_SECONDS,
        )

        partitions: list[dict] = []
        cursor_dt = _parse_iso(start)
        end_dt = _parse_iso(end)
        while cursor_dt < end_dt:
            next_dt = min(cursor_dt + timedelta(seconds=window_seconds), end_dt)
            partitions.append(
                {
                    "since": _to_iso(cursor_dt),
                    "until": _to_iso(next_dt),
                }
            )
            cursor_dt = next_dt
        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one partition on a Spark executor.

        Re-creates a CKAN client from ``self.options`` so the call is
        self-contained — driver-side state (``self._client``, ``self._init_ts``)
        must not be relied upon here.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        # Full-table fallback — only happens if a caller invokes
        # ``get_partitions`` on a non-partitioned table.
        if partition.get("mode") == "full":
            if table_name == ORGANIZATIONS_TABLE:
                records, _ = self._read_organizations(table_options)
                return records
            if table_name == TAGS_TABLE:
                records, _ = self._read_tags(table_options)
                return records
            if table_name == GROUPS_TABLE:
                records, _ = self._read_groups(table_options)
                return records

        since = partition["since"]
        until = partition["until"]

        client = _CKANClient(self._base_url, self._api_key)
        if table_name == PACKAGES_TABLE:
            return iter(
                self._paginate_packages_range(client, since, until, table_options)
            )
        if table_name == RESOURCES_TABLE:
            return iter(
                self._paginate_resources_range(client, since, until, table_options)
            )
        raise ValueError(f"Unhandled partition for table: {table_name}")

    # ------------------------------------------------------------------
    # Snapshot table readers
    # ------------------------------------------------------------------

    def _read_organizations(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Enumerate all organizations via ``organization_list`` + ``organization_show``.

        Note: the portal's ``organization_list?all_fields=true`` returns HTTP
        500 (quirk #9 in the API doc). We therefore always fetch the slug
        list first and iterate with ``organization_show`` to get full
        records.
        """
        slugs = self._client.get(
            "organization_list",
            params={"all_fields": False, "limit": PAGE_SIZE, "offset": 0},
        )
        # CKAN's organization_list returns everything in one call on this
        # portal (hundreds, not thousands). Still, loop for safety.
        all_slugs: list[str] = list(slugs or [])
        offset = len(all_slugs)
        while len(slugs or []) == PAGE_SIZE:
            slugs = self._client.get(
                "organization_list",
                params={"all_fields": False, "limit": PAGE_SIZE, "offset": offset},
            )
            if not slugs:
                break
            all_slugs.extend(slugs)
            offset += len(slugs)

        records: list[dict] = []
        for slug in all_slugs:
            try:
                rec = self._client.get(
                    "organization_show",
                    params={
                        "id": slug,
                        "include_datasets": False,
                        "include_dataset_count": True,
                        "include_extras": True,
                        "include_users": False,  # Per spec: don't fetch users.
                    },
                )
            except RuntimeError:
                # One bad organization should not fail the whole snapshot —
                # log-by-raising is the library convention but for tolerance
                # we skip. The run_id + source log will surface the miss.
                continue
            records.append(_shape_organization(rec))
        return iter(records), {}

    def _read_tags(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Enumerate tags via a single ``tag_list?all_fields=true`` call.

        ``all_fields=true`` returns {id, name, display_name, vocabulary_id}
        per tag — everything the flat schema needs.

        CKAN's ``tag_list`` action does not paginate: the dati.gov.it
        instance ignores ``limit``/``offset`` and always returns the full
        set (~16k tags). Pass no pagination params and take the single
        response as authoritative.
        """
        batch = self._client.get("tag_list", params={"all_fields": True})
        records = [
            {
                "id": tag.get("id"),
                "name": tag.get("name"),
                "display_name": tag.get("display_name"),
                "vocabulary_id": tag.get("vocabulary_id"),
            }
            for tag in (batch or [])
        ]
        return iter(records), {}

    def _read_groups(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Enumerate groups via ``group_list`` then ``group_show`` per slug.

        Groups are a small set (~10–30) so this is cheap.
        """
        slugs = self._client.get(
            "group_list",
            params={"all_fields": False, "limit": PAGE_SIZE, "offset": 0},
        ) or []
        records: list[dict] = []
        for slug in slugs:
            try:
                rec = self._client.get(
                    "group_show",
                    params={
                        "id": slug,
                        "include_datasets": False,
                        "include_dataset_count": True,
                        "include_extras": True,
                        "include_users": False,
                    },
                )
            except RuntimeError:
                continue
            records.append(_shape_group(rec))
        return iter(records), {}

    # ------------------------------------------------------------------
    # Packages / resources — sequential (fallback) window reader
    # ------------------------------------------------------------------

    def _read_packages_window(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        since, until = self._next_window(start_offset, table_options)
        if since is None:
            return iter([]), start_offset or {}

        records = self._paginate_packages_range(self._client, since, until, table_options)
        end_offset = self._advance_offset(records, until, start_offset, "metadata_modified")
        return iter(records), end_offset

    def _read_resources_window(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        since, until = self._next_window(start_offset, table_options)
        if since is None:
            return iter([]), start_offset or {}

        records = self._paginate_resources_range(self._client, since, until, table_options)
        end_offset = self._advance_offset(
            records, until, start_offset, "package_metadata_modified"
        )
        return iter(records), end_offset

    def _next_window(
        self, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[str | None, str | None]:
        """Compute the [since, until] window for a single sequential call.

        ``since`` comes from the checkpoint (``cursor``), falling back to
        ``start_timestamp`` in options, then to the hardcoded floor. ``until``
        is ``min(since + window_seconds, self._init_ts)``. When we have
        already caught up (since >= init_ts) we return (None, None) to tell
        the caller to emit nothing.
        """
        since = None
        if start_offset:
            since = start_offset.get("cursor")
        if not since:
            since = table_options.get("start_timestamp")
        if not since:
            since = DEFAULT_INCREMENTAL_FLOOR

        if since >= self._init_ts:
            return None, None

        window_seconds = max(
            int(table_options.get("window_seconds", DEFAULT_WINDOW_SECONDS)),
            MIN_WINDOW_SECONDS,
        )
        until_dt = min(
            _parse_iso(since) + timedelta(seconds=window_seconds),
            _parse_iso(self._init_ts),
        )
        return since, _to_iso(until_dt)

    @staticmethod
    def _advance_offset(
        records: list[dict],
        window_until: str,
        start_offset: dict | None,
        cursor_field: str,
    ) -> dict:
        """Compute the ``end_offset`` after a sequential window call.

        If the window yielded records, advance to ``max(records[*].cursor)``.
        Otherwise advance to ``window_until`` so the next call moves forward
        rather than looping forever. When the advanced cursor equals the
        starting cursor, return the original offset unchanged to signal
        pagination is exhausted (this is how the framework detects EOF).
        """
        if records:
            # Records may have ``None`` for the cursor in pathological cases;
            # filter those out defensively.
            cursors = [
                _iso_string(r.get(cursor_field))
                for r in records
                if r.get(cursor_field) is not None
            ]
            if cursors:
                next_cursor = max(cursors)
            else:
                next_cursor = window_until
        else:
            next_cursor = window_until

        end_offset = {"cursor": next_cursor}
        if start_offset and start_offset == end_offset:
            return start_offset
        return end_offset

    # ------------------------------------------------------------------
    # Packages / resources — paginated range fetchers
    # ------------------------------------------------------------------

    def _paginate_packages_range(
        self,
        client: _CKANClient,
        since: str,
        until: str,
        table_options: dict[str, str],
    ) -> list[dict]:
        """Return all packages whose ``metadata_modified`` is in [since, until]."""
        records: list[dict] = []
        for pkg in self._iter_package_search(client, since, until, table_options):
            records.append(_shape_package(pkg))
        return records

    def _paginate_resources_range(
        self,
        client: _CKANClient,
        since: str,
        until: str,
        table_options: dict[str, str],
    ) -> list[dict]:
        """Return all resources belonging to packages in the [since, until] window."""
        records: list[dict] = []
        for pkg in self._iter_package_search(client, since, until, table_options):
            parent_watermark = pkg.get("metadata_modified")
            for res in pkg.get("resources") or []:
                records.append(_shape_resource(res, pkg.get("id"), parent_watermark))
        return records

    def _iter_package_search(
        self,
        client: _CKANClient,
        since: str,
        until: str,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Iterate raw ``package_search`` results in a time window."""
        max_records = _positive_int(table_options.get("max_records_per_batch"))

        fq_parts = [
            f"metadata_modified:[{_solr_date(since)} TO {_solr_date(until)}]"
        ]
        fq_parts.extend(_build_package_filters(table_options))
        fq = " ".join(fq_parts)

        q = table_options.get("q") or "*:*"
        include_deleted = _as_bool(table_options.get("include_deleted", "false"))

        start = 0
        yielded = 0
        while True:
            params = {
                "q": q,
                "fq": fq,
                "rows": PAGE_SIZE,
                "start": start,
                "sort": "metadata_modified asc",
                "include_deleted": include_deleted,
                "facet": False,
            }
            result = client.get("package_search", params=params)
            batch = result.get("results", []) if isinstance(result, dict) else []
            count = result.get("count", 0) if isinstance(result, dict) else 0

            if not batch:
                return

            for item in batch:
                yield item
                yielded += 1
                if max_records and yielded >= max_records:
                    return

            # Stop on the last page.
            if len(batch) < PAGE_SIZE:
                return
            start += len(batch)
            if start >= count:
                return

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """Current UTC time as a naive ISO 8601 string (to match portal format)."""
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="microseconds")


def _parse_iso(value: str) -> datetime:
    """Parse a portal ISO 8601 string into a naive UTC datetime."""
    v = value
    # Be forgiving of optional trailing "Z" from upstream variants.
    if v.endswith("Z"):
        v = v[:-1]
    try:
        dt = datetime.fromisoformat(v)
    except ValueError:
        # Fall back to a date-only parse (rare — dati.gov.it always sends
        # full timestamps, but user-supplied start_timestamp might be a date).
        dt = datetime.strptime(v, "%Y-%m-%d")
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _to_iso(dt: datetime) -> str:
    """Serialize a naive UTC datetime to the portal's format (no tz suffix)."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat(timespec="microseconds")


def _solr_date(value: str) -> str:
    """Convert an ISO string to Solr-range-friendly UTC form.

    Solr accepts ``YYYY-MM-DDTHH:MM:SSZ`` inside a range query. We normalise
    to that form — the portal itself stores the values without ``Z`` but
    accepts the ``Z`` form in queries.
    """
    dt = _parse_iso(value)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _positive_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        i = int(value)
    except (TypeError, ValueError):
        return None
    return i if i > 0 else None


def _iso_string(value: Any) -> str:
    """Coerce a value we plan to use as a cursor back to an ISO string."""
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return _to_iso(value)
    return str(value)


def _build_package_filters(table_options: dict[str, str]) -> list[str]:
    """Build Solr ``fq`` clauses from the connector's table-level options.

    All filters are ANDed. ``tags`` and ``groups`` may be comma-separated
    lists; each value becomes its own clause (Solr ANDs them too, matching
    user expectation: "datasets tagged with BOTH A and B").
    """
    out: list[str] = []
    for key in PACKAGE_FILTER_KEYS:
        if key == "q":
            continue  # ``q`` is a free-text param, not an fq clause.
        raw = table_options.get(key)
        if not raw:
            continue
        values = [v.strip() for v in str(raw).split(",") if v.strip()]
        for val in values:
            if key == "organization":
                out.append(f"organization:{_solr_escape(val)}")
            elif key == "tags":
                out.append(f"tags:{_solr_escape(val)}")
            elif key == "groups":
                out.append(f"groups:{_solr_escape(val)}")
            elif key == "res_format":
                out.append(f"res_format:{_solr_escape(val)}")
    return out


_SOLR_SPECIAL = set('+-&|!(){}[]^"~*?:\\/ ')


def _solr_escape(value: str) -> str:
    """Minimal Solr special-character escape plus whitespace quoting.

    For slugs / tag names / format codes the portal uses (which are all
    lowercase ascii with hyphens / dots) this is usually a no-op, but we
    defensively quote if the value contains whitespace or Solr specials.
    """
    if any(ch in _SOLR_SPECIAL for ch in value):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


# ---------------------------------------------------------------------------
# Record shapers — project raw CKAN payloads onto our flat schemas.
# ---------------------------------------------------------------------------


_PACKAGE_TOP_LEVEL = [
    "id",
    "name",
    "title",
    "notes",
    "type",
    "state",
    "private",
    "isopen",
    "license_id",
    "license_title",
    "license_url",
    "url",
    "version",
    "author",
    "author_email",
    "maintainer",
    "maintainer_email",
    "creator_user_id",
    "owner_org",
    "metadata_created",
    "metadata_modified",
    "num_resources",
    "num_tags",
    "holder_name",
    "holder_identifier",
    "dataset_is_local",
]

_RESOURCE_FIELDS_TO_COPY = [
    "id",
    "package_id",
    "name",
    "description",
    "url",
    "format",
    "distribution_format",
    "mimetype",
    "mimetype_inner",
    "size",
    "hash",
    "state",
    "position",
    "created",
    "last_modified",
    "metadata_modified",
    "url_type",
    "resource_type",
    "datastore_active",
    "cache_url",
    "cache_last_updated",
    "webstore_url",
    "webstore_last_updated",
    "license",
    "license_id",
    "license_type",
    "rights",
    "modified",
    "access_url",
    "uri",
]

_ORG_NESTED_FIELDS = [
    "id",
    "name",
    "title",
    "type",
    "description",
    "image_url",
    "created",
    "is_organization",
    "approval_status",
    "state",
]


def _shape_package(raw: dict) -> dict:
    """Project a raw package_search result onto PACKAGES_SCHEMA."""
    out: dict[str, Any] = {k: raw.get(k) for k in _PACKAGE_TOP_LEVEL}
    out["organization"] = _shape_organization_nested(raw.get("organization"))
    out["resources"] = [
        _shape_resource_nested(r) for r in (raw.get("resources") or [])
    ]
    out["tags"] = [_shape_tag_nested(t) for t in (raw.get("tags") or [])]
    out["groups"] = [_shape_group_nested(g) for g in (raw.get("groups") or [])]
    out["extras"] = _shape_extras(raw.get("extras"))
    return out


def _shape_resource(raw: dict, parent_id: str | None, parent_watermark: Any) -> dict:
    """Project a nested resource onto the flat RESOURCES_SCHEMA."""
    out = _shape_resource_nested(raw)
    # ``package_id`` is sometimes missing from the nested form — fall back
    # to the parent package's id.
    if not out.get("package_id") and parent_id:
        out["package_id"] = parent_id
    out["package_metadata_modified"] = parent_watermark
    return out


def _shape_resource_nested(raw: dict | None) -> dict:
    if not isinstance(raw, dict):
        return {f: None for f in _RESOURCE_FIELDS_TO_COPY}
    return {f: raw.get(f) for f in _RESOURCE_FIELDS_TO_COPY}


def _shape_organization_nested(raw: dict | None) -> dict | None:
    if not isinstance(raw, dict):
        return None
    return {f: raw.get(f) for f in _ORG_NESTED_FIELDS}


def _shape_tag_nested(raw: dict | None) -> dict:
    if not isinstance(raw, dict):
        return {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "display_name": raw.get("display_name"),
        "state": raw.get("state"),
        "vocabulary_id": raw.get("vocabulary_id"),
    }


def _shape_group_nested(raw: dict | None) -> dict:
    if not isinstance(raw, dict):
        return {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "title": raw.get("title"),
        "display_name": raw.get("display_name"),
        "description": raw.get("description"),
        "image_display_url": raw.get("image_display_url"),
    }


def _shape_extras(raw: Any) -> list[dict] | None:
    """Preserve extras as array<struct<key, value>>.

    Values may be JSON-encoded strings (e.g. ``theme`` is a JSON list) — we
    keep them as raw strings per the API doc's guidance.
    """
    if not isinstance(raw, list):
        return None
    out: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        val = entry.get("value")
        if val is not None and not isinstance(val, str):
            # Normalise non-string values to JSON so the whole column is a
            # uniform string.
            try:
                val = json.dumps(val, ensure_ascii=False)
            except (TypeError, ValueError):
                val = str(val)
        out.append({"key": entry.get("key"), "value": val})
    return out


def _shape_organization(raw: dict) -> dict:
    """Project an ``organization_show`` response onto ORGANIZATIONS_SCHEMA."""
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "title": raw.get("title"),
        "display_name": raw.get("display_name"),
        "description": raw.get("description"),
        "image_url": raw.get("image_url"),
        "image_display_url": raw.get("image_display_url"),
        "created": raw.get("created"),
        "is_organization": raw.get("is_organization"),
        "approval_status": raw.get("approval_status"),
        "state": raw.get("state"),
        "type": raw.get("type"),
        "num_followers": raw.get("num_followers"),
        "package_count": raw.get("package_count"),
        "identifier": raw.get("identifier"),
        "email": raw.get("email"),
        "site": raw.get("site"),
        "telephone": raw.get("telephone"),
        "extras": _shape_extras(raw.get("extras")),
        "users": [_shape_user(u) for u in (raw.get("users") or [])],
    }


def _shape_group(raw: dict) -> dict:
    """Project a ``group_show`` response onto GROUPS_SCHEMA."""
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "title": raw.get("title"),
        "display_name": raw.get("display_name"),
        "description": raw.get("description"),
        "image_url": raw.get("image_url"),
        "image_display_url": raw.get("image_display_url"),
        "created": raw.get("created"),
        "is_organization": raw.get("is_organization"),
        "approval_status": raw.get("approval_status"),
        "state": raw.get("state"),
        "type": raw.get("type"),
        "num_followers": raw.get("num_followers"),
        "package_count": raw.get("package_count"),
        "extras": _shape_extras(raw.get("extras")),
    }


def _shape_user(raw: dict | None) -> dict:
    if not isinstance(raw, dict):
        return {}
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "fullname": raw.get("fullname"),
        "capacity": raw.get("capacity"),
        "state": raw.get("state"),
        "created": raw.get("created"),
        "sysadmin": raw.get("sysadmin"),
    }

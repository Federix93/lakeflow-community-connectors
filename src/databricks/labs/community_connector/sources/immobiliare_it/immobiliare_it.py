"""LakeflowConnect connector for the immobiliare.it Insights — Market Explorer API.

Design decisions
----------------

* **HTTP client**: standard library ``urllib`` (no ``requests`` dependency) to
  match the convention used elsewhere in this repo (see ``dati_gov_it``).
* **Auth**: OAuth2 password grant via :class:`OAuthTokenManager`. The token
  is cached in memory; we re-grant only on 401 or when the cached token is
  about to expire. The auth docs explicitly warn against re-authenticating
  per request — doing so triggers a temporary IP ban.
* **Zone fan-out is the central design challenge.** Every history endpoint is
  scoped to a single ``(ty_zone, id_zone, contract, window, year, month,
  typology|cadastral_typology)`` combination. The connector enumerates all
  zones at the chosen ``zone_level`` once at driver init (cached), then
  fans out one POST per ``(zone × contract × typology|cadastral × month)``
  combination across Spark executors via :class:`SupportsPartitionedStream`.
* **Cursor**: stored as a single sortable integer ``year * 100 + month``
  (e.g. ``202412``). The first run uses ``start_year_month`` from
  ``table_options`` (default ``202001``). The driver advances the cursor
  one month per micro-batch by reading ``latest_offset`` from the
  ``/api/taxonomies/temporal`` endpoint.
* **Response flattening**: history responses are nested
  dict-of-arrays (``maintenance_status: {"1": [...], ...}``,
  ``rooms: {"1": [...], ...}``). We explode them into one row per
  ``(zone, contract, year, month, series_type, series_key)``. See
  ``immobiliare_it_schemas.py`` for the per-table schema.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from typing import Any, Iterable, Iterator, Sequence

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.immobiliare_it.immobiliare_it_auth import (
    OAuthTokenManager,
    create_token_manager_from_options,
)
from databricks.labs.community_connector.sources.immobiliare_it.immobiliare_it_schemas import (
    ADS_HISTORY_TABLE,
    ALL_CONTRACTS_LISTING,
    ALL_CONTRACTS_SALES,
    ALLOWED_WINDOWS,
    CDC_TABLES,
    DATA_REQUEST_TIMEOUT,
    DEFAULT_BASE_URL,
    DEFAULT_CADASTRAL_TYPOLOGIES,
    DEFAULT_NATION,
    DEFAULT_START_YEAR_MONTH_INT,
    DEFAULT_TYPOLOGIES_LISTING,
    DEFAULT_WINDOW,
    DEFAULT_ZONE_LEVEL,
    HISTORY_CURSOR_FIELD,
    HISTORY_ENDPOINTS,
    INITIAL_BACKOFF,
    LISTING_HISTORY_TABLES,
    MAX_BACKOFF,
    MAX_RETRIES,
    MUNICIPALITIES_TABLE,
    PRICE_HISTORY_TABLE,
    PROVINCES_TABLE,
    REGIONS_TABLE,
    RETRIABLE_STATUS_CODES,
    SALES_HISTORY_TABLES,
    SALES_PRICE_HISTORY_TABLE,
    SALES_VOLUME_HISTORY_TABLE,
    SEARCH_DATA_HISTORY_TABLE,
    SNAPSHOT_TABLES,
    SNAPSHOT_TY_ZONE,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    USER_AGENT,
    ZONE_LEVELS,
)


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


class _ImmobiliareClient:
    """urllib-based HTTP client wrapping the OAuth token manager.

    Handles 401 → re-auth → retry, plus 429/5xx exponential backoff.
    Designed to be cheap to construct so executors can build a fresh client
    inside :meth:`ImmobiliareItLakeflowConnect.read_partition` without any
    cross-executor state.
    """

    def __init__(self, base_url: str, token_manager: OAuthTokenManager) -> None:
        self._base_url = base_url.rstrip("/")
        self._tokens = token_manager

    # ------------------------------------------------------------------
    # GET / POST helpers
    # ------------------------------------------------------------------

    def get(self, path: str) -> dict:
        """GET ``{base_url}{path}`` and return the parsed JSON body."""
        url = f"{self._base_url}{path}"
        return self._request("GET", url, body=None)

    def post(self, path: str, body: dict[str, Any]) -> dict:
        """POST a JSON body and return the parsed response."""
        url = f"{self._base_url}{path}"
        return self._request("POST", url, body=body)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _request(
        self, method: str, url: str, body: dict[str, Any] | None
    ) -> dict:
        """Perform one request with retries, transparent 401 re-auth, and JSON decoding."""
        backoff = INITIAL_BACKOFF
        last_exc: Exception | None = None
        attempted_reauth = False

        encoded: bytes | None = None
        if body is not None:
            encoded = json.dumps(body).encode("utf-8")

        for attempt in range(MAX_RETRIES):
            try:
                req = urllib.request.Request(url, data=encoded, method=method)
                req.add_header(
                    "Authorization", f"Bearer {self._tokens.get_token()}"
                )
                if encoded is not None:
                    req.add_header("Content-Type", "application/json")
                req.add_header("Accept", "application/json")
                req.add_header("User-Agent", USER_AGENT)
                with urllib.request.urlopen(
                    req, timeout=DATA_REQUEST_TIMEOUT
                ) as resp:
                    raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as exc:
                last_exc = exc
                # 401 → token might be expired or revoked. Force-refresh and
                # retry once. If it happens again, surface the error.
                if exc.code == 401 and not attempted_reauth:
                    attempted_reauth = True
                    self._tokens.force_refresh()
                    continue
                if exc.code in RETRIABLE_STATUS_CODES and attempt < MAX_RETRIES - 1:
                    time.sleep(min(backoff, MAX_BACKOFF))
                    backoff *= 2
                    continue
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", errors="replace")[:500]
                except Exception:
                    pass
                raise RuntimeError(
                    f"HTTP {exc.code} {method} {url}: {detail}"
                ) from exc
            except urllib.error.URLError as exc:
                last_exc = exc
                if attempt == MAX_RETRIES - 1:
                    raise RuntimeError(
                        f"Network error {method} {url}: {exc}"
                    ) from exc
                time.sleep(min(backoff, MAX_BACKOFF))
                backoff *= 2

        raise RuntimeError(f"Exhausted retries {method} {url}: {last_exc}")


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class ImmobiliareItLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for the Market Explorer service.

    Extends :class:`SupportsPartitionedStream` so that history fan-out (zone
    × month) is parallelised across Spark executors. Snapshot tables
    (``regions`` / ``provinces`` / ``municipalities``) opt out of partitioning
    via :meth:`is_partitioned` and use the single-driver ``read_table`` path.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._base_url = (
            (options.get("base_url") or "").strip().rstrip("/") or DEFAULT_BASE_URL
        )
        self._tokens = create_token_manager_from_options(options, DEFAULT_BASE_URL)
        self._client = _ImmobiliareClient(self._base_url, self._tokens)

        # Caches keyed by zone_level — populated lazily on first use.
        # ``_zones_cache`` holds the raw geo-list response items so that
        # snapshot tables and history fan-out share one fetch per level.
        self._zones_cache: dict[str, list[dict]] = {}

        # Init-time ceiling: the latest (year, month) we will fan out to in
        # this driver instance. Discovered lazily via ``/api/taxonomies/temporal``
        # the first time it's needed; cached thereafter so each micro-batch
        # uses a stable end_offset.
        self._latest_year_month: int | None = None

    # ------------------------------------------------------------------
    # LakeflowConnect surface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Static object list — no discovery endpoint exists for the table set."""
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
        # Copy so callers cannot mutate our module-level dict.
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read path.

        For snapshot tables this fetches the full geo list. For CDC tables
        this is the fallback (used when partitioning is disabled or when
        Spark calls through ``simpleStreamReader``) — it sequentially walks
        the zone × month cartesian product for a single month window.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name in SNAPSHOT_TABLES:
            return self._read_snapshot(table_name)
        return self._read_history_sequential(
            table_name, start_offset or {}, table_options
        )

    # ------------------------------------------------------------------
    # SupportsPartitionedStream surface
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """Only history tables benefit from zone-level fan-out.

        Snapshot tables are a single small list endpoint each (regions: 20,
        provinces: 110, municipalities: ~7900) and don't benefit from
        partitioning — they fall back to ``simpleStreamReader``.
        """
        return table_name in CDC_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the current high-water mark for the streaming cursor.

        The watermark is the latest available ``(year, month)`` for the
        configured window, discovered via the temporal taxonomy endpoint.
        We cache the result on ``self`` so successive micro-batches in the
        same driver use a stable ceiling — that's important so the stream
        terminates at end-of-data rather than chasing a moving target.
        """
        self._validate_table(table_name)
        if table_name not in CDC_TABLES:
            return {}
        latest = self._get_latest_year_month()
        return {"cursor": latest}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split (start, end] cursor range × zone list into partitions.

        For batch reads, ``start_offset`` / ``end_offset`` are both ``None``
        → cover the full configurable history. For streaming micro-batches
        they delimit the new range.

        Each returned partition descriptor carries everything ``read_partition``
        needs: target ``(year, month)``, zone, contract, typology/cadastral,
        and the static request shape. Descriptors are intentionally small
        (a handful of primitives) so JSON serialisation across the driver →
        executor boundary stays cheap.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name in SNAPSHOT_TABLES:
            # Single descriptor — whole table. Only exercised if the framework
            # ignores ``is_partitioned`` and asks for partitions anyway.
            return [{"mode": "snapshot"}]

        start_ym = self._extract_cursor(start_offset)
        end_ym = self._extract_cursor(end_offset)

        # Resolve floor / ceiling for batch mode.
        if start_ym is None:
            start_ym = self._configured_start_year_month(table_options) - 1
            # Subtract 1 so the first month is included (range is exclusive
            # on start_ym).
        if end_ym is None:
            end_ym = self._get_latest_year_month()

        if start_ym >= end_ym:
            return []

        months = _enumerate_year_months(start_ym, end_ym)

        # Resolve fan-out filters.
        zone_level = (
            table_options.get("zone_level")
            or DEFAULT_ZONE_LEVEL[table_name]
        ).strip().lower()
        if zone_level not in ZONE_LEVELS:
            raise ValueError(
                f"zone_level={zone_level!r} is not a valid taxonomy code; "
                f"expected one of {ZONE_LEVELS}"
            )

        zones = self._resolve_zone_ids(zone_level, table_options)
        if not zones:
            return []

        contracts = _split_csv_int(
            table_options.get("contract")
            or (
                ALL_CONTRACTS_SALES
                if table_name in SALES_HISTORY_TABLES
                else ALL_CONTRACTS_LISTING
            )
        )
        nation = (table_options.get("nation") or DEFAULT_NATION).strip().upper()
        window = (table_options.get("window") or DEFAULT_WINDOW).strip().upper()
        if window not in ALLOWED_WINDOWS:
            raise ValueError(
                f"window={window!r} not allowed; expected one of {ALLOWED_WINDOWS}"
            )

        if table_name in SALES_HISTORY_TABLES:
            cadastral = _split_csv_str(
                table_options.get("cadastral_typology")
                or DEFAULT_CADASTRAL_TYPOLOGIES
            )
            typologies: list[Any] = list(cadastral)
            typology_kind = "cadastral_typology"
        else:
            integer_typologies = _split_csv_int(
                table_options.get("typology")
                or DEFAULT_TYPOLOGIES_LISTING
            )
            typologies = list(integer_typologies)
            typology_kind = "typology"

        # Cartesian product → one partition per (zone × contract × typology × month).
        # That's a lot of partitions for a multi-month backfill at municipality
        # level, but each partition is a single API call so this maps 1:1 to
        # Spark task granularity.
        partitions: list[dict] = []
        for ym in months:
            year, month = _year_month_split(ym)
            for zone_id in zones:
                for contract in contracts:
                    for typ in typologies:
                        partition: dict[str, Any] = {
                            "ty_zone": zone_level,
                            "id_zone": zone_id,
                            "year": year,
                            "month": month,
                            "year_month": ym,
                            "contract": contract,
                            "window": window,
                            "nation": nation,
                            typology_kind: typ,
                        }
                        partitions.append(partition)
        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one partition on a Spark executor.

        Self-contained: re-creates the OAuth manager and HTTP client from
        ``self.options`` so the call doesn't rely on driver-side caches
        (``self._zones_cache`` etc. won't be populated on the executor).
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        # Snapshot fallback (only hit if framework ignores is_partitioned).
        if partition.get("mode") == "snapshot":
            records, _ = self._read_snapshot(table_name)
            return records

        # Build a fresh client per executor task. urllib clients are cheap
        # and the token manager amortises the OAuth grant across all partition
        # reads on this executor (subsequent tasks on the same JVM may share
        # the manager via Python module caching).
        tokens = create_token_manager_from_options(self.options, DEFAULT_BASE_URL)
        base_url = (
            (self.options.get("base_url") or "").strip().rstrip("/") or DEFAULT_BASE_URL
        )
        client = _ImmobiliareClient(base_url, tokens)

        body = _build_history_request_body(table_name, partition)
        endpoint = HISTORY_ENDPOINTS[table_name]
        try:
            response = client.post(endpoint, body)
        except RuntimeError as exc:
            # 404/422 for a zone with no data is expected at the long tail of
            # the geo distribution. Skip the partition rather than fail the
            # whole micro-batch.
            msg = str(exc)
            if " 404 " in msg or " 422 " in msg:
                return iter([])
            raise

        items = (response or {}).get("items") or []
        return iter(_shape_history_response(table_name, partition, items))

    # ------------------------------------------------------------------
    # Snapshot reading
    # ------------------------------------------------------------------

    def _read_snapshot(
        self, table_name: str
    ) -> tuple[Iterator[dict], dict]:
        """Read a full geo taxonomy table.

        ``regions`` / ``provinces`` / ``municipalities`` map 1:1 to the same
        ``GET /api/taxonomies/geo/IT/ty_zone/{level}`` call used during zone
        enumeration — we reuse the shared cache so that listing-side and
        history-fan-out paths only fetch each level once per driver instance.
        """
        ty_zone = SNAPSHOT_TY_ZONE[table_name]
        items = self._fetch_zone_list(ty_zone)
        records = [_shape_geo_item(item) for item in items]
        return iter(records), {}

    # ------------------------------------------------------------------
    # Sequential history reader (used by read_table fallback)
    # ------------------------------------------------------------------

    def _read_history_sequential(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Sequential single-driver fan-out advancing one month per call.

        This is the ``simpleStreamReader`` path. It is significantly slower
        than the partitioned path (one POST at a time, no executor parallelism)
        and is intended only for low-volume backfills or when a user opts
        out of partitioning.
        """
        end_ym = self._get_latest_year_month()
        start_ym = self._extract_cursor(start_offset)
        if start_ym is None:
            start_ym = self._configured_start_year_month(table_options) - 1

        if start_ym >= end_ym:
            return iter([]), start_offset or {"cursor": end_ym}

        # One month per micro-batch — keeps the fan-out bounded so the call
        # never balloons. Streaming partitioned path is preferred for backfills.
        next_ym = _next_year_month(start_ym)
        max_records = _positive_int(table_options.get("max_records_per_batch"))

        partitions = self.get_partitions(
            table_name,
            table_options,
            start_offset={"cursor": start_ym},
            end_offset={"cursor": next_ym},
        )

        records: list[dict] = []
        for partition in partitions:
            for record in self.read_partition(table_name, partition, table_options):
                records.append(record)
                if max_records and len(records) >= max_records:
                    # Hit the bounded-batch ceiling — return without advancing
                    # past the current month so we don't lose the rest of the
                    # month. CDC upserts on subsequent runs will dedupe the
                    # already-emitted rows.
                    return iter(records), {"cursor": start_ym}

        end_offset = {"cursor": next_ym}
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Zone enumeration
    # ------------------------------------------------------------------

    def _resolve_zone_ids(
        self, zone_level: str, table_options: dict[str, str]
    ) -> list[str]:
        """Resolve zone IDs honouring an optional ``zone_filter`` table option."""
        explicit = table_options.get("zone_filter")
        if explicit:
            ids = [v.strip() for v in str(explicit).split(",") if v.strip()]
            if ids:
                return ids
        items = self._fetch_zone_list(zone_level)
        return [str(item.get("id_zone")) for item in items if item.get("id_zone")]

    def _fetch_zone_list(self, zone_level: str) -> list[dict]:
        """Fetch (and cache) the flat geo list for a given level.

        Driver-side cache only — executors call this only via
        ``_read_snapshot`` (i.e. the snapshot fallback path) and rarely
        re-enter on history fan-out because partitions carry their zone IDs.
        """
        if zone_level in self._zones_cache:
            return self._zones_cache[zone_level]
        nation = DEFAULT_NATION
        path = f"/api/taxonomies/geo/{nation}/ty_zone/{zone_level}"
        body = self._client.get(path)
        items = (body or {}).get("items") or []
        if not isinstance(items, list):
            items = []
        self._zones_cache[zone_level] = items
        return items

    # ------------------------------------------------------------------
    # Cursor helpers
    # ------------------------------------------------------------------

    def _get_latest_year_month(self) -> int:
        """Discover the latest available (year, month) once per driver.

        Calls ``GET /api/taxonomies/temporal`` which returns availability
        flags per period. We pick the most recent ``year_month`` that has
        any data flag set. If the endpoint is unreachable for any reason,
        fall back to the current month — with a 1-month safety lookback —
        so the connector still makes progress.
        """
        if self._latest_year_month is not None:
            return self._latest_year_month

        try:
            body = self._client.get("/api/taxonomies/temporal")
        except RuntimeError:
            self._latest_year_month = _current_year_month_int(lookback_months=1)
            return self._latest_year_month

        latest = _extract_latest_period(body)
        if latest is None:
            latest = _current_year_month_int(lookback_months=1)
        self._latest_year_month = latest
        return latest

    @staticmethod
    def _extract_cursor(offset: dict | None) -> int | None:
        if not offset:
            return None
        cursor = offset.get("cursor")
        if cursor is None:
            return None
        try:
            return int(cursor)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _configured_start_year_month(table_options: dict[str, str]) -> int:
        """Resolve the user-configured backfill anchor to an integer cursor."""
        raw = table_options.get("start_year_month")
        if not raw:
            return DEFAULT_START_YEAR_MONTH_INT
        try:
            cleaned = str(raw).replace("-", "").strip()
            value = int(cleaned)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"start_year_month={raw!r} is not parseable as YYYYMM"
            ) from exc
        if value < 200001 or value > 999912:
            raise ValueError(
                f"start_year_month={raw!r} out of range; expected YYYYMM (200001..999912)"
            )
        if not (1 <= value % 100 <= 12):
            raise ValueError(
                f"start_year_month={raw!r} has invalid month component"
            )
        return value

    # ------------------------------------------------------------------
    # Misc
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


def _enumerate_year_months(start_exclusive: int, end_inclusive: int) -> list[int]:
    """Return the list of year_month integers in (start_exclusive, end_inclusive].

    Both inputs use the ``YYYYMM`` integer encoding.
    """
    out: list[int] = []
    current = _next_year_month(start_exclusive)
    while current <= end_inclusive:
        out.append(current)
        current = _next_year_month(current)
    return out


def _next_year_month(value: int) -> int:
    year = value // 100
    month = value % 100
    if month >= 12:
        return (year + 1) * 100 + 1
    return year * 100 + month + 1


def _year_month_split(value: int) -> tuple[int, int]:
    return value // 100, value % 100


def _current_year_month_int(lookback_months: int = 0) -> int:
    today = date.today()
    year = today.year
    month = today.month
    while lookback_months > 0:
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        lookback_months -= 1
    return year * 100 + month


def _split_csv_int(raw: str) -> list[int]:
    out: list[int] = []
    for piece in str(raw).split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            out.append(int(piece))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Could not parse {piece!r} as integer in CSV value {raw!r}"
            ) from exc
    return out


def _split_csv_str(raw: str) -> list[str]:
    return [piece.strip() for piece in str(raw).split(",") if piece.strip()]


def _positive_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        i = int(value)
    except (TypeError, ValueError):
        return None
    return i if i > 0 else None


def _extract_latest_period(body: dict | None) -> int | None:
    """Walk ``/api/taxonomies/temporal`` response to find the newest period.

    Response shape (per api_doc.md): four lists ``mesi``, ``trimestri``,
    ``semestri``, ``anni`` — each entry has ``year``, ``month``, and a few
    boolean availability flags. We look for the most recent entry across
    ``mesi`` (monthly) since this connector defaults to ``window=1M``.
    """
    if not isinstance(body, dict):
        return None
    items = body.get("items")
    if isinstance(items, list) and items:
        body = items[0] if isinstance(items[0], dict) else body
    candidates = []
    for key in ("mesi", "trimestri", "semestri", "anni"):
        seq = body.get(key) if isinstance(body, dict) else None
        if not isinstance(seq, list):
            continue
        for entry in seq:
            if not isinstance(entry, dict):
                continue
            year = entry.get("year") or entry.get("anno")
            month = entry.get("month") or entry.get("mese") or 12
            if not isinstance(year, int) or not isinstance(month, int):
                continue
            if 1 <= month <= 12 and year >= 1900:
                candidates.append(year * 100 + month)
    if not candidates:
        return None
    return max(candidates)


# ---------------------------------------------------------------------------
# Request body builders & response shapers
# ---------------------------------------------------------------------------


def _build_history_request_body(table_name: str, partition: dict) -> dict[str, Any]:
    """Project a partition descriptor onto the JSON body of a history POST.

    Centralised so both the partitioned executor path and the sequential
    fallback share the exact same wire format. ``typology`` is integer for
    listing-side endpoints, ``cadastral_typology`` is a string code for the
    sales endpoints.
    """
    body: dict[str, Any] = {
        "ty_zone": partition["ty_zone"],
        "id_zone": str(partition["id_zone"]),
        "window": partition["window"],
        "contract": int(partition["contract"]),
        "year": int(partition["year"]),
        "month": int(partition["month"]),
        "nation": partition.get("nation") or DEFAULT_NATION,
    }
    if table_name in LISTING_HISTORY_TABLES:
        if partition.get("typology") is not None:
            body["typology"] = int(partition["typology"])
    elif table_name in SALES_HISTORY_TABLES:
        cadastral = partition.get("cadastral_typology")
        if cadastral:
            body["cadastral_typology"] = str(cadastral)
    return body


def _shape_history_response(
    table_name: str, partition: dict, items: list[dict]
) -> Iterable[dict]:
    """Explode a nested history response into flat rows.

    Each item in ``items`` (typically a single-element list) carries one or
    more series — a flat array (e.g. ``price_sqm_avg``) or a dict-of-arrays
    keyed by series_key (e.g. ``maintenance_status: {"1": [...], ...}``).
    We yield one row per ``(series_type, series_key, year, month)`` tuple.
    """
    for item in items or []:
        if not isinstance(item, dict):
            continue
        if table_name == PRICE_HISTORY_TABLE or table_name == ADS_HISTORY_TABLE:
            yield from _shape_price_like(table_name, partition, item)
        elif table_name == SEARCH_DATA_HISTORY_TABLE:
            yield from _shape_search_data(table_name, partition, item)
        elif table_name == SALES_PRICE_HISTORY_TABLE:
            yield from _shape_sales_price(partition, item)
        elif table_name == SALES_VOLUME_HISTORY_TABLE:
            yield from _shape_sales_volume(partition, item)


def _base_history_row(partition: dict) -> dict[str, Any]:
    """Common scalar columns derived from the partition descriptor."""
    return {
        "ty_zone": partition["ty_zone"],
        "id_zone": str(partition["id_zone"]),
        "nation": partition.get("nation") or DEFAULT_NATION,
        "contract": int(partition["contract"]),
        "window": partition["window"],
    }


def _shape_price_like(
    table_name: str, partition: dict, item: dict
) -> Iterable[dict]:
    """price_history / ads_history share a layout: (price_sqm_avg | maintenance_status | rooms)."""
    typology = partition.get("typology")
    base = _base_history_row(partition)
    base["typology"] = int(typology) if typology is not None else None

    # Flat series — series_key is "_" (no inner segmentation).
    for entry in item.get("price_sqm_avg") or []:
        yield from _emit_price_metric(base, "price_sqm_avg", "_", entry)

    for series_type in ("maintenance_status", "rooms"):
        series = item.get(series_type)
        if not isinstance(series, dict):
            continue
        for key, entries in series.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                yield from _emit_price_metric(base, series_type, str(key), entry)


def _emit_price_metric(
    base: dict[str, Any],
    series_type: str,
    series_key: str,
    entry: dict | None,
) -> Iterable[dict]:
    if not isinstance(entry, dict):
        return
    year = entry.get("year")
    month = entry.get("month")
    if not isinstance(year, int) or not isinstance(month, int):
        return
    out = dict(base)
    out["series_type"] = series_type
    out["series_key"] = series_key
    out["year"] = year
    out["month"] = month
    out[HISTORY_CURSOR_FIELD] = year * 100 + month
    out["price_avg"] = _to_float(entry.get("price_avg"))
    out["price_avgin"] = _to_float(entry.get("price_avgin"))
    out["price_avgout"] = _to_float(entry.get("price_avgout"))
    yield out


def _shape_search_data(
    table_name: str, partition: dict, item: dict
) -> Iterable[dict]:
    """search_data_history: heterogeneous series, all flattened to one schema.

    Each top-level key is a different series; we map them to ``series_type``
    and emit different metric columns based on the series' inner shape.
    """
    typology = partition.get("typology")
    base = _base_history_row(partition)
    base["typology"] = int(typology) if typology is not None else None

    # ``conversion_rate`` and ``price_sqm_search_avg`` are flat arrays of
    # {year, month, value}.
    for series_type in ("conversion_rate", "price_sqm_search_avg"):
        for entry in item.get(series_type) or []:
            yield from _emit_search_value_row(base, series_type, "_", entry)

    # ``maintenance_status`` and ``rooms`` are dict-of-arrays keyed by code
    # with {year, month, value} entries.
    for series_type in ("maintenance_status", "rooms"):
        series = item.get(series_type)
        if not isinstance(series, dict):
            continue
        for key, entries in series.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                yield from _emit_search_value_row(
                    base, series_type, str(key), entry
                )

    # ``pc_rooms`` is a flat array but each entry mixes the per-period
    # totals (qt-raw / qt-minrooms) with per-room sub-objects keyed by
    # numeric strings (e.g. "1", "2", "3"). We split each entry into:
    #   - one "totals" row with series_key="_" and qt_raw / qt_minrooms set
    #   - one row per inner room key with that key as series_key and the
    #     room's pc-raw / qt-raw populated
    for entry in item.get("pc_rooms") or []:
        if not isinstance(entry, dict):
            continue
        year = entry.get("year")
        month = entry.get("month")
        if not isinstance(year, int) or not isinstance(month, int):
            continue
        totals = dict(base)
        totals.update(
            {
                "series_type": "pc_rooms",
                "series_key": "_",
                "year": year,
                "month": month,
                HISTORY_CURSOR_FIELD: year * 100 + month,
                "qt_raw": _to_float(entry.get("qt-raw")),
                "qt_minrooms": _to_float(entry.get("qt-minrooms")),
            }
        )
        yield totals
        for k, v in entry.items():
            if k in {"year", "month", "qt-raw", "qt-minrooms"}:
                continue
            if not isinstance(v, dict):
                continue
            row = dict(base)
            row.update(
                {
                    "series_type": "pc_rooms",
                    "series_key": str(k),
                    "year": year,
                    "month": month,
                    HISTORY_CURSOR_FIELD: year * 100 + month,
                    "pc_raw": _to_float(v.get("pc-raw")),
                    "qt_raw": _to_float(v.get("qt-raw")),
                }
            )
            yield row

    # ``res`` is a flat array of {year, month, qt-raw, pc-1floor, pc-1typology,
    # pc-garden, pc-minrooms, pc-status, pc-terrace}.
    for entry in item.get("res") or []:
        if not isinstance(entry, dict):
            continue
        year = entry.get("year")
        month = entry.get("month")
        if not isinstance(year, int) or not isinstance(month, int):
            continue
        row = dict(base)
        row.update(
            {
                "series_type": "res",
                "series_key": "_",
                "year": year,
                "month": month,
                HISTORY_CURSOR_FIELD: year * 100 + month,
                "qt_raw": _to_float(entry.get("qt-raw")),
                "pc_1floor": _to_float(entry.get("pc-1floor")),
                "pc_1typology": _to_float(entry.get("pc-1typology")),
                "pc_garden": _to_float(entry.get("pc-garden")),
                "pc_minrooms": _to_float(entry.get("pc-minrooms")),
                "pc_status": _to_float(entry.get("pc-status")),
                "pc_terrace": _to_float(entry.get("pc-terrace")),
            }
        )
        yield row


def _emit_search_value_row(
    base: dict[str, Any],
    series_type: str,
    series_key: str,
    entry: dict | None,
) -> Iterable[dict]:
    if not isinstance(entry, dict):
        return
    year = entry.get("year")
    month = entry.get("month")
    if not isinstance(year, int) or not isinstance(month, int):
        return
    out = dict(base)
    out["series_type"] = series_type
    out["series_key"] = series_key
    out["year"] = year
    out["month"] = month
    out[HISTORY_CURSOR_FIELD] = year * 100 + month
    out["value"] = _to_float(entry.get("value"))
    yield out


def _shape_sales_price(partition: dict, item: dict) -> Iterable[dict]:
    """sales_price_history: a single series ``compravendite_price_sqm_avg``.

    The history response only carries the avg series (per api_doc.md note);
    the non-history endpoint additionally returns min/max/cadastral arrays
    which we do not surface here.
    """
    cadastral = partition.get("cadastral_typology")
    base = _base_history_row(partition)
    base["cadastral_typology"] = str(cadastral) if cadastral else None
    base["series_type"] = "compravendite_price_sqm_avg"
    base["series_key"] = "_"

    for entry in item.get("compravendite_price_sqm_avg") or []:
        if not isinstance(entry, dict):
            continue
        year = entry.get("year")
        month = entry.get("month")
        if not isinstance(year, int) or not isinstance(month, int):
            continue
        out = dict(base)
        out["year"] = year
        out["month"] = month
        out[HISTORY_CURSOR_FIELD] = year * 100 + month
        out["price_avg"] = _to_float(entry.get("price_avg"))
        yield out


def _shape_sales_volume(partition: dict, item: dict) -> Iterable[dict]:
    """sales_volume_history: a single series ``sales_qtraw`` with ``qtraw``."""
    cadastral = partition.get("cadastral_typology")
    base = _base_history_row(partition)
    base["cadastral_typology"] = str(cadastral) if cadastral else None
    base["series_type"] = "sales_qtraw"
    base["series_key"] = "_"

    for entry in item.get("sales_qtraw") or []:
        if not isinstance(entry, dict):
            continue
        year = entry.get("year")
        month = entry.get("month")
        if not isinstance(year, int) or not isinstance(month, int):
            continue
        out = dict(base)
        out["year"] = year
        out["month"] = month
        out[HISTORY_CURSOR_FIELD] = year * 100 + month
        out["qtraw"] = _to_float(entry.get("qtraw"))
        yield out


def _shape_geo_item(item: dict | None) -> dict:
    """Project a geo taxonomy item onto GEO_ZONE_SCHEMA."""
    if not isinstance(item, dict):
        return {
            "id_zone": "",
            "id_reg": None,
            "nome": None,
            "nome_reg": None,
        }
    return {
        "id_zone": str(item.get("id_zone") or item.get("id") or ""),
        "id_reg": _to_str(item.get("id_reg")),
        "nome": _to_str(item.get("nome")),
        "nome_reg": _to_str(item.get("nome_reg")),
    }


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)

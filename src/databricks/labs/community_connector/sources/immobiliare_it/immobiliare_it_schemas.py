"""Schemas, metadata, and constants for the immobiliare.it Insights connector.

The Market Explorer service exposes eight tables — five CDC history tables
backed by zone-fanned-out POST endpoints, and three snapshot taxonomy tables
that simply list zones at a given level.

History responses are nested dict-of-arrays (e.g. ``rooms`` keyed by
``"1".."m5"``, ``maintenance_status`` keyed by ``"1".."4"``). The connector
flattens these into one row per ``(zone, contract, year, month, series_type,
series_key)`` tuple — see ``immobiliare_it.py`` for the shaping logic and
``api_doc.md`` for the underlying response shape.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default to the sandbox base URL — production access is gated and most users
# will start in dev. Overridable via ``base_url`` connection option.
DEFAULT_BASE_URL = "https://ws-osservatorio-dev.realitycs.it"

# Identify ourselves on every HTTP call.
USER_AGENT = (
    "lakeflow-community-connectors/0.1 immobiliare-it "
    "(+https://github.com/databrickslabs/lakeflow-community-connectors)"
)

# HTTP timeouts (seconds). Token requests are usually fast; data calls can be
# slower on large zones.
TOKEN_REQUEST_TIMEOUT = 30
DATA_REQUEST_TIMEOUT = 60

# Retry policy. The auth docs warn against re-authenticating too aggressively
# (IP ban risk), so we cache the token and only re-grant on 401. 429/5xx are
# retried with exponential backoff.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 0.25  # seconds; doubles each retry; capped at MAX_BACKOFF
MAX_BACKOFF = 30.0

# Refresh the token a minute before its documented expiry to avoid racing the
# clock when the server's clock skew is small.
TOKEN_REFRESH_BUFFER_SECONDS = 60

# Default fan-out parameters per CDC table. ``window`` controls the time
# granularity of the API response. ``zone_level`` controls how broadly we
# fan out — ``com`` (municipality, ~7900 zones) is the most useful default
# for the listing-side endpoints; sales endpoints are less granular and
# default to ``pro`` (province) to keep the call count manageable.
DEFAULT_WINDOW = "1M"
DEFAULT_NATION = "IT"

# Anchor cursor: data availability starts ~2019–2020 depending on table.
# Using 202001 keeps the initial backfill bounded and inside the documented
# window. Stored as integer ``year * 100 + month`` so the cursor is sortable.
DEFAULT_START_YEAR_MONTH_INT = 202001

# Cursor field name on the flattened history rows. We use a synthetic
# ``year_month`` column (``year * 100 + month``) because Spark's checkpointed
# offsets need a single comparable scalar.
HISTORY_CURSOR_FIELD = "year_month"

# Zone enumeration is shared between snapshot tables and history fan-out.
# We cache the per-level lookup on the driver-side connector instance to
# avoid re-fetching the ~7900-row municipality list on every micro-batch.
ZONE_LEVELS = ("reg", "pro", "com", "macro", "micro")

# All known contract codes ("1" sale, "2" rental). Sales endpoints reject
# rental in practice but we still let the user filter; the default for
# sales tables is just sale.
ALL_CONTRACTS_LISTING = "1,2"
ALL_CONTRACTS_SALES = "1"

# Default property typology codes for listing-side history tables.
# Matches the documented residential set (api_doc.md §"Property Typologies").
DEFAULT_TYPOLOGIES_LISTING = "4,5,7,10,11,12,13,31"
# Cadastral typologies (sales tables). A1..A11.
DEFAULT_CADASTRAL_TYPOLOGIES = "A1,A2,A3,A4,A5,A6,A7,A8,A9,A11"

# Allowed window values per the taxonomy.
ALLOWED_WINDOWS = ("1M", "3M", "6M", "12M")

# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------

PRICE_HISTORY_TABLE = "price_history"
ADS_HISTORY_TABLE = "ads_history"
SEARCH_DATA_HISTORY_TABLE = "search_data_history"
SALES_PRICE_HISTORY_TABLE = "sales_price_history"
SALES_VOLUME_HISTORY_TABLE = "sales_volume_history"
REGIONS_TABLE = "regions"
PROVINCES_TABLE = "provinces"
MUNICIPALITIES_TABLE = "municipalities"

CDC_TABLES = {
    PRICE_HISTORY_TABLE,
    ADS_HISTORY_TABLE,
    SEARCH_DATA_HISTORY_TABLE,
    SALES_PRICE_HISTORY_TABLE,
    SALES_VOLUME_HISTORY_TABLE,
}

# History tables that use the integer ``typology`` parameter.
LISTING_HISTORY_TABLES = {
    PRICE_HISTORY_TABLE,
    ADS_HISTORY_TABLE,
    SEARCH_DATA_HISTORY_TABLE,
}

# History tables that use the string ``cadastral_typology`` parameter (A1..A11).
SALES_HISTORY_TABLES = {
    SALES_PRICE_HISTORY_TABLE,
    SALES_VOLUME_HISTORY_TABLE,
}

SNAPSHOT_TABLES = {
    REGIONS_TABLE,
    PROVINCES_TABLE,
    MUNICIPALITIES_TABLE,
}

SUPPORTED_TABLES: list[str] = [
    PRICE_HISTORY_TABLE,
    ADS_HISTORY_TABLE,
    SEARCH_DATA_HISTORY_TABLE,
    SALES_PRICE_HISTORY_TABLE,
    SALES_VOLUME_HISTORY_TABLE,
    REGIONS_TABLE,
    PROVINCES_TABLE,
    MUNICIPALITIES_TABLE,
]

# Per-snapshot-table mapping from table name to the ``ty_zone`` taxonomy code.
SNAPSHOT_TY_ZONE: dict[str, str] = {
    REGIONS_TABLE: "reg",
    PROVINCES_TABLE: "pro",
    MUNICIPALITIES_TABLE: "com",
}

# Per-history-table mapping from table name to the API endpoint path.
HISTORY_ENDPOINTS: dict[str, str] = {
    PRICE_HISTORY_TABLE: "/api/price/history",
    ADS_HISTORY_TABLE: "/api/ads/history",
    SEARCH_DATA_HISTORY_TABLE: "/api/search-data/history",
    SALES_PRICE_HISTORY_TABLE: "/api/sales/price/history",
    SALES_VOLUME_HISTORY_TABLE: "/api/sales/volume/history",
}

# Default ``zone_level`` per history table — listing-side endpoints support
# per-municipality data; sales endpoints are AdE-driven and only emit at the
# province level reliably.
DEFAULT_ZONE_LEVEL: dict[str, str] = {
    PRICE_HISTORY_TABLE: "com",
    ADS_HISTORY_TABLE: "com",
    SEARCH_DATA_HISTORY_TABLE: "com",
    SALES_PRICE_HISTORY_TABLE: "pro",
    SALES_VOLUME_HISTORY_TABLE: "pro",
}

# ---------------------------------------------------------------------------
# Snapshot table schemas — flat geo taxonomy
# ---------------------------------------------------------------------------

GEO_ZONE_SCHEMA = StructType(
    [
        StructField("id_zone", StringType(), nullable=False),
        StructField("id_reg", StringType(), nullable=True),
        StructField("nome", StringType(), nullable=True),
        StructField("nome_reg", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# History schemas
#
# All five history tables flatten their nested dict-of-arrays response into
# rows keyed by ``(ty_zone, id_zone, contract, window, typology|cadastral,
# series_type, series_key, year, month)``. The metric columns differ per
# table — see comments below.
# ---------------------------------------------------------------------------

# Common columns present on every flattened history row.
_COMMON_HISTORY_FIELDS: list[StructField] = [
    StructField("ty_zone", StringType(), nullable=False),
    StructField("id_zone", StringType(), nullable=False),
    StructField("nation", StringType(), nullable=True),
    StructField("contract", IntegerType(), nullable=False),
    StructField("window", StringType(), nullable=False),
    StructField("series_type", StringType(), nullable=False),
    StructField("series_key", StringType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    # Synthetic sortable cursor: year * 100 + month.
    StructField(HISTORY_CURSOR_FIELD, IntegerType(), nullable=False),
]

# price_history / ads_history share field names. ``price_avg`` /
# ``price_avgin`` / ``price_avgout`` per series record.
_PRICE_LIKE_METRICS: list[StructField] = [
    StructField("price_avg", DoubleType(), nullable=True),
    StructField("price_avgin", DoubleType(), nullable=True),
    StructField("price_avgout", DoubleType(), nullable=True),
]

PRICE_HISTORY_SCHEMA = StructType(
    _COMMON_HISTORY_FIELDS
    + [
        StructField("typology", IntegerType(), nullable=True),
    ]
    + _PRICE_LIKE_METRICS
)

ADS_HISTORY_SCHEMA = PRICE_HISTORY_SCHEMA  # identical layout

# search_data_history is the most heterogeneous — different series carry
# different metric column sets. We model the union as nullable columns so
# any single row populates only the columns relevant to its series_type.
SEARCH_DATA_HISTORY_SCHEMA = StructType(
    _COMMON_HISTORY_FIELDS
    + [
        StructField("typology", IntegerType(), nullable=True),
        # value: used by conversion_rate, maintenance_status, rooms,
        # price_sqm_search_avg.
        StructField("value", DoubleType(), nullable=True),
        # pc_rooms / res series: raw counts and percentage breakdowns.
        StructField("qt_raw", DoubleType(), nullable=True),
        StructField("qt_minrooms", DoubleType(), nullable=True),
        StructField("pc_raw", DoubleType(), nullable=True),
        StructField("pc_1floor", DoubleType(), nullable=True),
        StructField("pc_1typology", DoubleType(), nullable=True),
        StructField("pc_garden", DoubleType(), nullable=True),
        StructField("pc_minrooms", DoubleType(), nullable=True),
        StructField("pc_status", DoubleType(), nullable=True),
        StructField("pc_terrace", DoubleType(), nullable=True),
    ]
)

# sales_price_history: single series ``compravendite_price_sqm_avg`` with
# only ``price_avg``. cadastral_typology is the segmentation key.
SALES_PRICE_HISTORY_SCHEMA = StructType(
    _COMMON_HISTORY_FIELDS
    + [
        StructField("cadastral_typology", StringType(), nullable=True),
        StructField("price_avg", DoubleType(), nullable=True),
    ]
)

# sales_volume_history: single series ``sales_qtraw`` with ``qtraw``.
SALES_VOLUME_HISTORY_SCHEMA = StructType(
    _COMMON_HISTORY_FIELDS
    + [
        StructField("cadastral_typology", StringType(), nullable=True),
        StructField("qtraw", DoubleType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Table registry
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: dict[str, StructType] = {
    PRICE_HISTORY_TABLE: PRICE_HISTORY_SCHEMA,
    ADS_HISTORY_TABLE: ADS_HISTORY_SCHEMA,
    SEARCH_DATA_HISTORY_TABLE: SEARCH_DATA_HISTORY_SCHEMA,
    SALES_PRICE_HISTORY_TABLE: SALES_PRICE_HISTORY_SCHEMA,
    SALES_VOLUME_HISTORY_TABLE: SALES_VOLUME_HISTORY_SCHEMA,
    REGIONS_TABLE: GEO_ZONE_SCHEMA,
    PROVINCES_TABLE: GEO_ZONE_SCHEMA,
    MUNICIPALITIES_TABLE: GEO_ZONE_SCHEMA,
}

# Primary key columns for each table.
# The history tables produce one row per (zone, contract, window, typology,
# series_type, series_key, year, month) — that whole tuple is the key.
_HISTORY_LISTING_PK = [
    "ty_zone",
    "id_zone",
    "contract",
    "window",
    "typology",
    "series_type",
    "series_key",
    "year",
    "month",
]
_HISTORY_SALES_PK = [
    "ty_zone",
    "id_zone",
    "contract",
    "window",
    "cadastral_typology",
    "year",
    "month",
]

TABLE_METADATA: dict[str, dict] = {
    PRICE_HISTORY_TABLE: {
        "primary_keys": _HISTORY_LISTING_PK,
        "cursor_field": HISTORY_CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    ADS_HISTORY_TABLE: {
        "primary_keys": _HISTORY_LISTING_PK,
        "cursor_field": HISTORY_CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    SEARCH_DATA_HISTORY_TABLE: {
        "primary_keys": _HISTORY_LISTING_PK,
        "cursor_field": HISTORY_CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    SALES_PRICE_HISTORY_TABLE: {
        "primary_keys": _HISTORY_SALES_PK,
        "cursor_field": HISTORY_CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    SALES_VOLUME_HISTORY_TABLE: {
        "primary_keys": _HISTORY_SALES_PK,
        "cursor_field": HISTORY_CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    REGIONS_TABLE: {
        "primary_keys": ["id_zone"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    PROVINCES_TABLE: {
        "primary_keys": ["id_zone"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    MUNICIPALITIES_TABLE: {
        "primary_keys": ["id_zone"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

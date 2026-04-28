"""Schemas, metadata, and constants for the immobiliare.it Insights connector.

The Market Explorer service exposes fifteen tables:

* Five CDC history tables backed by zone-fanned-out POST endpoints
  (``price_history``, ``ads_history``, ``search_data_history``,
  ``sales_price_history``, ``sales_volume_history``).
* Five point-in-time (snapshot) twin tables backed by sibling POST endpoints
  (``price``, ``ads``, ``search_data``, ``sales_price``, ``sales_volume``).
  Same fan-out shape as the history tables, but pinned to the latest
  available ``(year, month)`` per run — every run reloads the same period.
* Five geo-taxonomy snapshot tables: ``regions``, ``provinces``,
  ``municipalities``, ``macro_zones``, ``micro_zones`` — all sharing
  ``GEO_ZONE_SCHEMA`` and the same ``GET /api/taxonomies/geo/IT/ty_zone/{level}``
  call shape.

History responses are nested dict-of-arrays (e.g. ``rooms`` keyed by
``"1".."m5"``, ``maintenance_status`` keyed by ``"1".."4"``). The connector
flattens these into one row per ``(zone, contract, year, month, series_type,
series_key)`` tuple — see ``immobiliare_it.py`` for the shaping logic and
``api_doc.md`` for the underlying response shape.

Point-in-time responses wrap each scalar in ``{value, delta, ranking}`` and
add percentile-bucket breakdowns (``price_10pc``..``price_90pc``). We
flatten them onto the same ``(series_type, series_key)`` axis, modelling
top-level scalars as ``series_type="scalar"`` rows and breakdowns as one
row per inner key.
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
MACRO_ZONES_TABLE = "macro_zones"
MICRO_ZONES_TABLE = "micro_zones"
PRICE_TABLE = "price"
ADS_TABLE = "ads"
SEARCH_DATA_TABLE = "search_data"
SALES_PRICE_TABLE = "sales_price"
SALES_VOLUME_TABLE = "sales_volume"

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

# Point-in-time (current-period snapshot) tables. Same fan-out as history
# but each run reloads the latest available month — there is no time cursor.
LISTING_PIT_TABLES = {
    PRICE_TABLE,
    ADS_TABLE,
    SEARCH_DATA_TABLE,
}

SALES_PIT_TABLES = {
    SALES_PRICE_TABLE,
    SALES_VOLUME_TABLE,
}

POINT_IN_TIME_TABLES = LISTING_PIT_TABLES | SALES_PIT_TABLES

# Geo-taxonomy snapshot tables — flat list endpoints sharing GEO_ZONE_SCHEMA.
SNAPSHOT_TABLES = {
    REGIONS_TABLE,
    PROVINCES_TABLE,
    MUNICIPALITIES_TABLE,
    MACRO_ZONES_TABLE,
    MICRO_ZONES_TABLE,
}

SUPPORTED_TABLES: list[str] = [
    PRICE_HISTORY_TABLE,
    ADS_HISTORY_TABLE,
    SEARCH_DATA_HISTORY_TABLE,
    SALES_PRICE_HISTORY_TABLE,
    SALES_VOLUME_HISTORY_TABLE,
    PRICE_TABLE,
    ADS_TABLE,
    SEARCH_DATA_TABLE,
    SALES_PRICE_TABLE,
    SALES_VOLUME_TABLE,
    REGIONS_TABLE,
    PROVINCES_TABLE,
    MUNICIPALITIES_TABLE,
    MACRO_ZONES_TABLE,
    MICRO_ZONES_TABLE,
]

# Per-snapshot-table mapping from table name to the ``ty_zone`` taxonomy code.
# Drives both the snapshot-table read path and the history-table zone fan-out.
SNAPSHOT_TY_ZONE: dict[str, str] = {
    REGIONS_TABLE: "reg",
    PROVINCES_TABLE: "pro",
    MUNICIPALITIES_TABLE: "com",
    MACRO_ZONES_TABLE: "macro",
    MICRO_ZONES_TABLE: "micro",
}

# Per-history-table mapping from table name to the API endpoint path.
HISTORY_ENDPOINTS: dict[str, str] = {
    PRICE_HISTORY_TABLE: "/api/price/history",
    ADS_HISTORY_TABLE: "/api/ads/history",
    SEARCH_DATA_HISTORY_TABLE: "/api/search-data/history",
    SALES_PRICE_HISTORY_TABLE: "/api/sales/price/history",
    SALES_VOLUME_HISTORY_TABLE: "/api/sales/volume/history",
}

# Per-PIT-table mapping from table name to the API endpoint path. These are
# the sibling endpoints of the history tables, returning a single reference
# period rather than a time-series.
PIT_ENDPOINTS: dict[str, str] = {
    PRICE_TABLE: "/api/price",
    ADS_TABLE: "/api/ads",
    SEARCH_DATA_TABLE: "/api/search-data",
    SALES_PRICE_TABLE: "/api/sales/price",
    SALES_VOLUME_TABLE: "/api/sales/volume",
}

# Default ``zone_level`` per fan-out table — listing-side endpoints support
# per-municipality data; sales endpoints are AdE-driven and only emit at the
# province level reliably. PIT tables inherit the same defaults as their
# history twins.
DEFAULT_ZONE_LEVEL: dict[str, str] = {
    PRICE_HISTORY_TABLE: "com",
    ADS_HISTORY_TABLE: "com",
    SEARCH_DATA_HISTORY_TABLE: "com",
    SALES_PRICE_HISTORY_TABLE: "pro",
    SALES_VOLUME_HISTORY_TABLE: "pro",
    PRICE_TABLE: "com",
    ADS_TABLE: "com",
    SEARCH_DATA_TABLE: "com",
    SALES_PRICE_TABLE: "pro",
    SALES_VOLUME_TABLE: "pro",
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
# Point-in-time (PIT) schemas
#
# Each PIT response wraps top-level metrics in ``{value, delta, ranking}``
# objects and adds breakdowns keyed by inner code (maintenance_status,
# rooms, price_typologies for price/ads; price_cadastral_typologies for
# sales_price; cadastral_typologies + sales_surface_class for sales_volume;
# typologies + maintenance_status arrays for search_data).
#
# We flatten on the same ``(series_type, series_key)`` axis as the history
# tables. Top-level scalars become ``series_type="scalar"`` rows with
# ``series_key`` set to the metric name (e.g. ``"price_avg"``, ``"discount"``)
# — this way every metric the API returns lands in its own row, no schema
# churn when the API adds a new top-level metric.
#
# Each schema is the column-wise union of all populated fields across series
# types so callers can query any series_type with the same DataFrame.
# Columns irrelevant to a given series_type are nullable and left unset.
# ---------------------------------------------------------------------------

# Common PIT identification columns. Mirrors _COMMON_HISTORY_FIELDS minus
# the cursor field — PIT runs are snapshot-only (no cursor advancement).
_COMMON_PIT_FIELDS: list[StructField] = [
    StructField("ty_zone", StringType(), nullable=False),
    StructField("id_zone", StringType(), nullable=False),
    StructField("nation", StringType(), nullable=True),
    StructField("contract", IntegerType(), nullable=False),
    StructField("window", StringType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("series_type", StringType(), nullable=False),
    StructField("series_key", StringType(), nullable=False),
]

# Columns populated for top-level scalar metrics (series_type == "scalar").
# Every PIT endpoint shares the {value, delta:{value,window}, ranking:{of,
# position}} envelope, so these columns appear in every PIT schema.
_PIT_SCALAR_FIELDS: list[StructField] = [
    StructField("value", DoubleType(), nullable=True),
    StructField("delta_value", DoubleType(), nullable=True),
    StructField("delta_window", StringType(), nullable=True),
    StructField("ranking_of", IntegerType(), nullable=True),
    StructField("ranking_position", IntegerType(), nullable=True),
]

# Percentile-bucket columns used by maintenance_status / rooms /
# price_typologies / price_cadastral_typologies. The api_doc explicitly
# names ``price_10pc, price_20pc, price_50pc, price_80pc, price_90pc``
# (NOT 25/75) — see the Object Schema section.
_PIT_PERCENTILE_FIELDS: list[StructField] = [
    StructField("price_10pc", DoubleType(), nullable=True),
    StructField("price_20pc", DoubleType(), nullable=True),
    StructField("price_50pc", DoubleType(), nullable=True),
    StructField("price_80pc", DoubleType(), nullable=True),
    StructField("price_90pc", DoubleType(), nullable=True),
]

# price / ads PIT — same layout per api_doc (ads is described as point-in-time
# listings stock with prices, not raw ad counts).
PRICE_PIT_SCHEMA = StructType(
    _COMMON_PIT_FIELDS
    + [
        StructField("typology", IntegerType(), nullable=True),
    ]
    + _PIT_SCALAR_FIELDS
    + _PIT_PERCENTILE_FIELDS
)
ADS_PIT_SCHEMA = PRICE_PIT_SCHEMA  # identical layout

# search_data PIT — heterogeneous: scalar rows + maintenance_status array
# (status_id + qt_raw_perc) + typologies array (typology_id + qt_raw_perc) +
# pc_rooms map (pc_raw + qt_raw per room key) + res struct (one row with
# all pc_* attributes).
SEARCH_DATA_PIT_SCHEMA = StructType(
    _COMMON_PIT_FIELDS
    + [
        StructField("typology", IntegerType(), nullable=True),
    ]
    + _PIT_SCALAR_FIELDS
    + [
        # maintenance_status / typologies arrays in search_data PIT.
        StructField("qt_raw_perc", DoubleType(), nullable=True),
        # pc_rooms map: pc_raw / qt_raw (no percentile prices here, just
        # search-distribution counts).
        StructField("pc_raw", DoubleType(), nullable=True),
        StructField("qt_raw", DoubleType(), nullable=True),
        # res struct: search-attribute filter prevalence percentages.
        # series_type="res" emits one row with these fields populated.
        StructField("pc_1floor", DoubleType(), nullable=True),
        StructField("pc_1typology", DoubleType(), nullable=True),
        StructField("pc_garage", DoubleType(), nullable=True),
        StructField("pc_garden", DoubleType(), nullable=True),
        StructField("pc_minrooms", DoubleType(), nullable=True),
        StructField("pc_status", DoubleType(), nullable=True),
        StructField("pc_terrace", DoubleType(), nullable=True),
    ]
)

# sales_price PIT — scalar rows for the four compravendite_* metrics +
# percentile rows for price_cadastral_typologies array (one row per A-class).
SALES_PRICE_PIT_SCHEMA = StructType(
    _COMMON_PIT_FIELDS
    + [
        StructField("cadastral_typology", StringType(), nullable=True),
    ]
    + _PIT_SCALAR_FIELDS
    + _PIT_PERCENTILE_FIELDS
)

# sales_volume PIT — scalar rows (sales, sales_qtraw, sales_surface_avg) +
# distribution rows (cadastral_typologies, sales_surface_class) carrying
# qt_raw_perc.
SALES_VOLUME_PIT_SCHEMA = StructType(
    _COMMON_PIT_FIELDS
    + [
        StructField("cadastral_typology", StringType(), nullable=True),
    ]
    + _PIT_SCALAR_FIELDS
    + [
        StructField("qt_raw_perc", DoubleType(), nullable=True),
        # surface class id from sales_surface_class[].id — separate column
        # from series_key so the user can join against a class_surface
        # taxonomy without parsing series_key.
        StructField("class_surface", StringType(), nullable=True),
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
    PRICE_TABLE: PRICE_PIT_SCHEMA,
    ADS_TABLE: ADS_PIT_SCHEMA,
    SEARCH_DATA_TABLE: SEARCH_DATA_PIT_SCHEMA,
    SALES_PRICE_TABLE: SALES_PRICE_PIT_SCHEMA,
    SALES_VOLUME_TABLE: SALES_VOLUME_PIT_SCHEMA,
    REGIONS_TABLE: GEO_ZONE_SCHEMA,
    PROVINCES_TABLE: GEO_ZONE_SCHEMA,
    MUNICIPALITIES_TABLE: GEO_ZONE_SCHEMA,
    MACRO_ZONES_TABLE: GEO_ZONE_SCHEMA,
    MICRO_ZONES_TABLE: GEO_ZONE_SCHEMA,
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

# PIT primary keys mirror the history PKs (same fan-out, same
# series_type/series_key axis) — every PIT row is uniquely identified by
# the request fan-out tuple plus the inner series identifier.
_PIT_LISTING_PK = [
    "ty_zone",
    "id_zone",
    "contract",
    "window",
    "year",
    "month",
    "typology",
    "series_type",
    "series_key",
]
_PIT_SALES_PK = [
    "ty_zone",
    "id_zone",
    "contract",
    "window",
    "year",
    "month",
    "cadastral_typology",
    "series_type",
    "series_key",
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
    PRICE_TABLE: {
        "primary_keys": _PIT_LISTING_PK,
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    ADS_TABLE: {
        "primary_keys": _PIT_LISTING_PK,
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    SEARCH_DATA_TABLE: {
        "primary_keys": _PIT_LISTING_PK,
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    SALES_PRICE_TABLE: {
        "primary_keys": _PIT_SALES_PK,
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    SALES_VOLUME_TABLE: {
        "primary_keys": _PIT_SALES_PK,
        "cursor_field": None,
        "ingestion_type": "snapshot",
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
    MACRO_ZONES_TABLE: {
        "primary_keys": ["id_zone"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    MICRO_ZONES_TABLE: {
        "primary_keys": ["id_zone"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

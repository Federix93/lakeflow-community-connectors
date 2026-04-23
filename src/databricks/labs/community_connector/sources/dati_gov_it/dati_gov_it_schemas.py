"""Schemas, metadata, and constants for the dati.gov.it (CKAN) connector.

All five tables (packages, resources, organizations, tags, groups) are static
— the object list is defined by the connector rather than dynamically
discovered — so every schema is hard-coded here against the fields documented
in ``dati_gov_it_api_doc.md``.

Notes on type choices:

* UUIDs and slugs stay as ``StringType``.
* Timestamps are emitted as ``TimestampType``. dati.gov.it returns ISO 8601
  strings without a timezone suffix; we treat them as UTC. Spark will parse
  them as long as we hand back the original string — the framework converts
  according to this schema.
* ``resource.modified`` is *kept as a string* because the portal sometimes
  emits ``"DD-MM-YYYY"`` instead of ISO 8601 (see quirk #4 in the API doc).
* ``extras`` is preserved as ``array<struct<key:string, value:string>>`` —
  never flattened, since the set of keys is open-ended (DCAT-AP_IT profile)
  and values often contain JSON-encoded payloads we do not want to parse.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://www.dati.gov.it/opendata/api/3/action"

# CKAN's hard row cap per package_search call is 1000. We use that for
# throughput — the portal doc suggests 100 as a "conservative" guideline but
# there is no documented rate limit and CKAN accepts 1000.
PAGE_SIZE = 1000

# User-Agent identifies the connector to the upstream portal (dati.gov.it has
# no documented rate limit but it is polite to identify ourselves).
USER_AGENT = "lakeflow-community-connectors/0.1 (dati_gov_it)"

# HTTP retry policy for transient 5xx / 429 responses.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Default size of a time window used for partitioned incremental reads of
# packages/resources. 1 hour is a reasonable default — the portal's largest
# days contain a few thousand modified packages, well within one page.
DEFAULT_WINDOW_SECONDS = 24 * 60 * 60  # 24 hours
MIN_WINDOW_SECONDS = 60

# Hard floor for the incremental cursor when the connector has no prior
# watermark and the user did not provide ``start_timestamp`` either.
# dati.gov.it's oldest dataset is from 2014, so this is safe.
DEFAULT_INCREMENTAL_FLOOR = "2014-01-01T00:00:00"

# Supported tables (static — not dynamically discoverable).
PACKAGES_TABLE = "packages"
RESOURCES_TABLE = "resources"
ORGANIZATIONS_TABLE = "organizations"
TAGS_TABLE = "tags"
GROUPS_TABLE = "groups"

SUPPORTED_TABLES: list[str] = [
    PACKAGES_TABLE,
    RESOURCES_TABLE,
    ORGANIZATIONS_TABLE,
    TAGS_TABLE,
    GROUPS_TABLE,
]

# Tables driven off package_search. Only these support incremental /
# partitioned reads.
INCREMENTAL_TABLES = {PACKAGES_TABLE, RESOURCES_TABLE}

# Table-level connector filters that the user may set in ``external_options``.
# These are only applied to packages / resources (which derives from packages).
# Anything else is ignored.
PACKAGE_FILTER_KEYS = ("organization", "tags", "groups", "res_format", "q")

# ---------------------------------------------------------------------------
# Nested struct schemas (shared between packages and resources)
# ---------------------------------------------------------------------------

# The ``extras`` payload is a free-form array of {key, value} pairs from the
# DCAT-AP_IT profile. We intentionally keep values as raw strings because
# upstream encodes structured content (lists, objects) as JSON within those
# strings — downstream can parse on demand.
EXTRAS_FIELD = StructField(
    "extras",
    ArrayType(
        StructType(
            [
                StructField("key", StringType(), nullable=True),
                StructField("value", StringType(), nullable=True),
            ]
        )
    ),
    nullable=True,
)

ORGANIZATION_NESTED_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("image_url", StringType(), nullable=True),
        StructField("created", TimestampType(), nullable=True),
        StructField("is_organization", BooleanType(), nullable=True),
        StructField("approval_status", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
    ]
)

TAG_NESTED_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("display_name", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("vocabulary_id", StringType(), nullable=True),
    ]
)

GROUP_NESTED_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("display_name", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("image_display_url", StringType(), nullable=True),
    ]
)

# Fields that live inside a single resource record. These are used both as
# elements of ``packages.resources`` and — with ``package_id`` / parent
# context added — as the row schema of the flat ``resources`` table.
RESOURCE_FIELDS: list[StructField] = [
    StructField("id", StringType(), nullable=True),
    StructField("package_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("format", StringType(), nullable=True),
    StructField("distribution_format", StringType(), nullable=True),
    StructField("mimetype", StringType(), nullable=True),
    StructField("mimetype_inner", StringType(), nullable=True),
    StructField("size", IntegerType(), nullable=True),
    StructField("hash", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("position", IntegerType(), nullable=True),
    StructField("created", TimestampType(), nullable=True),
    StructField("last_modified", TimestampType(), nullable=True),
    StructField("metadata_modified", TimestampType(), nullable=True),
    StructField("url_type", StringType(), nullable=True),
    StructField("resource_type", StringType(), nullable=True),
    StructField("datastore_active", BooleanType(), nullable=True),
    StructField("cache_url", StringType(), nullable=True),
    StructField("cache_last_updated", StringType(), nullable=True),
    StructField("webstore_url", StringType(), nullable=True),
    StructField("webstore_last_updated", StringType(), nullable=True),
    StructField("license", StringType(), nullable=True),
    StructField("license_id", StringType(), nullable=True),
    StructField("license_type", StringType(), nullable=True),
    StructField("rights", StringType(), nullable=True),
    # Kept as string — the portal sometimes emits "DD-MM-YYYY" here.
    StructField("modified", StringType(), nullable=True),
    StructField("access_url", StringType(), nullable=True),
    StructField("uri", StringType(), nullable=True),
]

RESOURCE_NESTED_SCHEMA = StructType(RESOURCE_FIELDS)

# ---------------------------------------------------------------------------
# Top-level table schemas
# ---------------------------------------------------------------------------

PACKAGES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("notes", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("private", BooleanType(), nullable=True),
        StructField("isopen", BooleanType(), nullable=True),
        StructField("license_id", StringType(), nullable=True),
        StructField("license_title", StringType(), nullable=True),
        StructField("license_url", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("version", StringType(), nullable=True),
        StructField("author", StringType(), nullable=True),
        StructField("author_email", StringType(), nullable=True),
        StructField("maintainer", StringType(), nullable=True),
        StructField("maintainer_email", StringType(), nullable=True),
        StructField("creator_user_id", StringType(), nullable=True),
        StructField("owner_org", StringType(), nullable=True),
        StructField("metadata_created", TimestampType(), nullable=True),
        StructField("metadata_modified", TimestampType(), nullable=True),
        StructField("num_resources", IntegerType(), nullable=True),
        StructField("num_tags", IntegerType(), nullable=True),
        StructField("holder_name", StringType(), nullable=True),
        StructField("holder_identifier", StringType(), nullable=True),
        StructField("dataset_is_local", BooleanType(), nullable=True),
        StructField("organization", ORGANIZATION_NESTED_SCHEMA, nullable=True),
        StructField("resources", ArrayType(RESOURCE_NESTED_SCHEMA), nullable=True),
        StructField("tags", ArrayType(TAG_NESTED_SCHEMA), nullable=True),
        StructField("groups", ArrayType(GROUP_NESTED_SCHEMA), nullable=True),
        EXTRAS_FIELD,
    ]
)

# Flat resources table. Each row is one element of a package's resources
# array, with the parent ``package_metadata_modified`` watermark surfaced
# as a top-level column so downstream can filter incrementally.
RESOURCES_SCHEMA = StructType(
    RESOURCE_FIELDS
    + [
        StructField("package_metadata_modified", TimestampType(), nullable=True),
    ]
)

ORGANIZATIONS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("display_name", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("image_url", StringType(), nullable=True),
        StructField("image_display_url", StringType(), nullable=True),
        StructField("created", TimestampType(), nullable=True),
        StructField("is_organization", BooleanType(), nullable=True),
        StructField("approval_status", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("num_followers", IntegerType(), nullable=True),
        StructField("package_count", IntegerType(), nullable=True),
        StructField("identifier", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("site", StringType(), nullable=True),
        StructField("telephone", StringType(), nullable=True),
        EXTRAS_FIELD,
        StructField(
            "users",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), nullable=True),
                        StructField("name", StringType(), nullable=True),
                        StructField("fullname", StringType(), nullable=True),
                        StructField("capacity", StringType(), nullable=True),
                        StructField("state", StringType(), nullable=True),
                        StructField("created", TimestampType(), nullable=True),
                        StructField("sysadmin", BooleanType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)

TAGS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("display_name", StringType(), nullable=True),
        StructField("vocabulary_id", StringType(), nullable=True),
    ]
)

GROUPS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("display_name", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("image_url", StringType(), nullable=True),
        StructField("image_display_url", StringType(), nullable=True),
        StructField("created", TimestampType(), nullable=True),
        StructField("is_organization", BooleanType(), nullable=True),
        StructField("approval_status", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("num_followers", IntegerType(), nullable=True),
        StructField("package_count", IntegerType(), nullable=True),
        EXTRAS_FIELD,
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    PACKAGES_TABLE: PACKAGES_SCHEMA,
    RESOURCES_TABLE: RESOURCES_SCHEMA,
    ORGANIZATIONS_TABLE: ORGANIZATIONS_SCHEMA,
    TAGS_TABLE: TAGS_SCHEMA,
    GROUPS_TABLE: GROUPS_SCHEMA,
}

# Metadata describing primary key / cursor / ingestion semantics.
# packages + resources are ``cdc`` (soft-deletes via state=deleted; no
# separate delete endpoint, so we do not claim cdc_with_deletes).
TABLE_METADATA: dict[str, dict] = {
    PACKAGES_TABLE: {
        "primary_keys": ["id"],
        "cursor_field": "metadata_modified",
        "ingestion_type": "cdc",
    },
    RESOURCES_TABLE: {
        "primary_keys": ["id"],
        # Watermark surfaced from the parent package. A resource changes
        # (or gets soft-deleted) iff its parent package re-saves, so the
        # package-level watermark is the right cursor for incremental.
        "cursor_field": "package_metadata_modified",
        "ingestion_type": "cdc",
    },
    ORGANIZATIONS_TABLE: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TAGS_TABLE: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    GROUPS_TABLE: {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

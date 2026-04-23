# Lakeflow dati.gov.it Community Connector

This documentation describes how to configure and use the **dati.gov.it** Lakeflow community connector to ingest data from the Italian national open data portal into Databricks.

[dati.gov.it](https://www.dati.gov.it/) is Italy's national open data portal, operated on a customised CKAN instance with the DCAT-AP_IT extension. The portal catalogs ~70,000 datasets published by Italian Public Administration (PA) bodies and exposes them through the standard CKAN Action API v3.

## Prerequisites

- **Network access**: The environment running the connector must be able to reach `https://www.dati.gov.it`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.
- **No account required**: The portal is public and read-only access does not require an API key. An `api_key` field is exposed for compatibility with restricted CKAN instances or future write operations — leave it empty for dati.gov.it.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name       | Type   | Required | Description                                                                                 | Example                                              |
|------------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------------------------|
| `base_url` | string | no       | Base URL for the CKAN Action API. Defaults to `https://www.dati.gov.it/opendata/api/3/action`. Override to connect to another CKAN-based portal that shares this connector's logic. | `https://dati.regione.example.it/api/3/action`      |
| `api_key`  | string | no (secret) | Optional CKAN API token, forwarded as the `Authorization` header. Not required for dati.gov.it. | `<ckan-api-token>`                                  |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that the connection allows. This connector requires table-specific options for `packages` and `resources`, so this parameter must be set. | See the full list below. |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`organization,tags,groups,res_format,q,include_deleted,window_seconds,max_records_per_batch,start_timestamp`

> **Note**: Table-specific options such as `organization` or `res_format` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline spec. These option names must be included in `externalOptionsAllowList` for the connection to pass them through.

### Obtaining the Required Parameters

- **`api_key`**: Not required for dati.gov.it. Leave the field empty. If you are pointing the connector at a private CKAN instance, generate a token from **User Profile → Manage → API tokens** on that portal.
- **`base_url`**: Use the default for dati.gov.it. Only override it when targeting another CKAN portal.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select an existing Lakeflow Community Connector connection for this source, or create a new one.
3. Set `externalOptionsAllowList` to `organization,tags,groups,res_format,q,include_deleted,window_seconds,max_records_per_batch,start_timestamp`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of five tables derived from the CKAN catalog:

| Table           | Description                                                                                           | Primary Key | Ingestion Type | Cursor Field                  | Partitioned |
|-----------------|-------------------------------------------------------------------------------------------------------|-------------|----------------|-------------------------------|-------------|
| `packages`      | Datasets (the main fact table). Each record is a dataset published by a PA body, with nested organization, resources, tags, groups, and DCAT-AP_IT `extras`. | `id`        | `cdc`          | `metadata_modified`           | Yes         |
| `resources`     | Files and distributions (CSV, JSON, XML, etc.) belonging to packages. Derived from `package_search` results to avoid O(N) extra calls.                       | `id`        | `cdc`          | `package_metadata_modified`   | Yes         |
| `organizations` | Publisher bodies (Italian PA entities). Full snapshot on every run.                                   | `id`        | `snapshot`     | —                             | No          |
| `tags`          | Free-form keyword labels applied to packages. Full snapshot on every run.                             | `id`        | `snapshot`     | —                             | No          |
| `groups`        | Thematic categories (e.g. `governo`, `ambiente`, `salute`). Full snapshot on every run.               | `id`        | `snapshot`     | —                             | No          |

Notes:

- `packages` and `resources` support **partitioned, incremental reads** on `metadata_modified`. The connector splits the cursor range into time windows that Spark can fetch in parallel across executors.
- `organizations`, `tags`, and `groups` are small reference sets read in a single driver-side call each run.
- CKAN uses **soft deletes** only: deleted records have `state = "deleted"` rather than being hard-deleted. The ingestion type is `cdc` (not `cdc_with_deletes`) because there is no separate delete endpoint — pass `include_deleted=true` to surface tombstoned rows.

For the full field-by-field schemas, see [`dati_gov_it_api_doc.md`](./dati_gov_it_api_doc.md).

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of `packages`, `resources`, `organizations`, `tags`, `groups`). |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Applies to `cdc` and `snapshot` tables. |
| `primary_keys` | No | List of columns to override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |

### Source-specific `table_configuration` options

These options are set inside `table_configuration`. They only take effect for `packages` and `resources` (both are backed by `package_search`); the snapshot tables ignore them.

| Option | Applicable Objects | Description | Default |
|---|---|---|---|
| `organization` | `packages`, `resources` | Filter by organization slug (e.g. `aci`). Single value. | — |
| `tags` | `packages`, `resources` | Filter by tag name. Comma-separated values are ANDed together ("datasets tagged with BOTH A and B"). | — |
| `groups` | `packages`, `resources` | Filter by group slug (e.g. `governo`). Comma-separated values are ANDed. | — |
| `res_format` | `packages`, `resources` | Filter by resource format (e.g. `CSV`, `JSON`, `XML`). Matches the package's `res_format` facet. | — |
| `q` | `packages`, `resources` | Free-text Solr query passed to `package_search`. | `*:*` |
| `include_deleted` | `packages`, `resources` | Set to `true` to surface soft-deleted records (`state = "deleted"`). | `false` |
| `start_timestamp` | `packages`, `resources` | ISO 8601 lower bound used on the first run, when no checkpoint exists yet. Format: `YYYY-MM-DDTHH:MM:SS` (no timezone — treated as UTC). | `2014-01-01T00:00:00` |
| `window_seconds` | `packages`, `resources` | Size in seconds of each incremental partition window on `metadata_modified`. Larger values mean fewer, larger partitions. Minimum `60`. | `86400` (1 day) |
| `max_records_per_batch` | `packages`, `resources` | Cap on records returned per sequential `read_table` call. Does not apply to partitioned reads. | — (no cap) |

## Usage Example

Example `pipeline_spec` showing an incremental ingestion of ACI (Automobile Club Italia) datasets, plus a snapshot of all organizations:

```json
{
  "pipeline_spec": {
    "connection_name": "dati_gov_it_connection",
    "object": [
      {
        "table": {
          "source_table": "packages",
          "table_configuration": {
            "organization": "aci",
            "start_timestamp": "2024-01-01T00:00:00",
            "window_seconds": "86400",
            "max_records_per_batch": "500"
          }
        }
      },
      {
        "table": {
          "source_table": "resources",
          "table_configuration": {
            "organization": "aci",
            "res_format": "CSV",
            "start_timestamp": "2024-01-01T00:00:00",
            "window_seconds": "86400"
          }
        }
      },
      {
        "table": {
          "source_table": "organizations"
        }
      }
    ]
  }
}
```

- `connection_name` must point to a UC connection that uses this connector (with `externalOptionsAllowList` set as described above).
- For `packages` and `resources`, filters (`organization`, `tags`, `groups`, `res_format`, `q`) are ANDed together.
- On subsequent runs the connector uses the stored cursor (`metadata_modified`) and the `window_seconds` parameter to advance incrementally — `start_timestamp` is only consulted when there is no stored offset yet.

## Data Type Mapping

| CKAN JSON Type | Example Fields | Connector / Spark Type |
|---|---|---|
| string (UUID) | `id`, `owner_org`, `package_id` | `StringType` |
| string (text) | `name`, `title`, `notes`, `format` | `StringType` |
| string (ISO 8601 datetime, no tz) | `metadata_created`, `metadata_modified`, `created`, `last_modified` | `TimestampType` (treated as UTC) |
| boolean | `isopen`, `private`, `is_organization`, `datastore_active` | `BooleanType` |
| integer | `num_resources`, `num_tags`, `size`, `package_count` | `IntegerType` |
| object | `organization` nested inside a package | `StructType` |
| array of objects | `resources`, `tags`, `groups`, `users` | `ArrayType(StructType)` |
| `extras` (DCAT-AP_IT key/value pairs) | `extras` | `ArrayType(StructType[key:string, value:string])` |

## Known Limitations and Gotchas

- **Public, read-only portal**. dati.gov.it does not require credentials for read operations — leave `api_key` empty. No published rate limit exists; the connector retries `429` / `5xx` responses with exponential backoff and identifies itself via a User-Agent header.
- **`metadata_modified` has no timezone suffix**. The portal returns ISO 8601 strings like `"2024-03-08T18:45:38.931037"` without `Z`. The connector treats them as UTC. When providing `start_timestamp`, use the same format (no timezone).
- **`resource.modified` is stored as a string**, not a timestamp. The portal sometimes emits `"DD-MM-YYYY"` here instead of ISO 8601, so the connector preserves the raw value to avoid parse failures. Cast downstream if you need a real timestamp.
- **`extras` is kept as `array<struct<key, value>>`**. DCAT-AP_IT values like `theme`, `contact_point`, and `publisher` are themselves JSON-encoded strings inside the `value` field. The connector does not parse them — unpack with `from_json` downstream if you need the structured form.
- **`tag_list` is not paginated on this instance**. The dati.gov.it portal ignores `limit`/`offset` on `tag_list` and returns the full tag set (~16,000 entries) in a single response. The `tags` snapshot therefore issues one call per run.
- **`organization_list?all_fields=true` returns HTTP 500** on dati.gov.it. The connector works around this by calling `organization_list` with slug-only output and then issuing `organization_show` per organization.
- **Soft deletes only**. Pass `include_deleted=true` on `packages`/`resources` if you need to see tombstoned records (`state = "deleted"`); otherwise they are filtered out server-side.
- **Harvested vs. local datasets**. Most packages are harvested from regional PA portals (`dataset_is_local: false`). `metadata_modified` reflects when dati.gov.it last re-harvested the record, not when the source portal updated it.

## Troubleshooting

- **Empty result sets**: Double-check filter values against the portal UI. Organization and group values use URL slugs (e.g., `aci`, `governo`), not display names.
- **Pipeline exceeds expected duration on first run**: A full `packages` backfill touches ~70k rows. Lower `window_seconds` (e.g., to `3600`) to get finer-grained partitions and more Spark parallelism, or scope the first run with `start_timestamp` / `organization` filters.
- **HTTP 500 from `/organization_list`**: Already handled; the connector only uses the slug-only variant. If you see this on a custom `base_url`, confirm the upstream CKAN version.
- **Schema mismatches on `extras`**: `extras` values are always strings. If your downstream expects structured types, parse selectively with `from_json` — do not redefine the connector schema.

## References

- Connector implementation: [`dati_gov_it.py`](./dati_gov_it.py)
- Connector schemas and constants: [`dati_gov_it_schemas.py`](./dati_gov_it_schemas.py)
- Detailed API documentation and quirks: [`dati_gov_it_api_doc.md`](./dati_gov_it_api_doc.md)
- Official dati.gov.it API page: https://www.dati.gov.it/api
- CKAN Action API reference: https://docs.ckan.org/en/latest/api/
- DCAT-AP_IT CKAN extension: https://extensions.ckan.org/extension/dcatapit/

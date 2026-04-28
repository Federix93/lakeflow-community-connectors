# Lakeflow immobiliare.it Insights — Market Explorer Community Connector

This documentation describes how to configure and use the **immobiliare.it Insights — Market Explorer** Lakeflow community connector to ingest Italian real estate market intelligence into Databricks.

[immobiliare.it Insights](https://insights.immobiliare.it/) is the data and analytics arm of Immobiliare.it, Italy's largest property listings portal. The **Market Explorer** service (`ws-osservatorio-public-api`) exposes historical time-series of listing prices, ad stock, search behaviour, and AdE-derived sales (price and volume) for Italian zones, plus the underlying geo taxonomy (regions, provinces, municipalities). The service is **paid and authenticated**, with separate base URLs for production (`https://ws-osservatorio.realitycs.it`) and sandbox (`https://ws-osservatorio-dev.realitycs.it`).

## Prerequisites

- **Network access**: The environment running the connector must be able to reach `https://ws-osservatorio.realitycs.it` (production) and/or `https://ws-osservatorio-dev.realitycs.it` (sandbox).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.
- **Paid API credentials issued by Immobiliare.it Insights**: Market Explorer is a commercial product. You must have an active subscription and a set of OAuth2 credentials (`client_id`, `client_secret`, `username`, `password`) provisioned by Immobiliare.it. The sandbox uses a separate account from production — request both if you need to develop against the sandbox before promoting to production credentials.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name            | Type   | Required     | Description                                                                                                                                                                                                                                       | Example                                          |
|-----------------|--------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|
| `base_url`      | string | no           | Base URL for the Market Explorer service. Use the sandbox while developing (`https://ws-osservatorio-dev.realitycs.it`) and switch to production (`https://ws-osservatorio.realitycs.it`) once your account is provisioned. Defaults to sandbox.  | `https://ws-osservatorio.realitycs.it`           |
| `client_id`     | string | yes          | OAuth2 client identifier issued by Immobiliare.it. Used as the username half of the HTTP Basic header on the `/oauth/token` request.                                                                                                              | `acme_client_id`                                 |
| `client_secret` | string | yes (secret) | OAuth2 client secret issued by Immobiliare.it. Used as the password half of the HTTP Basic header on the `/oauth/token` request.                                                                                                                  | `<oauth-client-secret>`                          |
| `username`      | string | yes          | End-user account username, sent in the form-encoded body of the `/oauth/token` request alongside `grant_type=password`.                                                                                                                           | `acme_user`                                      |
| `password`      | string | yes (secret) | End-user account password, sent in the form-encoded body of the `/oauth/token` request alongside `grant_type=password`.                                                                                                                           | `<account-password>`                             |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that the connection allows. The history tables require fan-out parameters, so this must be set. | See the full list below. |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`start_year_month,window,contract,typology,cadastral_typology,nation,zone_level,zone_filter,max_records_per_batch`

> **Note**: Table-specific options such as `zone_level` or `cadastral_typology` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline spec. These option names must be included in `externalOptionsAllowList` for the connection to pass them through.

### Obtaining the Required Parameters

- **`client_id` / `client_secret` / `username` / `password`**: Request a Market Explorer subscription from Immobiliare.it Insights via their sales contact at <https://insights.immobiliare.it/>. Once provisioned, you receive a credential pack containing the OAuth2 client (`client_id`, `client_secret`) plus an end-user account (`username`, `password`). Sandbox and production accounts are separate; ask for both if you need a development environment.
- **`base_url`**: Defaults to the sandbox URL (`https://ws-osservatorio-dev.realitycs.it`) so first-run development against trial credentials does not accidentally consume production quota. Switch to `https://ws-osservatorio.realitycs.it` once your production credentials are provisioned. The sandbox returns the same response shapes as production but with sparser data.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select an existing Lakeflow Community Connector connection for this source, or create a new one.
3. Set `externalOptionsAllowList` to `start_year_month,window,contract,typology,cadastral_typology,nation,zone_level,zone_filter,max_records_per_batch`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of eight tables — five CDC history tables backed by zone-fanned-out POST endpoints, and three snapshot taxonomy tables that list zones at a given level:

| Table                  | Description                                                                                                  | Primary Key                                                                                              | Ingestion Type | Cursor Field   | Partitioned |
|------------------------|--------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|----------------|----------------|-------------|
| `price_history`        | Historical time-series of listing prices, segmented by maintenance status, price/sqm, and room count.        | `(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)`                  | `cdc`          | `year_month`   | Yes         |
| `ads_history`          | Historical time-series of listing stock (ad counts), segmented by maintenance status and rooms.              | `(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)`                  | `cdc`          | `year_month`   | Yes         |
| `search_data_history`  | Historical time-series of demand / search behaviour (conversion rate, room mix, search filter rates).       | `(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)`                  | `cdc`          | `year_month`   | Yes         |
| `sales_price_history`  | Historical time-series of actual sale prices (from Italian tax authority AdE data), keyed by cadastral class.| `(ty_zone, id_zone, contract, window, cadastral_typology, year, month)`                                  | `cdc`          | `year_month`   | Yes         |
| `sales_volume_history` | Historical time-series of transaction volume (NTN from AdE), keyed by cadastral class.                       | `(ty_zone, id_zone, contract, window, cadastral_typology, year, month)`                                  | `cdc`          | `year_month`   | Yes         |
| `regions`              | Italian regions taxonomy (~20 rows). Full snapshot on every run.                                             | `id_zone`                                                                                                | `snapshot`     | —              | No          |
| `provinces`            | Italian provinces taxonomy (~110 rows). Full snapshot on every run.                                          | `id_zone`                                                                                                | `snapshot`     | —              | No          |
| `municipalities`       | Italian municipalities (`comuni`) taxonomy (~7,900 rows). Full snapshot on every run.                        | `id_zone`                                                                                                | `snapshot`     | —              | No          |

Notes:

- `*_history` tables support **partitioned reads** with one partition per `(zone × contract × typology × month)` cell. This maps 1:1 to Spark task granularity, so a multi-month backfill at municipality level produces a large number of small partitions.
- `regions`, `provinces`, and `municipalities` are read with a single driver-side `GET /api/taxonomies/geo/IT/ty_zone/{level}` call each — they do not benefit from partitioning.
- The cursor `year_month` is a synthetic sortable integer (`year * 100 + month`, e.g. `202412`). The driver advances it by one month per micro-batch by reading the latest available period from `/api/taxonomies/temporal`.
- `series_type` and `series_key` flatten the API's nested dict-of-arrays response (e.g. `maintenance_status` keyed by `"1".."4"`, `rooms` keyed by `"1".."5"` plus `"m5"`). For aggregate series the `series_key` is `"_"`.

For the full field-by-field schemas, see [`immobiliare_it_schemas.py`](./immobiliare_it_schemas.py).

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option                | Required | Description                                                                                                |
|-----------------------|----------|------------------------------------------------------------------------------------------------------------|
| `source_table`        | Yes      | Table name in the source system (one of the eight listed in **Supported Objects**).                        |
| `destination_catalog` | No       | Target catalog (defaults to pipeline's default).                                                           |
| `destination_schema`  | No       | Target schema (defaults to pipeline's default).                                                            |
| `destination_table`   | No       | Target table name (defaults to `source_table`).                                                            |

### Common `table_configuration` options

| Option         | Required | Description                                                                                       |
|----------------|----------|---------------------------------------------------------------------------------------------------|
| `scd_type`     | No       | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Applies to `cdc` and `snapshot` tables.                   |
| `primary_keys` | No       | List of columns to override the connector's default primary keys.                                 |
| `sequence_by`  | No       | Column used to order records for SCD Type 2 change tracking.                                      |

### Source-specific `table_configuration` options

These options are set inside `table_configuration`. They only take effect for the five `*_history` tables; the three snapshot tables ignore them.

| Option                  | Applicable Objects                                                       | Description                                                                                                                                                                                                                                                                                                              | Default                                                                                       |
|-------------------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `start_year_month`      | All `*_history`                                                          | Backfill anchor in `YYYYMM` form (e.g. `202001` or `2020-01`). The first run starts from this month; subsequent runs use the stored cursor.                                                                                                                                                                              | `202001`                                                                                      |
| `window`                | All `*_history`                                                          | Time window of each API record. Allowed values: `1M`, `3M`, `6M`, `12M`.                                                                                                                                                                                                                                                 | `1M`                                                                                          |
| `contract`              | All `*_history`                                                          | Comma-separated contract codes: `1` = sale, `2` = rental. Sales endpoints reject rental in practice — keep `1` for `sales_*_history`.                                                                                                                                                                                    | Listing tables: `1,2`. Sales tables: `1`.                                                     |
| `typology`              | `price_history`, `ads_history`, `search_data_history` (listing-side only)| Comma-separated **integer** property typology codes (e.g. `4` = Appartamento, `12` = Villa). See the [API doc](./immobiliare_it_api_doc.md) for the full taxonomy.                                                                                                                                                        | `4,5,7,10,11,12,13,31` (residential set)                                                      |
| `cadastral_typology`    | `sales_price_history`, `sales_volume_history` (sales-side only)          | Comma-separated **string** cadastral typology codes (`A1`–`A11`). Distinct from `typology` — sales endpoints accept these instead of the integer codes.                                                                                                                                                                  | `A1,A2,A3,A4,A5,A6,A7,A8,A9,A11`                                                              |
| `nation`                | All `*_history`                                                          | Two-letter nation code. Italy is the only documented value.                                                                                                                                                                                                                                                              | `IT`                                                                                          |
| `zone_level`            | All `*_history`                                                          | Zone taxonomy level used for fan-out. One of `reg` (region), `pro` (province), `com` (municipality), `macro` (macro-zone), `micro` (micro-zone). Lower levels mean dramatically more partitions.                                                                                                                         | Listing tables: `com`. Sales tables: `pro`.                                                   |
| `zone_filter`           | All `*_history`                                                          | Comma-separated explicit list of `id_zone` values. When set, the connector skips the geo-list fetch and fans out only to these zones. Use this to scope a backfill (e.g. one province, one region).                                                                                                                      | — (enumerate every zone at `zone_level`)                                                      |
| `max_records_per_batch` | All `*_history` (sequential path only)                                   | Cap on records returned per `read_table` call. Only applies to the single-driver fallback path; partitioned reads ignore it.                                                                                                                                                                                             | — (no cap)                                                                                    |

> **`typology` vs `cadastral_typology`**: The listing-side endpoints (`price_history`, `ads_history`, `search_data_history`) accept integer `typology` codes. The sales-side endpoints (`sales_price_history`, `sales_volume_history`) accept string `cadastral_typology` codes (`A1`–`A11`) and **do not** accept `typology`. Setting the wrong field for a given table has no effect — the connector picks the right one based on the table name.

## Usage Example

Example `pipeline_spec` ingesting price history scoped to a single zone, contract, and typology, plus a snapshot of all regions:

```json
{
  "pipeline_spec": {
    "connection_name": "immobiliare_it_connection",
    "object": [
      {
        "table": {
          "source_table": "price_history",
          "table_configuration": {
            "start_year_month": "202401",
            "window": "1M",
            "zone_level": "com",
            "zone_filter": "100005",
            "contract": "1",
            "typology": "4"
          }
        }
      },
      {
        "table": {
          "source_table": "regions"
        }
      }
    ]
  }
}
```

- `connection_name` must point to a UC connection that uses this connector (with `externalOptionsAllowList` set as described above).
- The first run reads price history for municipality `100005`, sale contract, typology `4` (Appartamento), starting at `2024-01`. Each `(zone × contract × typology × month)` cell is one POST and one Spark partition.
- Subsequent runs use the stored cursor; `start_year_month` is only consulted when no offset exists yet.
- `regions` is a single driver-side call that re-snapshots ~20 rows.

## Data Type Mapping

| API JSON Type                           | Example Fields                                                | Connector / Spark Type |
|-----------------------------------------|---------------------------------------------------------------|------------------------|
| string (zone IDs, names)                | `id_zone`, `id_reg`, `nome`, `nome_reg`, `cadastral_typology` | `StringType`           |
| string (taxonomy codes)                 | `ty_zone`, `window`, `series_type`, `series_key`, `nation`    | `StringType`           |
| number (year, month, contract, typology)| `year`, `month`, `contract`, `typology`, `year_month`         | `IntegerType`          |
| number (prices, ratios, raw counts)     | `price_avg`, `price_avgin`, `price_avgout`, `value`, `qtraw`, `qt_raw`, `qt_minrooms`, `pc_raw`, `pc_1floor`, `pc_1typology`, `pc_garden`, `pc_minrooms`, `pc_status`, `pc_terrace` | `DoubleType`           |
| object keyed by string code             | `maintenance_status`, `rooms`, `pc_rooms` inner per-room dict | Exploded into rows with `series_type` / `series_key` columns |
| array of time-series records            | `price_sqm_avg`, `compravendite_price_sqm_avg`, `sales_qtraw` | Exploded into rows; `series_key` is `"_"` for flat arrays |

## Known Limitations and Gotchas

- **Paid, authenticated API.** Market Explorer is a commercial product; there is no free tier. OAuth2 tokens last ~14,400 seconds (~4 hours). The connector caches the token in memory and only re-grants on `401` or just before expiry. Re-authenticating per request **can trigger a temporary IP ban** (the auth docs explicitly warn against this) — do not bypass the cached token manager.
- **History endpoints require zone fan-out.** There is no "give me everything since X" bulk call. Every history POST is scoped to a single `(ty_zone, id_zone, contract, window, year, month, typology|cadastral_typology)` cell, so the connector enumerates zones once at driver init and POSTs one request per cell across executors. A full backfill at `zone_level=com` (~7,900 municipalities) × 2 contracts × 8 typologies × 12 months is well over a million calls — **scope with `zone_filter` or coarser `zone_level` for first runs**.
- **`sales_*_history` use `cadastral_typology` (string A-codes), not `typology` (integers).** Mixing them up has no effect (the connector picks the right field per table) but is a frequent source of "why is my filter being ignored?" tickets. Use the right field for the right table.
- **History responses are nested dict-of-arrays.** The API returns objects like `maintenance_status: {"1": [...], "2": [...]}` and `rooms: {"1": [...], "m5": [...]}`. The connector explodes them into one flat row per `(zone, contract, year, month, series_type, series_key)` tuple. Downstream queries should filter on `series_type` and `series_key` to isolate a specific sub-series.
- **`refresh_token` exchange is undocumented.** The `/oauth/token` response includes a `refresh_token` field, but its exchange endpoint, request format, and TTL are not published. The connector ignores it and falls back to a fresh password grant whenever the cached access token expires.
- **Sandbox data is sparser than production.** Some zones in sandbox return `404` or `422` for cells with no data. The connector swallows these per-cell so a single empty zone does not fail the whole micro-batch — empty cells simply contribute zero rows.
- **Italy-only.** The connector supports `nation=IT` only. The Greece variants (`avm_greece`, `comps-gr`) and the AVM, Comps Finder, Property Risk, and Perizie services are explicitly out of scope.
- **Open questions inherited from the API docs** (see [`immobiliare_it_api_doc.md`](./immobiliare_it_api_doc.md) §"Open Questions"):
  - **`ads_history` field semantics**: the response uses the same `price_avg` / `price_avgin` / `price_avgout` field names as `price_history`, but the endpoint description says "historical series of listings stock". Whether these fields represent ad counts or asking prices is undocumented.
  - **`price_avgin` / `price_avgout` meaning**: not explained in the docs. Possibly inside/outside the zone boundary, or internal/external pricing — needs clarification with the vendor.
  - **Rate limits**: no published per-second / per-day quotas. The auth docs only warn that excessive re-authentication can trigger a temporary ban. The connector retries `429` / `5xx` with exponential backoff but cannot pre-emptively rate-limit without published numbers.
  - **`success_if_empty` on history endpoints**: documented for the non-history variants only. The connector defensively swallows `404` / `422` per cell to behave as if `success_if_empty` were set.

## Troubleshooting

- **A partition produces no rows**: Check that the zone has data at the requested `(year, month)` and `contract`. Sandbox in particular returns sparse data — try a recent month (e.g. last 3 months) at province level (`zone_level=pro`) to confirm the credential pack is working before fanning out to municipalities.
- **401 loops / token errors**: Confirm `client_id`, `client_secret`, `username`, and `password` are all set on the UC connection. The connector force-refreshes the token once on `401`; if a second `401` arrives it surfaces the error rather than re-authenticating in a loop (which would risk an IP ban). If the issue persists, regenerate credentials via Immobiliare.it Insights and verify they work against the matching `base_url` (sandbox vs production credentials are not interchangeable).
- **Sales table filter is being ignored**: You are likely setting `typology` (integer) on a `sales_*_history` table. Sales endpoints accept `cadastral_typology` (string `A1`–`A11`) instead. Conversely, setting `cadastral_typology` on a listing-side table has no effect.
- **First run is too slow**: A `zone_level=com` backfill produces tens of thousands of partitions. Bound the first run with `zone_filter` (a comma-separated list of `id_zone` values from the `regions`/`provinces` snapshot) or coarsen `zone_level` to `pro` or `reg`. Once the backfill catches up, switch back to the finer level for incremental loads.
- **Sandbox returns 5xx**: Sandbox is less reliable than production. The connector retries `429` / `500` / `502` / `503` / `504` with exponential backoff up to 5 attempts. Persistent 5xx for the same zone usually clears within a few minutes; if not, re-run the pipeline.

## References

- Connector implementation: [`immobiliare_it.py`](./immobiliare_it.py)
- Authentication / OAuth token manager: [`immobiliare_it_auth.py`](./immobiliare_it_auth.py)
- Connector schemas and constants: [`immobiliare_it_schemas.py`](./immobiliare_it_schemas.py)
- Detailed API documentation and quirks: [`immobiliare_it_api_doc.md`](./immobiliare_it_api_doc.md)
- Official Immobiliare.it Insights documentation root: <https://insights.immobiliare.it/webdocs/getting-started/>
- Authentication (OAuth2 password grant): <https://insights.immobiliare.it/webdocs/authentication/>
- Market Explorer service getting started: <https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/getting-started/>

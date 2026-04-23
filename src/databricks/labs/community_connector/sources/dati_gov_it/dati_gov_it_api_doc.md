# **dati.gov.it CKAN Action API Documentation**

## **Authorization**

- **Chosen method**: No authentication required — anonymous (public) reads.
- **Base URL**: `https://www.dati.gov.it/opendata`
- **API version**: CKAN Action API v3

dati.gov.it is Italy's national open data portal. The CKAN Action API is publicly accessible for all read operations without an API key. The portal follows the CKAN convention that anonymous users have full read access; an API token is only required for write operations (creating/editing datasets, organizations, etc.), which this connector does not perform.

If a future use-case requires write operations, CKAN API tokens are passed via the `Authorization` HTTP header:

```
Authorization: <api_token>
```

Tokens are generated from a user's profile page (User Profile > Manage > API tokens) or programmatically via the `api_token_create` action. For this read-only connector, no token is needed.

**Example unauthenticated request** (confirms no key is needed):

```bash
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=2&start=0"
```

**Notes:**
- The portal documents recommend making calls of at most 100 records at a time (`limit=100`) as a conservative guideline, though CKAN itself supports up to 1000 rows per `package_search` request (see Pagination section).
- The `help` field in every response provides a URL to the CKAN documentation for that action. This is always returned and can be ignored by the connector.


## **Object List**

The connector exposes five objects from the dati.gov.it CKAN catalog. The list is **static** (defined by the connector); object names are not dynamically discovered.

| Object | Description | Primary Endpoint(s) | Ingestion Type |
|--------|-------------|---------------------|----------------|
| `packages` | Datasets (the main fact table). Each package contains metadata about a dataset published by a PA (Public Administration) body. | `package_search` (preferred) | `cdc` |
| `resources` | Files and distributions nested within packages (CSV, JSON, XML, etc.). Flattened from `package_search` results to avoid an extra API call per package. | Derived from `package_search` results | `cdc` |
| `organizations` | Publisher bodies (PA entities). Each dataset belongs to exactly one organization. | `organization_list` + `organization_show` | `snapshot` |
| `tags` | Free-form keyword labels applied to packages. | `tag_list` + `tag_show` | `snapshot` |
| `groups` | Thematic categories (e.g., "Governo", "Ambiente", "Salute"). | `group_list` + `group_show` | `snapshot` |

**Layering note**: `resources` live inside `packages`. The preferred approach is to flatten them out of `package_search` results (which already includes the full `resources` array in each package record) rather than calling `resource_show` individually per resource. This avoids O(N) extra API calls.


## **Object Schema**

### General notes

- All timestamps in the API are ISO 8601 strings (e.g., `"2024-03-08T18:45:38.931032"`). They are returned without a timezone suffix on dati.gov.it — they should be treated as UTC.
- `id` fields are UUID strings (e.g., `"09c9a6b5-758e-4a53-a9bc-063f9b2d6ec7"`).
- The `extras` array contains DCAT-AP_IT metadata (Italian national open data profile) as key-value pairs. Not all packages have all extras keys populated.

---

### `packages` object (primary table)

**Source endpoint**: `GET /api/3/action/package_search`

**Key behavior**: Returns a paginated list of datasets. Each record includes nested `organization`, `resources`, `tags`, `groups`, and `extras`. This is the primary fact table; all other tables can be derived or joined from it.

**Top-level fields**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique identifier for the dataset (stable, immutable). |
| `name` | string | URL-safe slug identifier (e.g., `dataset_room_87_quwlw`). Unique within the portal. |
| `title` | string | Human-readable dataset title. |
| `notes` | string or null | Dataset description (plain text or Markdown). |
| `type` | string | Always `"dataset"` for package records. |
| `state` | string | `"active"` or `"deleted"`. |
| `private` | boolean | Whether the dataset is private (public datasets have `false`). |
| `isopen` | boolean | Whether the dataset has an open license. |
| `license_id` | string or null | License identifier (e.g., `"cc-by"`). |
| `license_title` | string or null | License display name (e.g., `"Creative Commons Attribution"`). |
| `license_url` | string or null | URL to the license text. |
| `url` | string or null | External URL related to the dataset (publisher's page). |
| `version` | string or null | Dataset version string. |
| `author` | string or null | Author name. |
| `author_email` | string or null | Author email. |
| `maintainer` | string or null | Maintainer name. |
| `maintainer_email` | string or null | Maintainer email. |
| `creator_user_id` | string (UUID) | CKAN user ID of who created this record. |
| `owner_org` | string (UUID) | Organization UUID this dataset belongs to. Matches `organization.id`. |
| `metadata_created` | string (ISO 8601) | When the dataset record was first created in CKAN. |
| `metadata_modified` | string (ISO 8601) | When the dataset record was last modified in CKAN. Used as incremental watermark. |
| `num_resources` | integer | Number of resources (distributions) attached to the dataset. |
| `num_tags` | integer | Number of tags applied to the dataset. |
| `holder_name` | string or null | Name of the data holder (DCAT-AP_IT field surfaced at top level). |
| `holder_identifier` | string or null | Identifier of the data holder. |
| `dataset_is_local` | boolean | Whether the dataset is directly managed by this portal vs. harvested from elsewhere. |
| `organization` | struct | Nested organization object (see organization nested schema). |
| `resources` | array\<struct\> | Array of resource/distribution objects (see resource nested schema). |
| `tags` | array\<struct\> | Array of tag objects (see tag nested schema). |
| `groups` | array\<struct\> | Array of group objects (see group nested schema). |
| `extras` | array\<struct\> | Array of `{key, value}` pairs with DCAT-AP_IT metadata (see Extras section). |
| `relationships_as_subject` | array | Usually empty; CKAN inter-dataset relationships. |
| `relationships_as_object` | array | Usually empty; CKAN inter-dataset relationships. |

**Nested `organization` struct** (within package record):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Organization unique ID. |
| `name` | string | URL-safe organization slug (e.g., `"aci"`). |
| `title` | string | Display name (e.g., `"ACI - Automobile Club Italia"`). |
| `type` | string | Always `"organization"`. |
| `description` | string | Organization description (may be empty). |
| `image_url` | string | Logo URL (may be empty). |
| `created` | string (ISO 8601) | Organization creation timestamp. |
| `is_organization` | boolean | Always `true` for organization objects. |
| `approval_status` | string | `"approved"` for active organizations. |
| `state` | string | `"active"` or `"deleted"`. |

**Nested `resource` struct** (element of `resources` array within package):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique resource identifier. |
| `package_id` | string (UUID) | ID of the parent package. |
| `name` | string | Resource display name. |
| `description` | string | Resource description. |
| `url` | string | Direct download URL. |
| `format` | string | File format (e.g., `"CSV"`, `"JSON"`, `"XML"`, `"ZIP"`). |
| `distribution_format` | string | DCAT-AP_IT distribution format (mirrors `format`). |
| `mimetype` | string or null | MIME type (e.g., `"text/csv"`). |
| `mimetype_inner` | string or null | Inner MIME type for compressed files. |
| `size` | integer or null | File size in bytes (may be `0` or `null` if unknown). |
| `hash` | string | Checksum hash (may be empty). |
| `state` | string | `"active"` or `"deleted"`. |
| `position` | integer | Ordering index within the package's resource list (0-based). |
| `created` | string (ISO 8601) | When this resource was created. |
| `last_modified` | string (ISO 8601) or null | When this resource was last updated. |
| `metadata_modified` | string (ISO 8601) | When the resource metadata record was last modified. |
| `url_type` | string or null | `"upload"` for files hosted on CKAN, `null` for external links. |
| `resource_type` | string or null | Usually null. |
| `datastore_active` | boolean | Whether the resource is loaded in the CKAN DataStore. |
| `cache_url` | string or null | Cached copy URL. |
| `cache_last_updated` | string or null | When the cache was last refreshed. |
| `webstore_url` | string or null | Legacy field; usually null. |
| `webstore_last_updated` | string or null | Legacy field; usually null. |
| `license` | string or null | License URL (DCAT-AP_IT field). |
| `license_id` | string or null | License identifier string. |
| `license_type` | string or null | Controlled vocabulary URL for license type (e.g., `"https://w3id.org/italia/controlled-vocabulary/licences/A21_CCBY40"`). |
| `rights` | string or null | Access rights URI (e.g., `"http://publications.europa.eu/resource/authority/access-right/PUBLIC"`). |
| `modified` | string or null | Resource last-modified date (may use `"DD-MM-YYYY"` string format). |
| `access_url` | string or null | Access URL (may differ from download URL). |
| `uri` | string or null | Unique resource URI. |

**Nested `tag` struct** (element of `tags` array within package):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique tag identifier. |
| `name` | string | Tag name (e.g., `"ambiente"`). |
| `display_name` | string | Human-readable tag name. |
| `state` | string | `"active"` or `"deleted"`. |
| `vocabulary_id` | string or null | Vocabulary UUID if tag belongs to a controlled vocabulary; `null` for free-form tags. |

**Nested `group` struct** (element of `groups` array within package):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique group identifier. |
| `name` | string | URL-safe group slug (e.g., `"governo"`). |
| `title` | string | Group display name (e.g., `"Governo"`). |
| `display_name` | string | Same as `title`. |
| `description` | string | Group description. |
| `image_display_url` | string | Group logo URL. |

**`extras` array (DCAT-AP_IT metadata)**:

The `extras` field is an array of `{"key": "...", "value": "..."}` objects. The keys are defined by the DCAT-AP_IT profile (ckanext-dcatapit). Not all keys will be present on every package. Common keys observed:

| Key | Description |
|-----|-------------|
| `identifier` | Unique dataset identifier (URI or string) per DCAT-AP_IT. |
| `theme` | JSON-encoded list of EU Open Data Portal theme URIs (e.g., `["http://publications.europa.eu/resource/authority/data-theme/GOVE"]`). |
| `subthemes` | JSON-encoded list of sub-theme URIs. |
| `contact_point` | JSON-encoded contact point object (name, email, type). |
| `publisher` | JSON-encoded publisher object (name, identifier). |
| `holder_name` | Name of the data holder. |
| `holder_identifier` | Identifier of the data holder. |
| `issued` | Dataset publication date in ISO 8601. |
| `modified` | Dataset last modification date in ISO 8601. |
| `frequency` | Update frequency URI (EU controlled vocabulary). |
| `temporal_start` | Start of temporal coverage (ISO 8601 date). |
| `temporal_end` | End of temporal coverage (ISO 8601 date). |
| `geographical_name` | Geographic area name the dataset covers. |
| `geographical_geonames_url` | GeoNames URI for the geographic area. |
| `language` | Dataset language code. |
| `conforms_to` | JSON-encoded list of standards the dataset conforms to. |
| `is_version_of` | URI of the dataset this is a version of. |
| `source_catalog_description` | Description of the source catalog (for harvested datasets). |
| `accrual_periodicity` | Accrual periodicity URI. |

> The connector should preserve all extras as an `array<struct<key: string, value: string>>` column rather than flattening to avoid schema conflicts between packages.

**Example `package_search` request and response**:

```bash
# Full scan, first page
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=100&start=0&sort=metadata_modified+asc"

# Incremental: packages modified after a watermark
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=100&start=0&fq=metadata_modified:[2026-01-01T00:00:00Z+TO+NOW]&sort=metadata_modified+asc"
```

**Abbreviated example response**:

```json
{
  "help": "https://www.dati.gov.it/opendata/api/3/action/help_show?name=package_search",
  "success": true,
  "result": {
    "count": 69272,
    "sort": "metadata_modified asc",
    "facets": {},
    "results": [
      {
        "id": "09c9a6b5-758e-4a53-a9bc-063f9b2d6ec7",
        "name": "dataset_room_87_quwlw",
        "title": "Consiglio Regionale della Campania – Interrogazioni - 2013",
        "notes": "Lista delle Interrogazioni d'Aula presso il Consiglio Regionale della Campania - 2013",
        "type": "dataset",
        "state": "active",
        "private": false,
        "isopen": true,
        "license_id": "cc-by",
        "license_title": "Creative Commons Attribution",
        "license_url": "http://www.opendefinition.org/licenses/cc-by",
        "metadata_created": "2024-03-08T18:45:38.931032",
        "metadata_modified": "2024-03-08T18:45:38.931037",
        "num_resources": 2,
        "num_tags": 4,
        "holder_name": "Consiglio Regionale della Campania",
        "holder_identifier": "cr_campa",
        "dataset_is_local": false,
        "owner_org": "92f7108f-865e-4967-950d-68fee165c412",
        "organization": {
          "id": "92f7108f-865e-4967-950d-68fee165c412",
          "name": "consiglio-regionale-della-campania",
          "title": "Consiglio Regionale della Campania",
          "type": "organization",
          "description": "",
          "image_url": "",
          "created": "2024-03-08T18:44:04.035266",
          "is_organization": true,
          "approval_status": "approved",
          "state": "active"
        },
        "resources": [
          {
            "id": "50c74197-7772-4169-95df-16c1b9ef82b7",
            "package_id": "09c9a6b5-758e-4a53-a9bc-063f9b2d6ec7",
            "name": "Consiglio Regionale della Campania – Interrogazioni - 2013",
            "format": "CSV",
            "distribution_format": "CSV",
            "url": "http://opendata-crc.di.unisa.it/dataset/.../download/file.csv",
            "state": "active",
            "position": 0,
            "created": "2018-03-29T11:19:37.953678",
            "last_modified": "2018-03-30T12:53:36.983638",
            "metadata_modified": "2024-03-08T18:45:38.919701",
            "license_type": "https://w3id.org/italia/controlled-vocabulary/licences/A21_CCBY40",
            "rights": "http://publications.europa.eu/resource/authority/access-right/PUBLIC",
            "datastore_active": true
          }
        ],
        "tags": [
          {
            "id": "edc04f38-5025-4970-98d5-91a50f1316aa",
            "name": "campania",
            "display_name": "campania",
            "state": "active",
            "vocabulary_id": null
          }
        ],
        "groups": [
          {
            "id": "6f96c1fc-1107-4162-bc0d-f3dbdbd057c4",
            "name": "governo",
            "title": "Governo",
            "display_name": "Governo",
            "description": "Governo e settore pubblico",
            "image_display_url": ""
          }
        ],
        "extras": [
          { "key": "source_catalog_description", "value": "Portale OpenData" }
        ]
      }
    ],
    "search_facets": {}
  }
}
```

---

### `resources` object (derived from packages)

**Source**: Flattened from the `resources` array within each `package_search` result. No separate API call required.

**Key behavior**: Each row in the `resources` table corresponds to one element in a package's `resources` array. The `package_id` field links each resource back to its parent package.

**Primary key**: `id` (UUID, unique per resource across the portal).

**Complete field schema**: All fields in the nested `resource` struct documented above under `packages` apply directly, plus the `package_id` field which is always present.

**Alternative endpoint** (`resource_show`): If a resource ID is known, it can be fetched individually:

```bash
curl "https://www.dati.gov.it/opendata/api/3/action/resource_show?id=50c74197-7772-4169-95df-16c1b9ef82b7"
```

Response envelope: `{"success": true, "result": {<resource object>}}`.

> Assumption: The connector prefers to derive resources from `package_search` to avoid O(N) extra calls. `resource_show` is documented here for completeness and for targeted lookups.

---

### `organizations` object

**Source endpoints**:
1. `GET /api/3/action/organization_list` — returns a paginated list of organization name slugs.
2. `GET /api/3/action/organization_show?id={name_or_id}` — returns full organization details.

**Ingestion approach**: Enumerate all organizations via `organization_list`, then fetch each with `organization_show`.

**Full organization schema** (from `organization_show`):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique organization identifier. |
| `name` | string | URL-safe slug (e.g., `"aci"`). |
| `title` | string | Display name (e.g., `"ACI - Automobile Club Italia"`). |
| `display_name` | string | Same as `title`. |
| `description` | string | Organization description (may be empty). |
| `image_url` | string | Logo URL (may be empty). |
| `image_display_url` | string | Processed logo URL. |
| `created` | string (ISO 8601) | When the organization was created in CKAN. |
| `is_organization` | boolean | Always `true`. |
| `approval_status` | string | `"approved"` for active organizations. |
| `state` | string | `"active"` or `"deleted"`. |
| `type` | string | Always `"organization"`. |
| `num_followers` | integer | Number of users following the organization. |
| `package_count` | integer | Number of datasets published by this organization. |
| `identifier` | string | Optional external identifier. |
| `email` | string | Contact email (may be a PEC/certified email, may be empty). |
| `site` | string | Organization website URL (may be empty). |
| `telephone` | string | Contact telephone (may be empty). |
| `tags` | array | Usually empty for organizations. |
| `groups` | array | Groups this organization belongs to (usually empty). |
| `users` | array\<struct\> | Admin users of the organization (each user object has `id`, `name`, `fullname`, `capacity`, `state`, `created`, `sysadmin`). |

**Example requests**:

```bash
# Step 1: List all organization slugs (paginated)
curl "https://www.dati.gov.it/opendata/api/3/action/organization_list?limit=1000&offset=0"

# Step 2: Fetch full details for each org
curl "https://www.dati.gov.it/opendata/api/3/action/organization_show?id=aci&include_datasets=false"
```

**`organization_list` response**:

```json
{
  "help": "https://www.dati.gov.it/opendata/api/3/action/help_show?name=organization_list",
  "success": true,
  "result": ["aci", "agenzia-delle-dogane-e-dei-monopoli", "agenzia-di-tutela-della-salute-della-brianza"]
}
```

**`organization_show` response** (abbreviated):

```json
{
  "success": true,
  "result": {
    "id": "28bccb62-da5a-4610-96f8-eb274abe4245",
    "name": "aci",
    "title": "ACI - Automobile Club Italia",
    "display_name": "ACI - Automobile Club Italia",
    "description": "",
    "email": "automobileclubitalia@pec.aci.it",
    "site": "http://lod.aci.it/",
    "telephone": "",
    "identifier": "aci",
    "created": "2024-03-08T18:07:00.168891",
    "is_organization": true,
    "approval_status": "approved",
    "state": "active",
    "type": "organization",
    "num_followers": 0,
    "package_count": 35
  }
}
```

> `include_datasets=false` prevents `organization_show` from embedding the first 10 datasets (CKAN default). Pass it to avoid inflating the response and to keep schemas predictable.

---

### `tags` object

**Source endpoints**:
1. `GET /api/3/action/tag_list` — returns a list of all tag names.
2. `GET /api/3/action/tag_show?id={name_or_id}` — returns full tag details.

**Ingestion approach**: Enumerate all tags via `tag_list`, then fetch details with `tag_show`. Tags are simple objects; the list can also be used directly without `tag_show` for name-only ingestion.

**Full tag schema** (from `tag_show`):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique tag identifier. |
| `name` | string | Tag name (e.g., `"ambiente"`). |
| `display_name` | string | Human-readable tag name (usually same as `name`). |
| `vocabulary_id` | string or null | Vocabulary UUID if part of a controlled vocabulary; `null` for free-form tags. |

**Example requests**:

```bash
# List all tags (paginated by offset)
curl "https://www.dati.gov.it/opendata/api/3/action/tag_list?limit=1000&offset=0"

# Fetch tag details
curl "https://www.dati.gov.it/opendata/api/3/action/tag_show?id=ambiente"
```

**`tag_list` response** (abbreviated):

```json
{
  "success": true,
  "result": ["accessibilita", "acqua", "agricoltura", "ambiente", "anagrafe"]
}
```

**`tag_show` response**:

```json
{
  "success": true,
  "result": {
    "id": "5a2dbed1-8dc6-4d3b-a4a6-8f9d8b3715ee",
    "name": "ambiente",
    "display_name": "ambiente",
    "vocabulary_id": null
  }
}
```

> `tag_show` also supports an optional `include_datasets=true` parameter that embeds up to 1000 associated packages. Set `include_datasets=false` (the default) to keep the response small during snapshot ingestion.

---

### `groups` object

**Source endpoints**:
1. `GET /api/3/action/group_list` — returns a list of all group name slugs.
2. `GET /api/3/action/group_show?id={name_or_id}` — returns full group details.

**Ingestion approach**: Enumerate all groups via `group_list`, then fetch each with `group_show`.

**Full group schema** (from `group_show`):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique group identifier. |
| `name` | string | URL-safe slug (e.g., `"governo"`). |
| `title` | string | Display name (e.g., `"Governo"`). |
| `display_name` | string | Same as `title`. |
| `description` | string | Group description (e.g., `"Governo e settore pubblico"`). |
| `image_url` | string | Group logo URL (may be empty). |
| `image_display_url` | string | Processed logo URL. |
| `created` | string (ISO 8601) | When the group was created in CKAN. |
| `is_organization` | boolean | `false` for groups (distinguishes them from organizations). |
| `approval_status` | string | `"approved"` for active groups. |
| `state` | string | `"active"` or `"deleted"`. |
| `type` | string | Always `"group"`. |
| `num_followers` | integer | Number of followers. |
| `package_count` | integer | Number of datasets in this group. |
| `tags` | array | Usually empty. |
| `extras` | array | Usually empty. |
| `groups` | array | Sub-groups (usually empty). |
| `users` | array\<struct\> | Admin users of the group. |

**Example requests**:

```bash
# List all groups
curl "https://www.dati.gov.it/opendata/api/3/action/group_list?limit=1000&offset=0"

# Fetch group details
curl "https://www.dati.gov.it/opendata/api/3/action/group_show?id=governo&include_datasets=false"
```

**`group_list` response**:

```json
{
  "success": true,
  "result": ["agricoltura", "ambiente", "cultura", "economia", "governo", "istruzione", "salute", "trasporti"]
}
```


## **Get Object Primary Keys**

Primary keys are defined statically based on the CKAN resource model:

| Object | Primary Key | Type | Notes |
|--------|-------------|------|-------|
| `packages` | `id` | string (UUID) | Globally unique, immutable after creation. |
| `resources` | `id` | string (UUID) | Globally unique, immutable. |
| `organizations` | `id` | string (UUID) | Globally unique, immutable. |
| `tags` | `id` | string (UUID) | Globally unique. |
| `groups` | `id` | string (UUID) | Globally unique. |

There is no API endpoint to discover primary keys — they are always present in every object response as the `id` field.


## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `packages` | `cdc` | Each dataset has a `metadata_modified` timestamp that advances when any field changes. Supports upsert-based incremental sync. Deleted datasets change `state` to `"deleted"` (soft-delete) — no hard deletes. |
| `resources` | `cdc` | Resources have their own `metadata_modified` field. Derived from packages during package sync — incremental package scan also captures resource changes. Soft-deleted resources set `state="deleted"`. |
| `organizations` | `snapshot` | Organization count is bounded (hundreds, not thousands). No incremental cursor is available on `organization_list`. Full snapshot refresh is the appropriate strategy. |
| `tags` | `snapshot` | Tags are small, stable, and unordered. Full snapshot refresh is appropriate. |
| `groups` | `snapshot` | Thematic groups are a small, stable reference set (~10–30 groups). Full snapshot refresh is appropriate. |

**For `packages` (and derived `resources`) incremental strategy**:
- **Cursor field**: `metadata_modified`
- **Cursor type**: ISO 8601 datetime string
- **Order**: Sort by `metadata_modified asc` so the cursor advances monotonically
- **Lookback**: Apply a short lookback window (e.g., subtract 5 minutes from the last cursor value) to handle records that were in-flight during the previous sync
- **Deletes**: CKAN uses soft-deletes (`state = "deleted"`). The connector does not need to call a separate delete endpoint — deleted packages appear in `package_search` results when the `state` changes to `deleted`. Include `include_deleted=true` in the `package_search` call to catch soft-deleted records.


## **Read API for Data Retrieval**

### `package_search` (primary read endpoint for packages and resources)

- **HTTP method**: `GET` (also accepts `POST`)
- **Full URL**: `https://www.dati.gov.it/opendata/api/3/action/package_search`
- **Returns**: Packages with embedded resources, tags, groups, organization, and extras.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `q` | string | no | `"*:*"` | Solr free-text search query. Use `"*:*"` for all packages. |
| `fq` | string | no | — | Solr filter query. Does not affect relevance scoring. See filter examples below. |
| `fq_list` | array | no | — | List of filter queries (alternative to `fq`; can pass multiple filters). |
| `rows` | integer | no | `10` | Number of results to return. Maximum `1000`. |
| `start` | integer | no | `0` | Offset for pagination. |
| `sort` | string | no | `"score desc, metadata_modified desc"` | Sort order. Use `"metadata_modified asc"` for stable incremental pagination. |
| `include_deleted` | boolean | no | `false` | Include packages with `state="deleted"` in results. Set `true` to catch soft-deletes. |
| `include_drafts` | boolean | no | `false` | Include draft packages (requires auth; not relevant for read-only connector). |
| `facet` | boolean | no | `false` | Whether to return facet counts. Set `false` to reduce response size. |

**Solr `fq` filter syntax and connector config filters**:

| Config filter | `fq` expression | Example |
|---------------|-----------------|---------|
| `organization` (by slug) | `fq=organization:{slug}` | `fq=organization:aci` |
| `tags` (one tag) | `fq=tags:{tag_name}` | `fq=tags:ambiente` |
| `groups` (by slug) | `fq=groups:{slug}` | `fq=groups:governo` |
| `format` (resource format) | `fq=res_format:{FORMAT}` | `fq=res_format:CSV` |
| `free-text` | `q={text}` | `q=matrimoni` |
| Incremental watermark | `fq=metadata_modified:[{start}+TO+NOW]` | `fq=metadata_modified:[2026-01-01T00:00:00Z+TO+NOW]` |
| Date range | `fq=metadata_modified:[{start}+TO+{end}]` | `fq=metadata_modified:[2026-01-01T00:00:00Z+TO+2026-04-01T00:00:00Z]` |

Multiple `fq` filters can be combined using `fq=+filter1+filter2` or by using the `fq_list` array parameter:

```bash
# Filter by organization AND format
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=100&fq=organization:aci+res_format:CSV"

# Incremental with organization filter
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=100&start=0&fq=metadata_modified:[2026-04-01T00:00:00Z+TO+NOW]+organization:aci&sort=metadata_modified+asc"
```

**Pagination strategy** (cursor-based offset):

The API uses `rows` (page size) and `start` (offset). There is no token-based cursor; the connector must iterate by incrementing `start` until fewer than `rows` results are returned, or until `start >= count`.

```
page 1: rows=1000, start=0
page 2: rows=1000, start=1000
page 3: rows=1000, start=2000
...
stop when: len(results) < rows  OR  start >= count
```

**Maximum rows per request**: 1000. Use `rows=1000` for maximum efficiency. If the server returns fewer results than requested, it indicates the last page.

> Note: The portal documentation recommends keeping requests to 100 records at a time for performance. However, CKAN's hard limit is 1000. Use `rows=100` if stability is a concern; `rows=1000` if throughput matters. Either is valid.

**Complete incremental read example**:

```bash
# First run: full historical backfill (or use a start_date)
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=100&start=0&sort=metadata_modified+asc&include_deleted=true"

# Subsequent runs: watermark-based incremental
WATERMARK="2026-04-22T12:00:00Z"  # last max(metadata_modified) from previous run minus 5 min lookback
curl "https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=100&start=0&fq=metadata_modified:[${WATERMARK}+TO+NOW]&sort=metadata_modified+asc&include_deleted=true"
```

---

### `organization_list` (for organizations)

- **URL**: `https://www.dati.gov.it/opendata/api/3/action/organization_list`
- **Method**: `GET`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 1000 | Max organizations to return per call. |
| `offset` | `0` | Pagination offset. |
| `all_fields` | `false` | If `true`, returns full dicts instead of name strings (use `false` + follow-up `organization_show` calls for full schema). |
| `include_dataset_count` | `true` | Include `package_count`. |
| `include_extras` | `false` | Include extras. |
| `include_groups` | `false` | Include nested groups. |
| `include_users` | `false` | Include admin users. |

**Pagination**: Use `limit`/`offset`. Default limit of 1000 with `all_fields=false` typically returns all organizations in a single call (the portal has hundreds of organizations, not thousands).

---

### `organization_show` (per-organization details)

- **URL**: `https://www.dati.gov.it/opendata/api/3/action/organization_show?id={name_or_id}`
- **Method**: `GET`

| Parameter | Description |
|-----------|-------------|
| `id` | Organization `name` slug or UUID. Required. |
| `include_datasets` | `false` — always pass this to suppress embedded dataset list. |
| `include_dataset_count` | `true` — include the `package_count`. |
| `include_extras` | `true` — include extras if needed. |
| `include_users` | `true` — include the `users` array. |

---

### `tag_list` and `tag_show`

- **`tag_list` URL**: `https://www.dati.gov.it/opendata/api/3/action/tag_list?limit=1000&offset=0`
- **`tag_show` URL**: `https://www.dati.gov.it/opendata/api/3/action/tag_show?id={name_or_id}&include_datasets=false`

The portal has thousands of tags. Paginate `tag_list` using `limit`/`offset`.

---

### `group_list` and `group_show`

- **`group_list` URL**: `https://www.dati.gov.it/opendata/api/3/action/group_list?limit=1000&offset=0`
- **`group_show` URL**: `https://www.dati.gov.it/opendata/api/3/action/group_show?id={name_or_id}&include_datasets=false`

Groups are few (typically 10–30) and a single `group_list` call suffices.

---

### Rate Limits

No explicit rate limits are documented by dati.gov.it or the upstream CKAN documentation. The portal is publicly funded and intended for open access.

**Recommended conservative rate**: 1 request per second (1 RPS). For bulk ingestion (full backfill of ~70,000 packages), target a maximum of 5 RPS to avoid straining the server. Apply exponential backoff on `429 Too Many Requests` or `503 Service Unavailable` responses.

> Assumption: No publicly documented rate limit exists; the 1 RPS recommendation is a conservative default derived from common practice for public CKAN portals. Adjust upward if sustained testing reveals no throttling.


## **Field Type Mapping**

### JSON → Connector/Spark type mapping

| CKAN JSON Type | Example Fields | Connector Logical Type | Notes |
|----------------|----------------|------------------------|-------|
| string (UUID) | `id`, `owner_org`, `package_id` | string | Keep as string; do not parse as numeric. |
| string (text) | `name`, `title`, `notes`, `format` | string | UTF-8; may contain Italian characters. |
| string (ISO 8601 datetime) | `metadata_created`, `metadata_modified`, `created`, `last_modified` | timestamp | Parse as UTC timestamp. Strings lack timezone suffix on this portal — assume UTC. |
| string (date, "DD-MM-YYYY") | `modified` (resource field) | string or date | Resource `modified` field may use `"DD-MM-YYYY"` format (non-ISO). Parse carefully or keep as string. |
| boolean | `isopen`, `private`, `is_organization`, `datastore_active` | boolean | Standard true/false. |
| integer | `num_resources`, `num_tags`, `position`, `size`, `package_count` | integer (32-bit) | `size` may be 0 or null when unknown. |
| null | Many fields (e.g., `last_modified`, `resource_type`, `cache_url`) | corresponding type + nullable | Surface as `null` in the target schema. |
| object/struct | `organization` (nested in package) | struct | Represent as nested struct, not flattened. |
| array\<struct\> | `resources`, `tags`, `groups`, `extras`, `users` | array\<struct\> | Preserve as arrays; do not flatten. |
| array\<string\> | `organization_list` result, `tag_list` result, `group_list` result | array\<string\> | Used in list-step enumeration, not as a target table column. |

### Special behaviors

- **UUID fields**: Always strings; never cast to numeric types.
- **Timestamp fields**: No timezone designator on dati.gov.it — treat as UTC. When writing to Databricks Delta tables, parse with `to_timestamp()` and store in UTC.
- **`extras` array**: Preserve as `array<struct<key: string, value: string>>`. Values may themselves be JSON-encoded strings (e.g., the `theme` key holds a JSON array). The connector should store them as raw strings; downstream transforms can parse individual keys as needed.
- **`resources.modified`**: This field sometimes uses `"DD-MM-YYYY"` format (e.g., `"23-04-2026"`) rather than ISO 8601. Handle gracefully — parse or store as a string column.
- **`format` and `distribution_format`**: Usually identical on dati.gov.it. Both are included in the schema. Some resources may show `"OP_DATPRO"` instead of an actual format due to CKAN plugin misconfiguration on the publishing side.


## **Known Quirks & Gotchas**

1. **DCAT-AP_IT extras layer**: dati.gov.it runs the `ckanext-dcatapit` extension, which adds Italian/EU-specific metadata fields. Many of these surface as `extras` key-value pairs (e.g., `theme`, `contact_point`, `frequency`). The `extras` values for structured fields like `theme` and `contact_point` are JSON-encoded strings within the string value of the extras pair. The connector should store them as raw strings.

2. **`metadata_modified` format**: Timestamps on dati.gov.it are returned without a timezone suffix (e.g., `"2024-03-08T18:45:38.931032"`) — no `Z` or `+00:00`. Treat as UTC. This differs from vanilla CKAN instances that do return the `Z` suffix.

3. **Harvested vs. local datasets**: Most packages are harvested from other PA portals (`dataset_is_local: false`). The `metadata_modified` field reflects when dati.gov.it's harvester last updated the record, not necessarily when the source portal updated it. For true change detection, use `metadata_modified` on dati.gov.it as the watermark.

4. **`resource.modified` date format**: The `modified` field on resources sometimes appears as `"DD-MM-YYYY"` (e.g., `"23-04-2026"`) rather than an ISO timestamp. Handle this field as a string or apply format-detection logic during parsing.

5. **Soft deletes only**: CKAN does not hard-delete packages or resources. Instead, `state` changes to `"deleted"`. To capture deletes, use `include_deleted=true` in `package_search` and detect records where `state = "deleted"` in the incremental window.

6. **`organization_show` returns first 10 datasets by default**: When calling `organization_show` without `include_datasets=false`, the response embeds the first 10 datasets. Always pass `include_datasets=false` to keep the response clean.

7. **`group_show` returns first 1000 datasets by default**: Similar to organizations — pass `include_datasets=false`.

8. **Base URL structure**: The CKAN API base path on dati.gov.it is `https://www.dati.gov.it/opendata/api/3/action/`, not the portal root `https://www.dati.gov.it/api/3/action/`. Both may work, but `opendata` in the path is the confirmed correct sub-path.

9. **`organization_list` with `all_fields=true` returns 500 errors**: During testing, calling `organization_list?all_fields=true&limit=2` returned HTTP 500. Use `all_fields=false` (default) to list organizations, then call `organization_show` individually per organization. This is more reliable.

10. **Scale**: The portal has ~70,000 packages. A full backfill with `rows=100` requires ~700 API calls. With `rows=1000`, it is ~70 calls. Plan ingestion capacity accordingly.

11. **Free-form tags**: Tags are not governed by a controlled vocabulary at the portal level (most have `vocabulary_id: null`). This means tags can contain duplicates, Italian-language variants, or abbreviations. The connector should preserve tag names as-is.

12. **No official rate limit documentation**: No rate limits are documented. The 1 RPS recommendation is a conservative default. Monitor for HTTP 429/503 responses in production and apply exponential backoff.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs (dati.gov.it) | https://www.dati.gov.it/api | 2026-04-23 | High | Base URL (`/opendata`), API v3, no auth for reads, endpoint list, 100-record recommendation |
| Official Docs (dati.gov.it) | https://www.dati.gov.it/sviluppatori/faq | 2026-04-23 | High | DCAT-AP_IT compliance requirements, format gotchas (`OP_DATPRO`), catalog RDF pagination |
| Official Docs (CKAN) | https://docs.ckan.org/en/latest/api/ | 2026-04-23 | High | Full Action API reference: all endpoint parameters, response envelope, `rows` max of 1000, auth header, error format |
| Official Docs (CKAN) | https://docs.ckan.org/en/latest/maintaining/configuration.html | 2026-04-23 | High | `ckan.search.rows_max` default of 1000 |
| GitHub Discussion (CKAN) | https://github.com/ckan/ckan/discussions/6479 | 2026-04-23 | High | Exact `fq=metadata_modified:[DATE TO NOW]` Solr syntax with ISO 8601 format |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/package_search?q=*:*&rows=1&start=0 | 2026-04-23 | High | Confirmed package schema: all top-level fields, nested organization/resources/tags/groups/extras structure |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/organization_list?limit=3 | 2026-04-23 | High | Confirmed organization_list returns name slugs as strings |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/organization_show?id=aci | 2026-04-23 | High | Confirmed full organization schema including email, site, telephone, identifier fields |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/group_show?id=ambiente | 2026-04-23 | High | Confirmed full group schema with all fields |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/tag_show?id=ambiente | 2026-04-23 | High | Confirmed tag schema: id, name, display_name, vocabulary_id |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/resource_show?id=50c74197... | 2026-04-23 | High | Confirmed full resource schema including license_type, rights, distribution_format, datastore_active |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/package_search?fq=organization:aci | 2026-04-23 | High | Confirmed `fq=organization:{slug}` filter works |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/package_search?fq=tags:ambiente | 2026-04-23 | High | Confirmed `fq=tags:{name}` filter works |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/package_search?fq=res_format:CSV | 2026-04-23 | High | Confirmed `fq=res_format:{FORMAT}` filter works |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/package_search?fq=groups:governo | 2026-04-23 | High | Confirmed `fq=groups:{slug}` filter works |
| Live API test | https://www.dati.gov.it/opendata/api/3/action/package_search?fq=metadata_modified:[2026-04-01T00:00:00Z+TO+NOW] | 2026-04-23 | High | Confirmed incremental date filter returns correct results (41,212 packages modified in April 2026) |
| CKAN Extension | https://extensions.ckan.org/extension/dcatapit/ | 2026-04-23 | High | DCAT-AP_IT extras field list: theme, contact_point, publisher, issued, frequency, temporal_start/end, conforms_to |
| GitHub (dati.gov.it) | https://github.com/italia/dati.gov.it | 2026-04-23 | Medium | Confirmed DKAN/Drupal evolution path; current CKAN API at `/opendata` path |


## **Sources and References**

- **Official dati.gov.it API documentation** (high confidence)
  - https://www.dati.gov.it/api
  - https://www.dati.gov.it/sviluppatori/faq

- **Official CKAN Action API documentation** (highest confidence — dati.gov.it runs CKAN, so upstream docs apply directly)
  - https://docs.ckan.org/en/latest/api/
  - https://docs.ckan.org/en/latest/maintaining/configuration.html

- **CKAN GitHub** (high confidence)
  - https://github.com/ckan/ckan/discussions/6479 — `metadata_modified` date-range query syntax
  - https://github.com/ckan/ckan/blob/master/ckan/logic/action/get.py — source of truth for action parameter definitions

- **ckanext-dcatapit extension** (high confidence for extras/DCAT-AP_IT fields)
  - https://extensions.ckan.org/extension/dcatapit/

- **Live API endpoint tests** (highest confidence for dati.gov.it-specific behavior)
  - Tested directly against `https://www.dati.gov.it/opendata/api/3/action/` on 2026-04-23
  - Confirmed: package schema, organization schema, group schema, tag schema, resource schema, all `fq` filter patterns, incremental date filter, soft-delete behavior

When conflicts arise, **live API test results** against dati.gov.it take precedence over generic CKAN documentation, since the portal runs a customized CKAN instance with the DCAT-AP_IT extension.

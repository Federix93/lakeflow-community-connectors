# Immobiliare.it Insights API — Market Explorer (ws-osservatorio-public-api)

> Scope: Market Explorer service only. AVM, AVM Greece, Comps Finder, Property Risk (ws-rischio), and Perizie are explicitly excluded.

---

## Authorization

### Method: OAuth2 Resource Owner Password Grant

The API uses a single authentication method: OAuth2 password grant. Tokens must be cached and reused — re-authenticating on every request can trigger a temporary IP ban.

**Token endpoint**

```
POST /oauth/token
Host: <same base URL as the service>
```

Both the production and sandbox base URLs host the `/oauth/token` endpoint (confirmed by the service getting-started page referencing the shared `/oauth/token` path relative to the service host).

**Headers for token request**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-www-form-urlencoded` |
| `Authorization` | HTTP Basic — `Base64(<clientID>:<secretkey>)` |

**Request body (form-encoded)**

| Parameter | Required | Value |
|---|---|---|
| `grant_type` | Yes | `password` |
| `username` | Yes | User's login username |
| `password` | Yes | User's login password |

**Token response schema**

| Field | Type | Notes |
|---|---|---|
| `access_token` | string | The Bearer token to use on subsequent calls |
| `token_type` | string | Always `"Bearer"` |
| `expires_in` | number | `14399` seconds (~4 hours) |
| `refresh_token` | string | Returned but refresh flow not documented; re-authenticate with password grant when token expires |
| `scope` | string | `"all"` |

**Using the token**

All API calls must include:
```
Authorization: Bearer <access_token>
Content-Type: application/json
```

**Token lifecycle**

- Cache the token; do not authenticate before every API call.
- Re-authenticate when a `401 invalid_token` response is received.
- `refresh_token` is returned but its exchange endpoint and TTL are not documented — treat it as TBD and fall back to password re-grant.

**Example token request (cURL)**

```bash
curl --location --request POST 'https://ws-osservatorio.realitycs.it/oauth/token' \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --header 'Authorization: Basic <Base64(clientID:secretkey)>' \
  --data-urlencode 'grant_type=password' \
  --data-urlencode 'username=<username>' \
  --data-urlencode 'password=<password>'
```

**Example token response**

```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 14399,
  "refresh_token": "def502...",
  "scope": "all"
}
```

---

## Base URLs

| Environment | Base URL |
|---|---|
| Production | `https://ws-osservatorio.realitycs.it` |
| Sandbox | `https://ws-osservatorio-dev.realitycs.it` |

Both confirmed from the Market Explorer getting-started page.

---

## Object List

The Market Explorer service exposes the following objects covered in this document:

| Table Name | Ingestion Type | Endpoint | Description |
|---|---|---|---|
| `price_history` | `cdc` | `POST /api/price/history` | Historical time-series of listing prices segmented by maintenance status, price/sqm, and room count |
| `ads_history` | `cdc` | `POST /api/ads/history` | Historical time-series of listings stock (ad counts) segmented by maintenance status and rooms |
| `search_data_history` | `cdc` | `POST /api/search-data/history` | Historical time-series of market demand/search behaviour |
| `sales_price_history` | `cdc` | `POST /api/sales/price/history` | Historical time-series of actual sale prices (from tax authority AdE data) |
| `sales_volume_history` | `cdc` | `POST /api/sales/volume/history` | Historical time-series of transaction volume (NTN from AdE) |
| `regions` | `snapshot` | `GET /api/taxonomies/geo/IT/ty_zone/reg` | Static list of Italian regions |
| `provinces` | `snapshot` | `GET /api/taxonomies/geo/IT/ty_zone/pro` | Static list of Italian provinces |
| `municipalities` | `snapshot` | `GET /api/taxonomies/geo/IT/ty_zone/com` | Static list of Italian municipalities |

All objects are static in list (there is no discovery endpoint for the list itself). The objects are enumerated above.

Seven additional tables have been documented as of 2026-04-28: the point-in-time (snapshot) twins of the five history endpoints (`price`, `ads`, `search_data`, `sales_price`, `sales_volume`) and two extra geo-taxonomy levels (`macro_zones`, `micro_zones`). See the corresponding `### Table:` sections below the `municipalities` section in the Object Schema.

---

## Taxonomy Reference

### Zone Types (`ty_zone`)

| Code | Level | Meaning |
|---|---|---|
| `reg` | 1 — broadest | Region (regione) |
| `pro` | 2 | Province (provincia) |
| `com` | 3 | Municipality / comune |
| `macro` | 4 | Macro-zone (aggregated neighbourhood zone) |
| `micro` | 5 — finest | Micro-zone (specific local zone) |

### Contract Types (`contract`)

| Code | Meaning (IT) | Meaning (EN) |
|---|---|---|
| `1` | Vendita | Sale |
| `2` | Affitto | Rental |

### Property Typologies (`typology`)

| Code | Meaning (IT) | Category |
|---|---|---|
| `4` | Appartamento | Residential |
| `5` | Attico / Mansarda | Residential |
| `7` | Casa indipendente | Residential |
| `10` | Stabile / Palazzo | Residential |
| `11` | Rustico / Casale | Residential |
| `12` | Villa | Residential |
| `13` | Villetta a schiera | Residential |
| `31` | Loft / Open space | Residential |
| `110` | Negozi | Commercial |
| `140` | Uffici | Directional |

### Maintenance Status (`maintenance_status`)

Keys returned as integer strings in response objects (e.g. `"1"`, `"2"`):

| Code | Meaning (EN) |
|---|---|
| `1` | New / Under Construction |
| `2` | Excellent / Renovated |
| `3` | Good / Habitable |
| `4` | Not Renovated |

### Time Windows (`window`)

| Value | Period |
|---|---|
| `1M` | Monthly (mensile) |
| `3M` | Quarterly (trimestrale) |
| `6M` | Semi-annual (semestrale) |
| `12M` | Annual (annuale) |

### Surface Classes (`class_surface`)

| Code | Range |
|---|---|
| `1` | < 50 m² |
| `2` | 50–85 m² |
| `3` | 85–115 m² |
| `4` | 115–145 m² |
| `5` | > 145 m² |

### Cadastral Typologies (`cadastral_typology`)

Used by sales-price and sales-volume endpoints (string codes, not integers):

| Code | Meaning (IT) |
|---|---|
| `A1` | Abitazioni di tipo signorile |
| `A2` | Abitazioni di tipo civile |
| `A3` | Abitazioni di tipo economico |
| `A4` | Abitazioni di tipo popolare |
| `A5` | Abitazioni di tipo ultrapopolare |
| `A6` | Abitazioni di tipo rurale |
| `A7` | Abitazioni in villini |
| `A8` | Abitazioni in ville |
| `A9` | Castelli, palazzi di eminenti pregi artistici o storici |
| `A11` | Abitazioni ed alloggi tipici dei luoghi |

### Temporal Taxonomy

`GET /api/taxonomies/temporal` returns all valid `(year, month)` pairs for each window size, with availability flags per data type.

Data availability starts from **2016**; meaningful data begins from approximately **2019–2020** depending on data type:
- `compravendita` (sales transactions): available from ~2019
- `energy`: available from ~2020
- `kpi_composti_compravendita` (composite KPIs): available from ~2019

The response is structured as four lists: `mesi` (monthly), `trimestri` (quarterly), `semestri` (semi-annual), `anni` (annual). Each entry has `year`, `month`, and boolean flags for data availability.

---

## Zone Enumeration Plan

History endpoints require `ty_zone` + `id_zone` as mandatory inputs. A connector must enumerate all zones to build a fan-out. Three endpoints support this:

### 1. List All Zones of a Given Type

```
GET /api/taxonomies/geo/{nation}/ty_zone/{ty_zone}
Authorization: Bearer <token>
```

Returns every zone of a given level for the entire nation. Use this to enumerate:
- All regions: `GET /api/taxonomies/geo/IT/ty_zone/reg`
- All provinces: `GET /api/taxonomies/geo/IT/ty_zone/pro`
- All municipalities: `GET /api/taxonomies/geo/IT/ty_zone/com`
- All macro-zones: `GET /api/taxonomies/geo/IT/ty_zone/macro`
- All micro-zones: `GET /api/taxonomies/geo/IT/ty_zone/micro`

Response items:

| Field | Type | Meaning |
|---|---|---|
| `id_zone` | string | Zone identifier (use as `id_zone` in history calls) |
| `id_reg` | string | Parent region identifier |
| `nome` | string | Zone name |
| `nome_reg` | string | Parent region name |

No pagination is documented; the full list is returned in a single response.

**Example cURL (list all municipalities):**

```bash
curl --location 'https://ws-osservatorio-dev.realitycs.it/api/taxonomies/geo/IT/ty_zone/com' \
  --header 'Authorization: Bearer <token>'
```

### 2. List Sub-zones of a Specific Parent Zone

```
GET /api/taxonomies/geo/{nation}/ty_zone/{ty_zone}/id_zone/{id_zone}
Authorization: Bearer <token>
```

Returns zones directly beneath a given parent zone. Useful for hierarchical traversal.

**Path parameters:**

| Parameter | Required | Type | Example |
|---|---|---|---|
| `nation` | Yes | string | `IT` |
| `ty_zone` | Yes | string | `reg` |
| `id_zone` | Yes | string | `1` (Piemonte region ID) |

Response items:

| Field | Type | Meaning |
|---|---|---|
| `id` | string | Sub-zone identifier |
| `id_pro` | string | Parent province identifier |
| `id_reg` | string | Parent region identifier |
| `nome` | string | Sub-zone name |
| `nome_pro` | string | Parent province name |
| `nome_reg` | string | Parent region name |

**Example cURL (list provinces in Piemonte):**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/taxonomies/geo/IT/ty_zone/reg/id_zone/1' \
  --header 'Authorization: Bearer <token>'
```

### 3. Geo Hierarchy by Lat/Lng

```
GET /api/taxonomies/geo/{nation}/hierarchy/lat/{lat}/lng/{lng}
Authorization: Bearer <token>
```

Returns the full administrative hierarchy for a coordinate point. Useful for associating a property address with its zone IDs across all levels.

**Path parameters:**

| Parameter | Required | Type | Example |
|---|---|---|---|
| `nation` | Yes | string | `IT` |
| `lat` | Yes | number | `45.2081156` |
| `lng` | Yes | number | `12.0291133` |

Response items (single item in array):

| Field | Type | Sub-fields |
|---|---|---|
| `regione` | object | `id` (string), `nome` (string) |
| `provincia` | object | `id` (string), `nome` (string) |
| `comune` | object | `id` (string), `nome` (string) |
| `macro` | object | `id` (string), `nome` (string) |
| `micro` | object | `id` (string), `nome` (string) |

**Example response:**

```json
{
  "_metadata": {"message": "", "query": {}, "status": 200},
  "items": [{
    "regione":  {"id": "5",        "nome": "Veneto"},
    "provincia": {"id": "28",      "nome": "Padova"},
    "comune":   {"id": "28035",    "nome": "Correzzola"},
    "macro":    {"id": "28035_D040R",  "nome": "Zona Rurale..."},
    "micro":    {"id": "28035_D040R1", "nome": "Zona Rurale..."}
  }]
}
```

### Recommended Zone Enumeration Strategy for Connector

The recommended approach for a connector that targets a specific `ty_zone` level is:

1. Call `GET /api/taxonomies/geo/IT/ty_zone/{target_level}` once to get the full flat list of all zone IDs for that level.
2. Use the returned `id_zone` values to fan out history endpoint calls.
3. For `macro` and `micro` levels (potentially tens of thousands of zones), expose a `zone_filter` configuration parameter to limit scope.

The sub-zone endpoint (`/id_zone/{id_zone}`) is useful for hierarchical top-down traversal (e.g., enumerate provinces within a region) but is not required if the flat list endpoint covers all needed zones.

---

## Object Schema

### Table: `price_history`

**Ingestion type:** `cdc`
**Cursor fields:** `year` (number), `month` (number) within each time-series record
**HTTP Method:** POST
**Endpoint:** `/api/price/history`

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type; see ty_zone taxonomy |
| `id_zone` | Yes | string | `"100005"` | Zone identifier from geo list endpoint |
| `window` | Yes | string | `"1M"` | Time window; allowed: `1M`, `3M`, `6M`, `12M` |
| `contract` | Yes | number | `1` | `1` = sale, `2` = rental |
| `year` | Yes | number | `2024` | Year of the requested snapshot date |
| `month` | Yes | number | `12` | Month of the requested snapshot date |
| `typology` | No | number | `4` | Property typology code; omit for aggregate across all typologies |
| `nation` | No | string | `"IT"` | Defaults to `IT` |

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "price_sqm_avg": [           -- array of time-series records (overall avg price/sqm)
        {
          "year":          number,
          "month":         number,
          "price_avg":     number,  -- average price per sqm
          "price_avgin":   number,  -- average price per sqm (inside/internal)
          "price_avgout":  number   -- average price per sqm (outside/external)
        }
      ],
      "maintenance_status": {       -- object keyed by maintenance_status code ("1","2","3","4")
        "<status_id>": [
          {
            "year":         number,
            "month":        number,
            "price_avg":    number,
            "price_avgin":  number,
            "price_avgout": number
          }
        ]
      },
      "rooms": {                    -- object keyed by room count ("1","2","3","4","5","m5")
        "<room_count>": [
          {
            "year":         number,
            "month":        number,
            "price_avg":    number,
            "price_avgin":  number,
            "price_avgout": number
          }
        ]
      }
    }
  ]
}
```

**Nested arrays note:** Each response contains three parallel time-series structures:
- `price_sqm_avg`: flat array of `(year, month, price_avg, price_avgin, price_avgout)` — overall aggregate
- `maintenance_status`: dict of arrays indexed by condition code `"1"–"4"`
- `rooms`: dict of arrays indexed by room count `"1"–"5"` plus `"m5"` (5+ rooms)

All three use identical record structure.

**Primary key proposal (for flattened rows):**

When normalising to a flat table, a row is uniquely identified by the input parameters plus the series dimension:
`(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)`

Where `series_type` is one of `"price_sqm_avg"`, `"maintenance_status"`, `"rooms"` and `series_key` is `"_"` / `"1"–"4"` / `"1"–"m5"` respectively.

**Pagination:** None documented. Single response per request.

**Cursor for incremental sync:** The `(year, month)` tuple within each record. The endpoint accepts a snapshot `(year, month)` and returns the history up to that date; advance the cursor by passing successive `(year, month)` values.

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/price/history' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "typology": 4,
    "nation": "IT"
  }'
```

**Verbatim response example (abbreviated):**

```json
{
  "_metadata": {
    "message": "",
    "query": {},
    "status": 200,
    "request_id": "a1b2c3d4-0000-0000-0000-000000000001"
  },
  "items": [
    {
      "price_sqm_avg": [
        {"year": 2020, "month": 6, "price_avg": 1950.5, "price_avgin": 1980.0, "price_avgout": 1900.0},
        {"year": 2020, "month": 9, "price_avg": 1965.0, "price_avgin": 1995.0, "price_avgout": 1910.0}
      ],
      "maintenance_status": {
        "1": [{"year": 2020, "month": 6, "price_avg": 2100.0, "price_avgin": 2130.0, "price_avgout": 2050.0}],
        "2": [{"year": 2020, "month": 6, "price_avg": 2050.0, "price_avgin": 2080.0, "price_avgout": 2000.0}],
        "3": [{"year": 2020, "month": 6, "price_avg": 1900.0, "price_avgin": 1930.0, "price_avgout": 1850.0}],
        "4": [{"year": 2020, "month": 6, "price_avg": 1750.0, "price_avgin": 1780.0, "price_avgout": 1700.0}]
      },
      "rooms": {
        "1": [{"year": 2020, "month": 6, "price_avg": 2200.0, "price_avgin": 2230.0, "price_avgout": 2150.0}],
        "2": [{"year": 2020, "month": 6, "price_avg": 2000.0, "price_avgin": 2030.0, "price_avgout": 1960.0}],
        "m5": [{"year": 2020, "month": 6, "price_avg": 1800.0, "price_avgin": 1830.0, "price_avgout": 1760.0}]
      }
    }
  ]
}
```

---

### Table: `ads_history`

**Ingestion type:** `cdc`
**Cursor fields:** `year` (number), `month` (number)
**HTTP Method:** POST
**Endpoint:** `/api/ads/history`

#### Request Body Parameters

Identical parameter set as `price_history`:

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Snapshot year |
| `month` | Yes | number | `12` | Snapshot month |
| `typology` | No | number | `4` | Property typology |
| `nation` | No | string | `"IT"` | Nation code |

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "price_sqm_avg": [           -- TBD: docs show same structure as price_history;
        {                          -- field name may refer to listing price avg here
          "year":         number,
          "month":        number,
          "price_avg":    number,
          "price_avgin":  number,
          "price_avgout": number
        }
      ],
      "maintenance_status": {      -- keyed by "1","2","3","4"
        "<status_id>": [
          {
            "year":         number,
            "month":        number,
            "price_avg":    number,
            "price_avgin":  number,
            "price_avgout": number
          }
        ]
      },
      "rooms": {                   -- keyed by "1","2","3","4","5","m5"
        "<room_count>": [
          {
            "year":         number,
            "month":        number,
            "price_avg":    number,
            "price_avgin":  number,
            "price_avgout": number
          }
        ]
      }
    }
  ]
}
```

**Note:** The docs note that `ads/history` retrieves "historical series of listings stock." The response structure mirrors `price/history` exactly (same field names: `price_sqm_avg`, `maintenance_status`, `rooms` with `price_avg`, `price_avgin`, `price_avgout`). Whether `price_avg` in this context represents listing count or asking price is TBD — see Open Questions.

**Primary key proposal:** Same composite as `price_history`:
`(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/ads/history' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "typology": 4,
    "nation": "IT"
  }'
```

---

### Table: `search_data_history`

**Ingestion type:** `cdc`
**Cursor fields:** `year` (number), `month` (number)
**HTTP Method:** POST
**Endpoint:** `/api/search-data/history`

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Snapshot year |
| `month` | Yes | number | `12` | Snapshot month |
| `typology` | No | number | `4` | Property typology |
| `nation` | No | string | `"IT"` | Nation code |

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "conversion_rate": [           -- time-series of demand-conversion metric
        {
          "year":  number,
          "month": number,
          "value": number
        }
      ],
      "maintenance_status": {        -- keyed by "1","2","3","4"
        "<status_id>": [
          {"year": number, "month": number, "value": number}
        ]
      },
      "pc_rooms": [                  -- room distribution percentages per period
        {
          "year":         number,
          "month":        number,
          "qt-raw":       number,    -- total raw quantity of searches
          "qt-minrooms":  number,    -- quantity with min-rooms filter applied
          "<room_key>": {            -- e.g. "1", "2", "3" ...
            "pc-raw": number,        -- percentage share for that room count
            "qt-raw": number         -- raw quantity for that room count
          }
        }
      ],
      "price_sqm_search_avg": [      -- avg spending propensity of searches (€/sqm)
        {
          "year":  number,
          "month": number,
          "value": number
        }
      ],
      "res": [                       -- search attribute percentages
        {
          "year":          number,
          "month":         number,
          "qt-raw":        number,   -- total raw search count
          "pc-1floor":     number,   -- % searches filtering on 1 floor
          "pc-1typology":  number,   -- % searches filtering on 1 typology
          "pc-garden":     number,   -- % searches with garden filter
          "pc-minrooms":   number,   -- % searches with min-rooms filter
          "pc-status":     number,   -- % searches with status filter
          "pc-terrace":    number    -- % searches with terrace filter
        }
      ],
      "rooms": {                     -- keyed by room count "1","2","3","4","5","m5" (TBD)
        "<room_count>": [
          {"year": number, "month": number, "value": number}
        ]
      }
    }
  ]
}
```

**Primary key proposal:**
`(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/search-data/history' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "typology": 4,
    "nation": "IT"
  }'
```

**Verbatim response example:**

```json
{
  "_metadata": {"message": "", "query": {}, "status": 200, "request_id": "uuid"},
  "items": [
    {
      "conversion_rate": [
        {"month": 6, "value": 1.4, "year": 2020}
      ],
      "maintenance_status": {
        "1": [{"month": 6, "value": 1.2, "year": 2020}],
        "2": [{"month": 6, "value": 1.5, "year": 2020}]
      },
      "pc_rooms": [
        {
          "1": {"pc-raw": 2.28, "qt-raw": 194},
          "month": 6,
          "qt-minrooms": 8511,
          "qt-raw": 31616,
          "year": 2020
        }
      ],
      "price_sqm_search_avg": [
        {"month": 6, "value": 2507.58, "year": 2020}
      ],
      "res": [
        {
          "month": 6, "year": 2020,
          "pc-1floor": 15.35, "pc-1typology": 49.35,
          "pc-garden": 15.51, "pc-minrooms": 16.33,
          "pc-status": 16.33, "pc-terrace": 1.62,
          "qt-raw": 31616
        }
      ],
      "rooms": {
        "1": [{"month": 6, "value": 1.1, "year": 2020}]
      }
    }
  ]
}
```

---

### Table: `sales_price_history`

**Ingestion type:** `cdc`
**Cursor fields:** `year` (number), `month` (number)
**HTTP Method:** POST
**Endpoint:** `/api/sales/price/history`

Note: path is `/api/sales/price/history` — note the `/sales/` sub-path (different from `/api/price/history`).

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Snapshot year |
| `month` | Yes | number | `12` | Snapshot month |
| `cadastral_typology` | No | string | `"A4"` | Cadastral class filter (A1–A11); omit for aggregate |
| `nation` | No | string | `"IT"` | Nation code; defaults to `IT` |

Note: `typology` (property typology code) is NOT a parameter for sales endpoints; instead, use `cadastral_typology` (string A-codes).

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "compravendite_price_sqm_avg": [   -- time-series of avg sale price/sqm
        {
          "year":      number,
          "month":     number,
          "price_avg": number            -- average price per sqm (€)
        }
      ]
    }
  ]
}
```

**Note:** The point-in-time (non-history) `/api/sales/price` endpoint additionally returns `compravendite_price_sqm_max`, `compravendite_price_sqm_min`, `compravendite_sales_price_avg`, and `price_cadastral_typologies` arrays. The history endpoint returns only `compravendite_price_sqm_avg` time-series (confirmed from docs).

**Primary key proposal:**
`(ty_zone, id_zone, contract, window, cadastral_typology, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/sales/price/history' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "cadastral_typology": "A4",
    "nation": "IT"
  }'
```

**Verbatim response example:**

```json
{
  "_metadata": {
    "message": "",
    "query": {},
    "status": 200,
    "request_id": "4cc03232-7427-4f7e-ae64-762372e9e14b"
  },
  "items": [
    {
      "compravendite_price_sqm_avg": [
        {"month": 6,  "price_avg": 1431, "year": 2020},
        {"month": 9,  "price_avg": 1480, "year": 2020},
        {"month": 12, "price_avg": 1538, "year": 2020}
      ]
    }
  ]
}
```

---

### Table: `sales_volume_history`

**Ingestion type:** `cdc`
**Cursor fields:** `year` (number), `month` (number)
**HTTP Method:** POST
**Endpoint:** `/api/sales/volume/history`

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Snapshot year |
| `month` | Yes | number | `12` | Snapshot month |
| `cadastral_typology` | No | string | `"A4"` | Cadastral class filter |
| `nation` | No | string | `"IT"` | Nation code; defaults to `IT` |

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "sales_qtraw": [          -- time-series of transaction volume (NTN)
        {
          "year":   number,
          "month":  number,
          "qtraw":  number      -- normalized transaction count (from AdE reprocessing)
        }
      ]
    }
  ]
}
```

**Primary key proposal:**
`(ty_zone, id_zone, contract, window, cadastral_typology, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/sales/volume/history' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "cadastral_typology": "A4",
    "nation": "IT"
  }'
```

**Verbatim response example:**

```json
{
  "_metadata": {
    "message": "",
    "query": {},
    "status": 200,
    "request_id": "4cc03232-7427-4f7e-ae64-762372e9e14b"
  },
  "items": [
    {
      "sales_qtraw": [
        {"month": 6,  "qtraw": 6046, "year": 2020},
        {"month": 9,  "qtraw": 6715, "year": 2020},
        {"month": 12, "qtraw": 9226, "year": 2020}
      ]
    }
  ]
}
```

---

### Table: `regions`

**Ingestion type:** `snapshot`
**HTTP Method:** GET
**Endpoint:** `/api/taxonomies/geo/IT/ty_zone/reg`

#### Request Parameters

No request body. Path parameters only: nation=`IT`, ty_zone=`reg`.

#### Response Schema

```
{
  "_metadata": {
    "message": string,
    "query":   object,
    "status":  number
  },
  "items": [
    {
      "id_zone":  string,   -- region identifier (use as id_zone in history calls with ty_zone="reg")
      "id_reg":   string,   -- same as id_zone for region-level records
      "nome":     string,   -- region name (e.g. "Lombardia")
      "nome_reg": string    -- same as nome for region-level records
    }
  ]
}
```

**Primary key proposal:** `id_zone`

**Pagination:** None documented. Full list returned in single response. Italy has 20 regions.

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/taxonomies/geo/IT/ty_zone/reg' \
  --header 'Authorization: Bearer <token>'
```

**Verbatim response example (abbreviated):**

```json
{
  "_metadata": {"message": "", "query": {}, "status": 200},
  "items": [
    {"id_zone": "1", "id_reg": "1", "nome": "Piemonte",       "nome_reg": "Piemonte"},
    {"id_zone": "2", "id_reg": "2", "nome": "Valle d'Aosta",  "nome_reg": "Valle d'Aosta"},
    {"id_zone": "3", "id_reg": "3", "nome": "Lombardia",      "nome_reg": "Lombardia"}
  ]
}
```

---

### Table: `provinces`

**Ingestion type:** `snapshot`
**HTTP Method:** GET
**Endpoint:** `/api/taxonomies/geo/IT/ty_zone/pro`

#### Response Schema

```
{
  "_metadata": {
    "message": string,
    "query":   object,
    "status":  number
  },
  "items": [
    {
      "id_zone":  string,   -- province identifier
      "id_reg":   string,   -- parent region identifier
      "nome":     string,   -- province name
      "nome_reg": string    -- parent region name
    }
  ]
}
```

**Primary key proposal:** `id_zone`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/taxonomies/geo/IT/ty_zone/pro' \
  --header 'Authorization: Bearer <token>'
```

---

### Table: `municipalities`

**Ingestion type:** `snapshot`
**HTTP Method:** GET
**Endpoint:** `/api/taxonomies/geo/IT/ty_zone/com`

#### Response Schema

Same structure as `provinces`:

```
{
  "_metadata": {
    "message": string,
    "query":   object,
    "status":  number
  },
  "items": [
    {
      "id_zone":  string,   -- municipality (comune) identifier
      "id_reg":   string,   -- parent region identifier
      "nome":     string,   -- municipality name
      "nome_reg": string    -- parent region name
    }
  ]
}
```

**Primary key proposal:** `id_zone`

**Note:** Italy has ~7,900 municipalities (comuni). The response may be large but is still returned in a single call (no pagination documented).

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/taxonomies/geo/IT/ty_zone/com' \
  --header 'Authorization: Bearer <token>'
```

---

### Table: `macro_zones`

**Ingestion type:** `snapshot`
**HTTP Method:** GET
**Endpoint:** `/api/taxonomies/geo/IT/ty_zone/macro`

#### Request Parameters

No request body. Path parameters only: nation=`IT`, ty_zone=`macro`. Same URL pattern as `regions`, `provinces`, and `municipalities` — only the trailing segment changes.

#### Response Schema

Identical to `regions` / `provinces` / `municipalities`. See the `### Table: regions` section for the shared schema.

```
{
  "_metadata": {
    "message": string,
    "query":   object,
    "status":  number
  },
  "items": [
    {
      "id_zone":  string,   -- macro-zone identifier (e.g. "28035_D040R")
      "id_reg":   string,   -- parent region identifier
      "nome":     string,   -- macro-zone name
      "nome_reg": string    -- parent region name
    }
  ]
}
```

**Primary key proposal:** `id_zone`

**Scale estimate:** Macro-zones are aggregated neighbourhood-level zones. Expected order of magnitude: ~thousands of records (substantially more than 110 provinces but fewer than ~7,900 municipalities). Exact count TBD empirically.

**Pagination:** None documented. Full list expected in a single response (as per the shared `ty_zone` list endpoint pattern).

**Note on `id_zone` format:** Macro-zone IDs use a compound format (e.g. `"28035_D040R"`) as observed in the `/api/taxonomies/geo/IT/hierarchy/lat/.../lng/...` response. This differs from the simple integer IDs used for regions/provinces/municipalities.

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/taxonomies/geo/IT/ty_zone/macro' \
  --header 'Authorization: Bearer <token>'
```

---

### Table: `micro_zones`

**Ingestion type:** `snapshot`
**HTTP Method:** GET
**Endpoint:** `/api/taxonomies/geo/IT/ty_zone/micro`

#### Request Parameters

No request body. Path parameters only: nation=`IT`, ty_zone=`micro`. Same URL pattern as all other geo-taxonomy list endpoints.

#### Response Schema

Identical to `regions` / `provinces` / `municipalities`. See the `### Table: regions` section for the shared schema.

```
{
  "_metadata": {
    "message": string,
    "query":   object,
    "status":  number
  },
  "items": [
    {
      "id_zone":  string,   -- micro-zone identifier (e.g. "28035_D040R1")
      "id_reg":   string,   -- parent region identifier
      "nome":     string,   -- micro-zone name
      "nome_reg": string    -- parent region name
    }
  ]
}
```

**Primary key proposal:** `id_zone`

**Scale estimate:** Micro-zones are the finest geographic granularity available. Expected order of magnitude: ~tens of thousands of records. This is the largest taxonomy list and may produce a sizeable single-response payload.

**Pagination:** None documented; the full list is expected in a single (potentially large) response. See Open Question #4 about pagination for large zone lists.

**Note on `id_zone` format:** Micro-zone IDs use a compound format extending the macro-zone ID (e.g. `"28035_D040R1"`) as observed in the hierarchy endpoint. Confirm this pattern holds universally before building zone-ID parsing logic.

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/taxonomies/geo/IT/ty_zone/micro' \
  --header 'Authorization: Bearer <token>'
```

---

### Table: `price`

**Ingestion type:** `snapshot`
**HTTP Method:** POST
**Endpoint:** `/api/price`

Point-in-time twin of `price_history`. Returns the **current** market values for a given `(ty_zone, id_zone, window, contract, year, month)` combination rather than a time-series. The response is structurally very different from `price_history`: instead of arrays of `(year, month, price_avg, price_avgin, price_avgout)` records, each metric is a single object with `value`, `delta` (change vs. prior window), and `ranking` (position among peers). The `price_typologies`, `rooms`, and `maintenance_status` breakdowns use a percentile distribution schema (`price_10pc`–`price_90pc`) not present in the history endpoint.

#### Fields present in `price` but NOT in `price_history`

| Field | Type | Notes |
|---|---|---|
| `discount` | object | Negotiation discount metric — no equivalent in history |
| `price_avg` | object (`value`, `delta`, `ranking`) | Absolute listing price avg — history only tracks price/sqm |
| `price_min` | object (`value`, `delta`, `ranking`) | Absolute listing price min — not in history |
| `price_max` | object (`value`, `delta`, `ranking`) | Absolute listing price max — not in history |
| `price_sqm_elasticity` | object (`value`, `delta`, `ranking`) | Elasticity metric — not in history |
| `price_sqm_variability` | object (`value`, `delta`, `ranking`) | Variability metric — not in history |
| `price_typologies` | object (keyed by typology ID) | Percentile breakdowns by property type — history has no typology breakdown |
| `maintenance_status[n].price_Xpc` | number | Percentile fields (`price_10pc`–`price_90pc`) — history uses `price_avg`, `price_avgin`, `price_avgout` instead |
| `rooms[n].price_Xpc` | number | Percentile fields per room count — not in history |
| `delta` / `ranking` sub-objects | object | All point-in-time metrics carry delta and ranking — history has none |

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type; see ty_zone taxonomy |
| `id_zone` | Yes | string | `"100005"` | Zone identifier from geo list endpoint |
| `window` | Yes | string | `"1M"` | Time window; allowed: `1M`, `3M`, `6M`, `12M` |
| `contract` | Yes | number | `1` | `1` = sale, `2` = rental |
| `year` | Yes | number | `2024` | Reference year for the snapshot period |
| `month` | Yes | number | `12` | Reference month for the snapshot period |
| `typology` | No | number | `4` | Property typology code; omit for aggregate |
| `nation` | No | string | `"IT"` | Defaults to `IT` |
| `success_if_empty` | No | boolean | `false` | When `true`, returns a 200 with empty `items` instead of `404`/`422` for zones with no data |

**Note on `year`/`month`:** Both are documented as **required** — the non-history endpoint does NOT default to "current". The caller must supply an explicit `(year, month)` target period (use `/api/taxonomies/temporal` to find the latest valid period).

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "discount": {
        "value":   number,   -- negotiation discount percentage
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_avg": {
        "value":   number,   -- average absolute listing price (€)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_min": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_max": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_avg": {
        "value":   number,   -- average price per sqm (€/m²)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_min": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_max": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_elasticity": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_variability": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "maintenance_status": {           -- object keyed by maintenance_status code ("1","2","3","4")
        "<status_id>": {
          "price_10pc": number,         -- 10th percentile price/sqm
          "price_20pc": number,
          "price_50pc": number,         -- median price/sqm
          "price_80pc": number,
          "price_90pc": number          -- 90th percentile price/sqm
        }
      },
      "price_typologies": {             -- object keyed by typology code (e.g. "4","5","7","10","11","12","13","31")
        "<typology_id>": {
          "price_10pc": number,
          "price_20pc": number,
          "price_50pc": number,
          "price_80pc": number,
          "price_90pc": number
        }
      },
      "rooms": {                        -- object keyed by room count ("1","2","3","4","5","m5")
        "<room_count>": {
          "price_10pc": number,
          "price_20pc": number,
          "price_50pc": number,
          "price_80pc": number,
          "price_90pc": number
        }
      }
    }
  ]
}
```

**Primary key proposal (for flattened rows):**

A single API call returns one snapshot record. When normalising to a flat table, the request parameters are the natural key:
`(ty_zone, id_zone, contract, window, typology, year, month)`

For the breakdowns (`maintenance_status`, `price_typologies`, `rooms`), add a `breakdown_type` and `breakdown_key` column analogously to the history flattening strategy.

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/price' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "typology": 4,
    "nation": "IT"
  }'
```

**Verbatim response example (abbreviated):**

```json
{
  "_metadata": {
    "message": "",
    "query": {},
    "status": 200,
    "request_id": "a1b2c3d4-0000-0000-0000-000000000002"
  },
  "items": [
    {
      "discount": {
        "value": 8.5,
        "delta": {"value": -0.3, "window": "12M"},
        "ranking": {"of": 20, "position": 5}
      },
      "price_sqm_avg": {
        "value": 1950.0,
        "delta": {"value": 2.1, "window": "12M"},
        "ranking": {"of": 20, "position": 8}
      },
      "maintenance_status": {
        "1": {"price_10pc": 1800, "price_20pc": 1950, "price_50pc": 2200, "price_80pc": 2500, "price_90pc": 2800},
        "2": {"price_10pc": 1600, "price_20pc": 1750, "price_50pc": 2000, "price_80pc": 2300, "price_90pc": 2600}
      },
      "rooms": {
        "2": {"price_10pc": 1700, "price_20pc": 1850, "price_50pc": 2100, "price_80pc": 2400, "price_90pc": 2700},
        "m5": {"price_10pc": 1500, "price_20pc": 1650, "price_50pc": 1900, "price_80pc": 2200, "price_90pc": 2500}
      },
      "price_typologies": {
        "4": {"price_10pc": 1600, "price_20pc": 1750, "price_50pc": 2000, "price_80pc": 2350, "price_90pc": 2650}
      }
    }
  ]
}
```

---

### Table: `ads`

**Ingestion type:** `snapshot`
**HTTP Method:** POST
**Endpoint:** `/api/ads`

Point-in-time twin of `ads_history`. Returns the **current** listings stock metrics for a given period rather than a time-series. The response schema is structurally distinct from `ads_history`: instead of arrays of `(year, month, price_avg, price_avgin, price_avgout)`, each metric is a single object with `value`, `delta`, and `ranking`. The `maintenance_status` and `rooms` breakdowns use a percentile schema (`price_10pc`–`price_90pc`).

**Clarification on `ads_history` field semantics (Open Question #2):** The docs describe `/api/ads` as "real estate listings stock infos" — the `price_avg`, `price_sqm_avg`, etc. fields in this endpoint confirm that both `ads` and `ads_history` track **asking prices of listed properties**, not raw ad counts. The `discount` field reinforces this: it measures negotiation discount on listed prices.

#### Fields present in `ads` but NOT in `ads_history`

| Field | Type | Notes |
|---|---|---|
| `discount` | object | Negotiation discount — not in history |
| `price_avg` | object (`value`, `delta`, `ranking`) | Absolute price avg — history tracks price/sqm only |
| `price_min` | object | Absolute price min — not in history |
| `price_max` | object | Absolute price max — not in history |
| `price_sqm_elasticity` | object | Elasticity metric — not in history |
| `price_sqm_variability` | object | Variability metric — not in history |
| `price_typologies` | object (keyed by typology ID) | Percentile breakdowns by property type — not in history |
| `maintenance_status[n].price_Xpc` | number | Percentile fields — history uses `price_avg`, `price_avgin`, `price_avgout` |
| `rooms[n].price_Xpc` | number | Percentile fields — history uses `price_avg`, `price_avgin`, `price_avgout` |
| `delta` / `ranking` sub-objects | object | Point-in-time metrics include delta and ranking — history has none |

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Reference year — required, no default |
| `month` | Yes | number | `12` | Reference month — required, no default |
| `typology` | No | number | `4` | Property typology code |
| `nation` | No | string | `"IT"` | Defaults to `IT` |
| `success_if_empty` | No | boolean | `false` | Return 200 with empty items instead of error for zones with no data |

**Note on `year`/`month`:** Both are **required** — same as `price`. No defaulting to current period.

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "discount": {
        "value":   number,   -- negotiation discount %
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_avg": {
        "value":   number,   -- average absolute listing price (€)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_min": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_max": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_avg": {
        "value":   number,   -- average price per sqm (€/m²)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_min": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_max": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_elasticity": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_sqm_variability": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "maintenance_status": {           -- object keyed by maintenance_status code ("1","2","3","4")
        "<status_id>": {
          "price_10pc": number,
          "price_20pc": number,
          "price_50pc": number,
          "price_80pc": number,
          "price_90pc": number
        }
      },
      "price_typologies": {             -- object keyed by typology code
        "<typology_id>": {
          "price_10pc": number,
          "price_20pc": number,
          "price_50pc": number,
          "price_80pc": number,
          "price_90pc": number
        }
      },
      "rooms": {                        -- object keyed by room count ("1","2","3","4","5","m5")
        "<room_count>": {
          "price_10pc": number,
          "price_20pc": number,
          "price_50pc": number,
          "price_80pc": number,
          "price_90pc": number
        }
      }
    }
  ]
}
```

**Primary key proposal:** `(ty_zone, id_zone, contract, window, typology, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/ads' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "typology": 4,
    "nation": "IT"
  }'
```

---

### Table: `search_data`

**Ingestion type:** `snapshot`
**HTTP Method:** POST
**Endpoint:** `/api/search-data`

Point-in-time twin of `search_data_history`. Returns the **current** demand/search behaviour metrics for a given period rather than a time-series. The response schema differs significantly from `search_data_history`: each scalar metric (`conversion_rate`, `price_sqm_search_avg`, etc.) is wrapped in a `{value, delta, ranking}` object rather than a bare time-series array. Several fields present here are absent from the history endpoint.

#### Fields present in `search_data` but NOT in `search_data_history`

| Field | Type | Notes |
|---|---|---|
| `contribution` | object (`value`, `delta`, `ranking`) | Zone contribution to overall market search share — not in history |
| `contribution_views` | object (`value`, `delta`, `ranking`) | Zone contribution to views — not in history |
| `leads_avg` | object (`value`, `delta`, `ranking`) | Average leads per listing — not in history |
| `maintenance_status` | array of `{status_id, qt_raw_perc}` | Distribution by condition (percentages) — history has time-series values per status |
| `min_surface_avg` | object (`value`, `delta`, `ranking`) | Average minimum surface filter in searches — not in history |
| `typologies` | array of `{typology_id, qt_raw_perc}` | Search distribution by property type — not in history |
| `res.pc_garage` | number | Garage filter percentage — history `res` does not include `pc_garage` |
| `delta` / `ranking` sub-objects | object | Point-in-time metrics carry delta and ranking — history arrays have no ranking |

Note: `pc_rooms` and `res` exist in both endpoints but with different structures — in history they contain raw counts and time-series; here they are percentage/distribution snapshots. The `conversion_rate`, `price_sqm_search_avg`, and `rooms` keys exist in both but carry different schemas (scalar+delta+ranking here vs. array of `{year, month, value}` in history).

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Reference year — required, no default |
| `month` | Yes | number | `12` | Reference month — required, no default |
| `typology` | No | number | `4` | Property typology code |
| `nation` | No | string | `"IT"` | Defaults to `IT` |
| `success_if_empty` | No | boolean | `false` | Return 200 with empty items instead of error |

**Note on `year`/`month`:** Both are **required** — confirmed from docs.

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "contribution": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number, "value": array }
      },
      "contribution_views": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number, "value": array }
      },
      "conversion_rate": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "leads_avg": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "maintenance_status": [             -- array (not keyed object as in history)
        {
          "status_id":    string,         -- maintenance condition code ("1"–"4")
          "qt_raw_perc":  number          -- percentage of searches for this condition
        }
      ],
      "min_surface_avg": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "pc_rooms": {                       -- object keyed by room count ("1"–"5","m5")
        "<room_count>": {
          "pc_raw": number,               -- percentage share of searches for this room count
          "qt_raw": number                -- raw quantity of searches for this room count
        }
      },
      "price_sqm_search_avg": {
        "value":   number,
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "res": {                            -- search attribute percentages (point-in-time snapshot)
        "pc_1floor":    number,
        "pc_1typology": number,
        "pc_garage":    number,           -- present here; NOT in search_data_history res
        "pc_garden":    number,
        "pc_minrooms":  number,
        "pc_status":    number,
        "pc_terrace":   number
      },
      "typologies": [                     -- array (not present in history)
        {
          "typology_id":  string,         -- property typology code
          "qt_raw_perc":  number          -- percentage of searches for this typology
        }
      ]
    }
  ]
}
```

**Note on `res` field naming:** The history endpoint uses hyphenated keys (`pc-garage`, `pc-garden`, etc.) while the point-in-time endpoint appears to use underscore keys (`pc_garage`, `pc_garden`). Implementers should confirm empirically — the history response example in the existing doc shows hyphenated versions (`"pc-garden": 15.51`).

**Primary key proposal:** `(ty_zone, id_zone, contract, window, typology, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/search-data' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "typology": 4,
    "nation": "IT"
  }'
```

---

### Table: `sales_price`

**Ingestion type:** `snapshot`
**HTTP Method:** POST
**Endpoint:** `/api/sales/price`

Point-in-time twin of `sales_price_history`. Returns current-period sale price metrics including percentile breakdowns and a `price_cadastral_typologies` array, none of which appear in the history endpoint.

#### Fields present in `sales_price` but NOT in `sales_price_history`

| Field | Type | Notes |
|---|---|---|
| `compravendite_price_sqm_max` | object (`value`, `delta`, `ranking`) | Max price/sqm — history has no max field |
| `compravendite_price_sqm_min` | object (`value`, `delta`, `ranking`) | Min price/sqm — history has no min field |
| `compravendite_sales_price_avg` | object (`value`, `delta`, `ranking`) | Average total transaction price (€) — history has no absolute price field |
| `price_cadastral_typologies` | array | Per-cadastral-class percentile distribution — not in history |
| `delta` / `ranking` sub-objects | object | Point-in-time metrics carry delta and ranking — history time-series records have neither |

`compravendite_price_sqm_avg` exists in both, but in the history endpoint it is an array of `{year, month, price_avg}` records; here it is a scalar `{value, delta, ranking}` object.

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Reference year — required, no default |
| `month` | Yes | number | `12` | Reference month — required, no default |
| `cadastral_typology` | No | string | `"A4"` | Cadastral class filter (A1–A11); omit for aggregate |
| `nation` | No | string | `"IT"` | Defaults to `IT` |
| `success_if_empty` | No | boolean | `false` | Return 200 with empty items instead of error |

**Note on `year`/`month`:** Both are **required** — confirmed from docs.

**cURL example discrepancy:** The verbatim cURL example in the official docs passes `"typology": 4` in the body. However, the request parameters table specifies `cadastral_typology` as the optional filter, not `typology`. This appears to be a copy-paste error in the docs — use `cadastral_typology` per the parameters table.

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "compravendite_price_sqm_avg": {
        "value":   number,   -- average sale price per sqm (€/m²)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "compravendite_price_sqm_max": {
        "value":   number,   -- maximum sale price per sqm (€/m²)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "compravendite_price_sqm_min": {
        "value":   number,   -- minimum sale price per sqm (€/m²)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "compravendite_sales_price_avg": {
        "value":   number,   -- average total transaction price (€) from AdE data
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "price_cadastral_typologies": [   -- per-cadastral-class percentile distribution
        {
          "cadastral_typology": string, -- e.g. "A1", "A2", ..., "A11"
          "price_10pc":         number, -- 10th percentile sale price/sqm
          "price_20pc":         number,
          "price_50pc":         number, -- median sale price/sqm
          "price_80pc":         number,
          "price_90pc":         number  -- 90th percentile sale price/sqm
        }
      ]
    }
  ]
}
```

**Primary key proposal:** `(ty_zone, id_zone, contract, window, cadastral_typology, year, month)`

For the `price_cadastral_typologies` array rows, add `cadastral_typology` from the array element as an additional key dimension.

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/sales/price' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "cadastral_typology": "A4",
    "nation": "IT"
  }'
```

**Verbatim response example (from official docs):**

```json
{
  "_metadata": {
    "message": "",
    "query": {},
    "status": 200,
    "request_id": "017d6a53-9346-43a8-981e-d8c361202b51"
  },
  "items": [
    {
      "compravendite_price_sqm_avg": {
        "delta":   {"value": -5.82,  "window": "12M"},
        "ranking": {"of": 19,        "position": 12},
        "value":   1270
      },
      "compravendite_price_sqm_max": {
        "delta":   {"value": -18.75, "window": "12M"},
        "ranking": {"of": 19,        "position": 10},
        "value":   3623
      },
      "compravendite_price_sqm_min": {
        "delta":   {"value": -43.26, "window": "12M"},
        "ranking": {"of": 19,        "position": 17},
        "value":   48
      },
      "compravendite_sales_price_avg": {
        "delta":   {"value": -1.02,  "window": "12M"},
        "ranking": {"of": 19,        "position": 11},
        "value":   130526
      },
      "price_cadastral_typologies": [
        {
          "cadastral_typology": "A1",
          "price_10pc": 86,
          "price_20pc": 919,
          "price_50pc": 2054,
          "price_80pc": 2426,
          "price_90pc": 3162
        }
      ]
    }
  ]
}
```

---

### Table: `sales_volume`

**Ingestion type:** `snapshot`
**HTTP Method:** POST
**Endpoint:** `/api/sales/volume`

Point-in-time twin of `sales_volume_history`. Returns current-period transaction volume metrics including `sales` (total revenue), `sales_surface_avg`, `cadastral_typologies` distribution, and `sales_surface_class` distribution — none of which are present in the history endpoint.

#### Fields present in `sales_volume` but NOT in `sales_volume_history`

| Field | Type | Notes |
|---|---|---|
| `sales` | object (`value`, `delta`, `ranking`) | Total transaction revenue — not in history |
| `sales_surface_avg` | object (`value`, `delta`, `ranking`) | Average surface of transacted properties — not in history |
| `cadastral_typologies` | array of `{typology_id, qt_raw_perc}` | Transaction distribution by cadastral class — not in history |
| `sales_surface_class` | array of `{id, qt_raw_perc}` | Transaction distribution by surface class — not in history |
| `delta` / `ranking` sub-objects | object | Point-in-time `sales_qtraw` carries delta/ranking — history `sales_qtraw` is a bare `{year, month, qtraw}` record |

`sales_qtraw` exists in both, but the schemas differ: history uses `{year, month, qtraw: number}` per time-series record; here it is `{value, delta, ranking}`.

#### Request Body Parameters

| Parameter | Required | Type | Example | Notes |
|---|---|---|---|---|
| `ty_zone` | Yes | string | `"com"` | Zone type |
| `id_zone` | Yes | string | `"100005"` | Zone identifier |
| `window` | Yes | string | `"1M"` | Time window |
| `contract` | Yes | number | `1` | Contract type |
| `year` | Yes | number | `2024` | Reference year — required, no default |
| `month` | Yes | number | `12` | Reference month — required, no default |
| `cadastral_typology` | No | string | `"A4"` | Cadastral class filter |
| `nation` | No | string | `"IT"` | Defaults to `IT` |
| `success_if_empty` | No | boolean | `false` | Return 200 with empty items instead of error |

**Note on `year`/`month`:** Both are **required** — confirmed from docs.

**Note on `contract` parameter:** The docs cURL example passes `"typology": 4` (same copy-paste issue as `sales_price`). The request parameters table shows `cadastral_typology` as the valid optional filter. Use `cadastral_typology` per the parameters table.

#### Response Schema

```
{
  "_metadata": {
    "message":    string,
    "query":      object,
    "status":     number,
    "request_id": string  -- UUID
  },
  "items": [
    {
      "sales": {
        "value":   number,   -- total transaction revenue (€)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "sales_qtraw": {
        "value":   number,   -- normalized transaction count (NTN from AdE)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "sales_surface_avg": {
        "value":   number,   -- average surface of transacted properties (m²)
        "delta":   { "value": number, "window": string },
        "ranking": { "of": number, "position": number }
      },
      "cadastral_typologies": [      -- transaction distribution by cadastral class
        {
          "typology_id":  string,    -- cadastral class code (e.g. "A3")
          "qt_raw_perc":  number     -- percentage of transactions for this class
        }
      ],
      "sales_surface_class": [       -- transaction distribution by surface class
        {
          "id":           string,    -- surface class code; see class_surface taxonomy
          "qt_raw_perc":  number     -- percentage of transactions in this surface bracket
        }
      ]
    }
  ]
}
```

**Primary key proposal:** `(ty_zone, id_zone, contract, window, cadastral_typology, year, month)`

**Verbatim cURL example:**

```bash
curl --location 'https://ws-osservatorio.realitycs.it/api/sales/volume' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer <token>' \
  --data '{
    "ty_zone": "com",
    "id_zone": "100005",
    "window": "1M",
    "contract": 1,
    "year": 2024,
    "month": 12,
    "cadastral_typology": "A4",
    "nation": "IT"
  }'
```

**Verbatim response example (from official docs):**

```json
{
  "_metadata": {
    "message": "",
    "query": {},
    "status": 200,
    "request_id": "4cc03232-7427-4f7e-ae64-762372e9e14b"
  },
  "items": [
    {
      "sales": {
        "value": 1248273623.74,
        "delta": {"value": -5.12, "window": "3M"},
        "ranking": {"position": 1, "of": 8}
      },
      "sales_qtraw": {
        "value": 10004.21,
        "delta": {"value": 38.27, "window": "3M"},
        "ranking": {"position": 1, "of": 8}
      },
      "sales_surface_avg": {
        "value": 96.51,
        "delta": {"value": -1.39, "window": "3M"},
        "ranking": {"position": 8, "of": 8}
      },
      "cadastral_typologies": [
        {"typology_id": "A3", "qt_raw_perc": 50.0}
      ],
      "sales_surface_class": [
        {"id": "2", "qt_raw_perc": 40.0}
      ]
    }
  ]
}
```

---

## Get Object Primary Keys

| Table | Primary Key Column(s) | Notes |
|---|---|---|
| `price_history` (flattened) | `(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)` | `series_type` = `price_sqm_avg` / `maintenance_status` / `rooms`; `series_key` = `_` / `"1"–"4"` / `"1"–"m5"` |
| `ads_history` (flattened) | `(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)` | Same structure as price_history |
| `search_data_history` (flattened) | `(ty_zone, id_zone, contract, window, typology, series_type, series_key, year, month)` | `series_key` varies by sub-series (`conversion_rate`, `maintenance_status`, `pc_rooms`, `price_sqm_search_avg`, `res`, `rooms`) |
| `sales_price_history` (flattened) | `(ty_zone, id_zone, contract, window, cadastral_typology, year, month)` | Single series `compravendite_price_sqm_avg` |
| `sales_volume_history` (flattened) | `(ty_zone, id_zone, contract, window, cadastral_typology, year, month)` | Single series `sales_qtraw` |
| `regions` | `id_zone` | |
| `provinces` | `id_zone` | |
| `municipalities` | `id_zone` | |

---

## Object Ingestion Types

| Table | Ingestion Type | Rationale |
|---|---|---|
| `price_history` | `cdc` | History endpoint returns all records up to `(year, month)`; advance cursor to ingest new months |
| `ads_history` | `cdc` | Same pattern |
| `search_data_history` | `cdc` | Same pattern |
| `sales_price_history` | `cdc` | Same pattern |
| `sales_volume_history` | `cdc` | Same pattern |
| `regions` | `snapshot` | Static reference data; no incremental endpoint; re-snapshot periodically |
| `provinces` | `snapshot` | Same |
| `municipalities` | `snapshot` | Same |

No delete semantics are documented for any endpoint. There is no tombstone or delete feed — treat all history tables as upsert-only `cdc`.

---

## Read API for Data Retrieval

### Incremental Strategy for History Endpoints

The history endpoints return all data up to the given `(year, month)`. For incremental ingestion:

1. **Initial load:** Call each history endpoint with the latest available `(year, month)` (from `/api/taxonomies/temporal`) to get the full history.
2. **Incremental runs:** On each subsequent sync, call with the new latest `(year, month)` and upsert all returned records (records for periods already in the warehouse will overwrite, new months will be inserted).
3. **Cursor state:** Store `last_synced_year` + `last_synced_month` per `(ty_zone, id_zone, contract, window, typology)` combination.

### Fan-out Requirement

Every history endpoint call is scoped to a single `(ty_zone, id_zone, contract, window, typology)` combination. A full sync requires:

- **Zone enumeration:** Call `GET /api/taxonomies/geo/IT/ty_zone/{level}` once to get all zone IDs.
- **Combinations:** For each zone × each contract (1, 2) × each window × optionally each typology.
- **Scale estimate (municipalities + 1M window + sale contract, no typology filter):** ~7,900 calls per sync cycle.

The connector should expose `zone_filter`, `contract_filter`, `window_filter`, and `typology_filter` configuration parameters to bound this fan-out.

### Temporal Taxonomy Lookup

Before syncing, call `GET /api/taxonomies/temporal` to determine the latest valid `(year, month)` for each window size and data type. This avoids requesting periods with no data.

### Snapshot Strategy for Taxonomy Tables

`regions`, `provinces`, and `municipalities` should be re-snapshotted infrequently (e.g. weekly or on-demand). There is no change-detection mechanism; the connector must diff against the stored snapshot.

### Success-If-Empty Pattern

For non-history (point-in-time) endpoints like `/api/price` and `/api/ads`, an optional `success_if_empty: true` body parameter avoids `404`/`422` errors for zones with no data. The history endpoints do not document this parameter — validate empirically.

---

## Error Codes

| HTTP Status | Code | Description |
|---|---|---|
| `400` | `bad_request` | Generic bad request |
| `400` | `invalid_client` | Invalid client ID or secret key |
| `400` | `invalid_request` | Missing or malformed parameters |
| `400` | `invalid_grant` | Wrong username or password |
| `400` | `unauthorized_request` | Unauthorized request |
| `401` | `invalid_token` | Missing, invalid, or expired Bearer token |
| `403` | `forbidden` | Access denied (no subscription for this endpoint) |
| `404` | `not_found` | Resource not found |
| `422` | `input_validation_error` | Request payload failed validation |
| `500` | `internal_error` | Internal server error |

**Error response format:** Not documented. TBD — inspect actual responses.

**Rate limiting:** Not documented. No `429` behavior specified. The authentication docs warn that excessive re-authentication "can be flagged by our protection systems and may lead to a temporary ban." Assume rate limiting exists; implement exponential backoff on `429` and `500`.

**Retry recommendations (inferred, not documented):**
- `401 invalid_token` → re-authenticate and retry once
- `429` → exponential backoff with jitter
- `500` → retry up to 3 times with exponential backoff
- `400`, `403`, `404`, `422` → non-retryable; log and skip

---

## Field Type Mapping

| API Type | Spark/Python Type | Notes |
|---|---|---|
| `string` (zone IDs, names) | `StringType` | IDs are numeric strings (e.g. `"100005"`) — keep as string |
| `number` (year, month) | `IntegerType` | Year: 4-digit; Month: 1–12 |
| `number` (price, value, qtraw) | `DoubleType` | Decimal values for prices/ratios |
| `string` (UUID request_id) | `StringType` | UUIDs |
| `object` (nested) | `StructType` | Expand to nested struct or flatten to separate columns |
| `array` (time-series) | `ArrayType(StructType)` | Explode to rows for flat table |
| `object` (keyed by code) | `MapType` or explode | Keys are string integers ("1","2","3","4"); explode to rows with `series_key` column |

**Special behaviors:**
- `price_avgin` / `price_avgout` — "internal" and "external" price variants; semantics are TBD (may refer to inside/outside the zone or internal/external listing price)
- `qt-raw` / `pc-raw` — raw quantity and raw percentage; hyphen in field name requires quoting in SQL
- `rooms` dict key `"m5"` — represents 5+ rooms (not a numeric key)

---

## Sources and References

| Source Type | URL | Confidence | What it confirmed |
|---|---|---|---|
| Official Docs | https://insights.immobiliare.it/webdocs/getting-started/ | High | Platform overview, service list |
| Official Docs | https://insights.immobiliare.it/webdocs/authentication/ | High | OAuth2 password grant, token TTL (14399s), re-auth warning |
| Official Docs | https://insights.immobiliare.it/webdocs/errors/ | High | Error code table |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/getting-started/ | High | Production and sandbox base URLs |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies/ | High | Taxonomy overview |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies-general/ | High | Contract codes, typology codes, surface classes |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies-temporal/ | High | Window values, data availability years |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies-geo-hierarchy/ | High | Geo hierarchy lat/lng endpoint, response structure |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/zones-ty-zone/ | High | Zone list endpoint, response schema |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/zones-sublevel/ | High | Sub-zone endpoint URL pattern, response fields |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/price-history/ | High | price_history endpoint, request params, nested response arrays |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/ads-history/ | High | ads_history endpoint, request params, response schema |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/search-data-history/ | High | search_data_history endpoint, full response with verbatim example |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/sales-price-history/ | High | sales_price_history endpoint, path difference (/api/sales/price/history), response schema |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/sales-volume-history/ | High | sales_volume_history endpoint, verbatim response example with qtraw |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/changelog/ | High | Changelog — single entry 2026-04-15, no breaking changes noted |

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://insights.immobiliare.it/webdocs/getting-started/ | 2026-04-28 | High | Platform overview |
| Official Docs | https://insights.immobiliare.it/webdocs/authentication/ | 2026-04-28 | High | Full auth flow, token TTL |
| Official Docs | https://insights.immobiliare.it/webdocs/errors/ | 2026-04-28 | High | Error codes |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/getting-started/ | 2026-04-28 | High | Production/sandbox URLs |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies/ | 2026-04-28 | High | Taxonomy types |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies-general/ | 2026-04-28 | High | Contract, typology, surface enumerations |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies-temporal/ | 2026-04-28 | High | Temporal windows, data availability |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/taxonomies-geo-hierarchy/ | 2026-04-28 | High | Lat/lng hierarchy endpoint |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/zones-ty-zone/ | 2026-04-28 | High | Zone list endpoint |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/zones-sublevel/ | 2026-04-28 | High | Sub-zone endpoint |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/price-history/ | 2026-04-28 | High | price_history schema |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/ads-history/ | 2026-04-28 | High | ads_history schema |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/search-data-history/ | 2026-04-28 | High | search_data_history schema, verbatim response |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/sales-price-history/ | 2026-04-28 | High | sales_price_history schema |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/sales-volume-history/ | 2026-04-28 | High | sales_volume_history schema, verbatim response |
| Official Docs | https://insights.immobiliare.it/webdocs/service/ws-osservatorio-public-api/changelog/ | 2026-04-28 | High | Changelog |

---

## Open Questions / Unknowns

1. **Refresh token endpoint:** The `/oauth/token` response includes a `refresh_token` field but no exchange endpoint, request format, or TTL for the refresh token is documented. It is unclear if a refresh grant flow is supported or if the returned `refresh_token` is vestigial. **Implication:** until clarified, implement simple password re-grant on `401`.

2. **`ads_history` field semantics:** The `ads/history` endpoint response uses the same field names as `price/history` (`price_avg`, `price_avgin`, `price_avgout`). The description says "historical series of listings stock" — unclear whether these fields represent listing counts (number of ads) or their asking prices. The non-history `/api/ads` endpoint returns `discount`, `maintenance_status` with price percentiles, etc. — suggesting the history endpoint may also track prices rather than pure ad counts.

3. **`price_avgin` / `price_avgout` semantics:** Not explained in the docs. May mean inside/outside the zone boundary, or internal/external property pricing conventions. Needs clarification.

4. **Pagination for large zone lists:** `GET /api/taxonomies/geo/IT/ty_zone/micro` could return tens of thousands of micro-zones. No pagination is documented. Confirm empirically whether all micro-zones are returned in a single (potentially large) response.

5. **`success_if_empty` on history endpoints:** Documented only for `/api/price`, `/api/ads`, `/api/search-data`, `/api/sales/price`, and `/api/sales/volume` (non-history). Whether it applies to `*/history` variants is undocumented. Needs empirical verification.

6. **Room count key `"m5"` vs `"5"`:** The taxonomy docs mention room codes `"1"` through `"5"` and `"m5"` (5+). Whether `"m5"` or `"5"` is the actual dict key in history responses is undocumented. Needs verification.

7. **Exact ty_zone values supported by history endpoints:** The docs confirm `reg`, `pro`, `com`, `macro`, `micro` for taxonomy/geo endpoints. Whether all five levels are valid inputs for the `ty_zone` field in history (price, ads, search-data, sales) endpoints is not stated explicitly — needs validation.

8. **Rate limits:** No rate limit or throttle policy is documented anywhere. The auth docs warn against excessive re-auth. Actual request-per-second / request-per-day limits are unknown.

9. **Error response body format:** The error codes table is documented but the JSON body structure of error responses is not. Unknown whether errors follow `{"error": "code", "error_description": "..."}` or a different schema.

10. **`nation` parameter for non-IT data:** The `nation` parameter accepts `IT` (confirmed). Whether other nation codes are accepted and what zones/data exist for them is undocumented.

11. **`window` interaction with `(year, month)` cursor:** Whether calling with `window=1M` and `window=3M` for the same `(year, month)` returns different history depths or the same history with different aggregation granularity is not explained.

12. **`id_zone` format for macro/micro zones:** Regions use simple integer IDs (e.g., `"1"`, `"5"`), provinces use 5-digit codes, and macro/micro zones appear to use compound codes (e.g., `"28035_D040R"`, `"28035_D040R1"`). Confirm this pattern holds universally.

13. **Sales endpoints and `contract` parameter:** The `/api/sales/price` and `/api/sales/volume` docs show `contract: 1` in examples but sales (compravendite) data from AdE is inherently purchase-only. Whether `contract: 2` (rental) is valid for sales endpoints is unclear — likely always `1`.

---

## Connector Design Implications

1. **Fan-out is the central design challenge.** Every history endpoint is zone × contract × window × typology-scoped. At municipality level (`~7,900 zones`) × 2 contracts × 1 window × 1 typology = ~15,800 API calls per sync. Expose `zone_filter` (list of `id_zone`), `contract_filter` (list of contract codes), `window_filter`, and `typology_filter` in `table_configuration` to allow users to scope syncs.

2. **Zone enumeration as a prerequisite step.** Before any history sync, the connector must call the zone-list endpoint(s) to obtain valid `(ty_zone, id_zone)` pairs. Cache this list between syncs (zones are stable reference data). Reuse the `regions`/`provinces`/`municipalities` snapshot tables as the zone manifest.

3. **Temporal taxonomy as sync horizon.** Before each sync cycle, call `GET /api/taxonomies/temporal` to determine the latest valid `(year, month)` for each window. This prevents wasted calls for periods not yet available.

4. **Incremental cursor per zone-combination.** The connector must maintain cursor state per `(ty_zone, id_zone, contract, window, typology)` tuple. Since each call returns the full history to the requested date, the simplest strategy is: request the latest available `(year, month)`, upsert all returned rows, advance cursor. The `(year, month)` within each time-series record is the de-facto incremental cursor.

5. **Schema flattening design.** The nested dict-of-arrays response structure (e.g. `maintenance_status` keyed by `"1"–"4"`, `rooms` keyed by `"1"–"m5"`) must be exploded into flat rows. The recommended approach is to add synthetic columns: `series_type` (which top-level key) and `series_key` (the dict key or `"_"` for arrays). The composite primary key then incorporates these.

6. **`sales_price_history` and `sales_volume_history` use cadastral typology, not property typology.** These two endpoints accept `cadastral_typology` (string A-codes: `A1`–`A11`) instead of `typology` (integer codes). Keep them as separate table types with different filter parameters.

7. **Token caching is mandatory.** Implement a token manager that stores the token in memory (or a connector-local cache) with a TTL buffer of 5 minutes before the 14,400-second expiry. Do not authenticate per-request.

8. **`price_avgin` / `price_avgout` columns should be preserved.** Even though their semantics are unclear (see Open Questions), both fields appear consistently across all history schemas — include them as nullable `DoubleType` columns rather than dropping.

9. **Parallelism.** The fan-out over thousands of zone × contract combinations is embarrassingly parallel. The connector should support concurrent HTTP calls (e.g. via `asyncio` or a thread pool) with configurable concurrency limits to respect any undocumented rate limits.

10. **The `municipalities` snapshot is large.** With ~7,900 rows and potentially large geo list responses, the municipalities table should be streamed/batched if the API ever paginates. Currently no pagination is documented, but handle large responses gracefully (avoid loading the full response into memory before processing).

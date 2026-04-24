# Databricks notebook source
# MAGIC %md
# MAGIC # dati.gov.it — enrich `organizations` / `groups` / `tags` from `package_search`
# MAGIC
# MAGIC **Problem.** dati.gov.it's WAF blocks `/organization_show`,
# MAGIC `/group_show`, and `/tag_show` from Databricks serverless pipeline
# MAGIC egress IPs (HTTP 403). The connector therefore falls back to a
# MAGIC minimal row per slug — most columns are `NULL` in
# MAGIC `organizations` / `groups` / `tags`.
# MAGIC
# MAGIC **Fix.** `/package_search` **is** allowed, and every package embeds
# MAGIC the full nested `organization` + `groups` array + `tags` array.
# MAGIC This notebook scans `package_search`, dedupes by `id`, and writes
# MAGIC enriched tables alongside the pipeline's snapshots.
# MAGIC
# MAGIC Output tables (`_enriched` suffix keeps them separate from the
# MAGIC pipeline-managed destinations, which DLT would overwrite on the
# MAGIC next run):
# MAGIC
# MAGIC - `users.federico_rizzo.organizations_enriched`
# MAGIC - `users.federico_rizzo.groups_enriched`
# MAGIC - `users.federico_rizzo.tags_enriched`
# MAGIC
# MAGIC Run this once after the pipeline completes, or on a schedule.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "users"
SCHEMA = "federico_rizzo"
BASE_URL = "https://www.dati.gov.it/opendata/api/3/action"
ROWS = 1000          # page size (CKAN hard cap)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CKAN client (urllib, Mozilla-ish UA)

# COMMAND ----------

import urllib.request
import urllib.parse
import urllib.error
import json
import time

UA = (
    "Mozilla/5.0 (compatible; dati-gov-it-enrich/0.1; "
    "+https://github.com/databrickslabs/lakeflow-community-connectors)"
)


def ckan_get(action: str, params: dict) -> dict:
    query = urllib.parse.urlencode(params, doseq=False)
    url = f"{BASE_URL}/{action}?{query}"
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("Accept", "application/json")
            req.add_header("User-Agent", UA)
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            if not body.get("success"):
                raise RuntimeError(f"CKAN {action} error: {body.get('error')}")
            return body["result"]
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < 4:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError(f"Exhausted retries for {url}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Page through `package_search` and dedupe by id

# COMMAND ----------

orgs_by_id: dict[str, dict] = {}
groups_by_id: dict[str, dict] = {}
tags_by_id: dict[str, dict] = {}

start = 0
total = None
while True:
    page = ckan_get("package_search", {"q": "*:*", "rows": ROWS, "start": start})
    results = page.get("results") or []
    if not results:
        break

    for pkg in results:
        org = pkg.get("organization")
        if isinstance(org, dict) and org.get("id"):
            orgs_by_id.setdefault(org["id"], org)

        for g in (pkg.get("groups") or []):
            if isinstance(g, dict) and g.get("id"):
                groups_by_id.setdefault(g["id"], g)

        for t in (pkg.get("tags") or []):
            if isinstance(t, dict) and t.get("id"):
                tags_by_id.setdefault(t["id"], t)

    start += len(results)
    total = page.get("count", total or 0)
    print(
        f"scanned={start}/{total}  "
        f"orgs={len(orgs_by_id)}  groups={len(groups_by_id)}  tags={len(tags_by_id)}"
    )
    if len(results) < ROWS or start >= (total or 0):
        break

print(
    f"\nDONE: {len(orgs_by_id)} organizations, "
    f"{len(groups_by_id)} groups, {len(tags_by_id)} tags"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write enriched tables

# COMMAND ----------

from pyspark.sql import functions as F


def project(rows: list[dict], cols: list[str]) -> list[dict]:
    return [{c: r.get(c) for c in cols} for r in rows]


org_cols = [
    "id", "name", "title", "description", "image_url",
    "created", "is_organization", "approval_status", "state", "type",
    "num_followers", "package_count",
]

group_cols = [
    "id", "name", "title", "description", "image_display_url",
    "is_organization", "state", "type", "package_count",
]

tag_cols = ["id", "name", "display_name", "vocabulary_id"]

orgs_df = spark.createDataFrame(project(list(orgs_by_id.values()), org_cols))
groups_df = spark.createDataFrame(project(list(groups_by_id.values()), group_cols))
tags_df = spark.createDataFrame(project(list(tags_by_id.values()), tag_cols))

# ISO 8601 → timestamp for the orgs.created column.
orgs_df = orgs_df.withColumn("created", F.to_timestamp("created"))

orgs_target = f"{CATALOG}.{SCHEMA}.organizations_enriched"
groups_target = f"{CATALOG}.{SCHEMA}.groups_enriched"
tags_target = f"{CATALOG}.{SCHEMA}.tags_enriched"

orgs_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(orgs_target)
print(f"  wrote {orgs_df.count():>6} rows → {orgs_target}")

groups_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(groups_target)
print(f"  wrote {groups_df.count():>6} rows → {groups_target}")

tags_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tags_target)
print(f"  wrote {tags_df.count():>6} rows → {tags_target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: create views that fill null columns on the pipeline tables

# COMMAND ----------

# These views LEFT JOIN the enriched tables onto the pipeline snapshots,
# using COALESCE so any column that is null in the snapshot falls back to
# the enriched value. Recreate these once — the pipeline only updates the
# snapshot rows, not these views.

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.organizations_full AS
SELECT
  s.id,
  COALESCE(s.name, e.name) AS name,
  COALESCE(e.title, s.title) AS title,
  COALESCE(e.description, s.description) AS description,
  COALESCE(e.image_url, s.image_url) AS image_url,
  COALESCE(e.created, s.created) AS created,
  COALESCE(e.is_organization, s.is_organization) AS is_organization,
  COALESCE(e.approval_status, s.approval_status) AS approval_status,
  COALESCE(e.state, s.state) AS state,
  COALESCE(e.type, s.type) AS type,
  COALESCE(e.num_followers, s.num_followers) AS num_followers,
  COALESCE(e.package_count, s.package_count) AS package_count
FROM {CATALOG}.{SCHEMA}.organizations s
LEFT JOIN {CATALOG}.{SCHEMA}.organizations_enriched e USING (id)
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.groups_full AS
SELECT
  s.id,
  COALESCE(s.name, e.name) AS name,
  COALESCE(e.title, s.title) AS title,
  COALESCE(e.description, s.description) AS description,
  COALESCE(e.image_display_url, s.image_display_url) AS image_display_url,
  COALESCE(e.is_organization, s.is_organization) AS is_organization,
  COALESCE(e.state, s.state) AS state,
  COALESCE(e.type, s.type) AS type,
  COALESCE(e.package_count, s.package_count) AS package_count
FROM {CATALOG}.{SCHEMA}.groups s
LEFT JOIN {CATALOG}.{SCHEMA}.groups_enriched e USING (id)
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.tags_full AS
SELECT
  COALESCE(e.id, s.id) AS id,
  COALESCE(s.name, e.name) AS name,
  COALESCE(e.display_name, s.display_name) AS display_name,
  COALESCE(e.vocabulary_id, s.vocabulary_id) AS vocabulary_id
FROM {CATALOG}.{SCHEMA}.tags s
LEFT JOIN {CATALOG}.{SCHEMA}.tags_enriched e ON s.name = e.name
""")

print("views created:")
print(f"  {CATALOG}.{SCHEMA}.organizations_full")
print(f"  {CATALOG}.{SCHEMA}.groups_full")
print(f"  {CATALOG}.{SCHEMA}.tags_full")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity check

# COMMAND ----------

display(spark.table(orgs_target).orderBy(F.desc("package_count")).limit(10))

# COMMAND ----------

display(spark.table(groups_target).orderBy(F.desc("package_count")).limit(20))

# COMMAND ----------

display(spark.table(tags_target).limit(10))

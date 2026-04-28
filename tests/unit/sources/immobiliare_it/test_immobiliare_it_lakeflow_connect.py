"""Live-API test suite for the immobiliare.it Insights (Market Explorer) connector.

Runs the shared ``LakeflowConnectTests`` and ``SupportsPartitionedStreamTests``
harness against the Market Explorer service, plus a handful of connector-
specific assertions.

Sizing notes
------------

The Market Explorer API fans out per (zone × contract × typology × month).
A full backfill at the municipality level (~7,900 zones) over even one year
of monthly data with all typologies would issue tens of thousands of POSTs.
To keep the suite well under the 60s pytest budget we:

* scope every history table to a single ``zone_filter`` (one comune for
  listing-side tables, one province for sales tables),
* set ``start_year_month`` to ``202401`` so the cursor range covers about a
  year of history,
* fix ``contract`` to a single value (sale only) and ``typology`` /
  ``cadastral_typology`` to a single code per table.

Snapshot tables (``regions`` / ``provinces`` / ``municipalities``) hit a
single GET each and don't fan out — no per-table tuning required.

If the run ever blows past 60s, narrow ``start_year_month`` further (move
it closer to the latest available month) rather than bumping the timeout.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.immobiliare_it.immobiliare_it import (
    ImmobiliareItLakeflowConnect,
)
from databricks.labs.community_connector.sources.immobiliare_it.immobiliare_it_schemas import (
    CDC_TABLES,
    SNAPSHOT_TABLES,
    SUPPORTED_TABLES,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


_CONFIG_DIR = Path(__file__).parent / "configs"


def _load_json(name: str) -> dict:
    path = _CONFIG_DIR / name
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Shared-harness driven tests
# ---------------------------------------------------------------------------


class TestImmobiliareItConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Runs the full shared test harness against the live Market Explorer API."""

    connector_class = ImmobiliareItLakeflowConnect
    # Each history POST returns a moderate amount of data per (zone, month);
    # 25 records is enough to verify schema/parse without exhausting many
    # partitions (every partition is one HTTP call).
    sample_records = 25


# ---------------------------------------------------------------------------
# Connector-specific assertions beyond the shared harness
# ---------------------------------------------------------------------------


class TestImmobiliareItConnectorExtras:
    """Additional assertions that exercise connector-specific behaviour.

    These sit outside the shared harness because they rely on Market Explorer
    invariants (table-set membership, snapshot vs CDC partitioning split,
    cursor-field name, zone-filter plumbing).
    """

    connector: ImmobiliareItLakeflowConnect

    @classmethod
    def setup_class(cls):
        cls.config = _load_json("dev_config.json")
        cls.table_configs = _load_json("dev_table_config.json")
        cls.connector = ImmobiliareItLakeflowConnect(cls.config)

    # -- helpers -----------------------------------------------------------

    def _opts(self, table: str) -> dict:
        return self.table_configs.get(table, {})

    # -- tests -------------------------------------------------------------

    def test_list_tables_expected_eight(self):
        """All eight documented tables are exposed."""
        assert set(self.connector.list_tables()) == {
            "price_history",
            "ads_history",
            "search_data_history",
            "sales_price_history",
            "sales_volume_history",
            "regions",
            "provinces",
            "municipalities",
        }

    def test_supported_tables_match_module_constants(self):
        """list_tables() lines up with the module-level SUPPORTED_TABLES list."""
        assert list(self.connector.list_tables()) == list(SUPPORTED_TABLES)

    def test_is_partitioned_split(self):
        """Only history (CDC) tables are partitioned; snapshots are not."""
        for t in CDC_TABLES:
            assert self.connector.is_partitioned(t) is True, t
        for t in SNAPSHOT_TABLES:
            assert self.connector.is_partitioned(t) is False, t

    # -- snapshot tables: at least one row with non-null id_zone -----------

    @pytest.mark.parametrize("table", ["regions", "provinces", "municipalities"])
    def test_snapshot_read_yields_rows_with_id(self, table: str):
        """Snapshot reads return rows with a populated id_zone (non-nullable)."""
        iterator, offset = self.connector.read_table(table, {}, self._opts(table))
        records = list(iterator)
        assert records, f"Expected at least one record for {table}"
        assert offset == {}, f"{table} should return a stable (empty) offset"
        for rec in records:
            assert rec.get("id_zone"), f"{table} record missing id_zone: {rec}"

    def test_regions_count_is_in_expected_range(self):
        """Italy has 20 regions — sanity-check that the snapshot returns roughly that."""
        iterator, _ = self.connector.read_table("regions", {}, self._opts("regions"))
        records = list(iterator)
        # Be tolerant of edge cases (e.g. additional sub-divisions or
        # provisional rows). The hard floor is "non-empty" and the soft
        # ceiling is "not absurd".
        assert 15 <= len(records) <= 30, (
            f"Unexpected regions count: {len(records)} (expected ~20)"
        )

    # -- partitioned (CDC) tables: latest_offset has a numeric cursor ------

    @pytest.mark.parametrize("table", sorted(CDC_TABLES))
    def test_latest_offset_has_year_month_cursor(self, table: str):
        """latest_offset for CDC tables carries an integer YYYYMM cursor."""
        offset = self.connector.latest_offset(table, self._opts(table))
        assert "cursor" in offset, offset
        cursor = offset["cursor"]
        assert isinstance(cursor, int), (
            f"{table}: cursor should be int (YYYYMM), got {type(cursor).__name__}"
        )
        # YYYYMM in the documented data-availability window: 2016 onwards.
        assert 201601 <= cursor <= 999912, f"{table}: cursor out of range: {cursor}"
        assert 1 <= cursor % 100 <= 12, f"{table}: invalid month component: {cursor}"

    @pytest.mark.parametrize("table", sorted(CDC_TABLES))
    def test_partitions_and_read_yield_rows(self, table: str):
        """get_partitions produces >=1 partition and at least one record comes out."""
        end_offset = self.connector.latest_offset(table, self._opts(table))
        partitions = self.connector.get_partitions(
            table,
            self._opts(table),
            start_offset=None,
            end_offset=end_offset,
        )
        assert partitions, f"{table}: expected at least one partition"

        schema = self.connector.get_table_schema(table, self._opts(table))
        total = 0
        for p in partitions:
            for rec in self.connector.read_partition(table, p, self._opts(table)):
                assert isinstance(rec, dict), rec
                # ty_zone/id_zone are non-nullable on every history schema.
                assert rec.get("id_zone"), f"{table}: record missing id_zone"
                assert rec.get("ty_zone"), f"{table}: record missing ty_zone"
                total += 1
                if total >= 20:
                    break
            if total >= 20:
                break

        # We tolerate empty results across partitions only if the chosen
        # zone has no data at all in the configured window — log a clear
        # message rather than a generic fail.
        assert total >= 1, (
            f"{table}: expected at least one record across partitions "
            f"(filter: {self._opts(table)}). Are the zone_filter / "
            f"start_year_month settings still pointing at a zone with data?"
        )
        assert schema.fieldNames(), f"{table}: empty schema"

    # -- incremental cursor propagation ------------------------------------

    def test_incremental_start_offset_shrinks_partition_range(self):
        """A later start_offset produces fewer or equally-many partitions.

        Exercises that ``start_offset`` (cursor) is honoured by
        ``get_partitions`` for the partitioned-stream path.
        """
        table = "price_history"
        opts = self._opts(table)
        end_offset = self.connector.latest_offset(table, opts)

        # Baseline: from the configured floor.
        partitions_all = self.connector.get_partitions(
            table, opts, start_offset=None, end_offset=end_offset,
        )

        # Advance the cursor close to the latest. Resulting range should be
        # at most 1 month wide → fewer partitions than the baseline (which
        # spans roughly a year given dev_table_config.json).
        cursor_value = end_offset["cursor"]
        # Use one month before the latest — guaranteed to be <= end_offset.
        late_year = cursor_value // 100
        late_month = cursor_value % 100
        if late_month <= 1:
            late_year -= 1
            late_month = 12
        else:
            late_month -= 1
        late_start = {"cursor": late_year * 100 + late_month}

        partitions_late = self.connector.get_partitions(
            table, opts, start_offset=late_start, end_offset=end_offset,
        )

        assert len(partitions_late) <= len(partitions_all), (
            f"later start_offset produced MORE partitions: "
            f"all={len(partitions_all)} late={len(partitions_late)}"
        )

    def test_history_cursor_field_is_year_month(self):
        """The cursor_field declared in metadata is the synthetic year_month integer."""
        for table in sorted(CDC_TABLES):
            meta = self.connector.read_table_metadata(table, self._opts(table))
            assert meta.get("cursor_field") == "year_month", (
                f"{table}: cursor_field={meta.get('cursor_field')!r}, expected 'year_month'"
            )
            assert meta.get("ingestion_type") == "cdc", (
                f"{table}: ingestion_type={meta.get('ingestion_type')!r}, expected 'cdc'"
            )

    def test_history_cursor_field_present_in_records(self):
        """The cursor_field declared in metadata is populated in real records."""
        table = "price_history"
        opts = self._opts(table)
        end_offset = self.connector.latest_offset(table, opts)
        partitions = self.connector.get_partitions(
            table, opts, start_offset=None, end_offset=end_offset,
        )
        meta = self.connector.read_table_metadata(table, opts)
        cursor_field = meta["cursor_field"]

        found = False
        for p in partitions:
            for rec in self.connector.read_partition(table, p, opts):
                if rec.get(cursor_field):
                    found = True
                    break
            if found:
                break
        assert found, (
            f"{table}: no record had populated cursor_field {cursor_field!r} — "
            f"incremental reads will not advance."
        )

    # -- zone_filter plumbing ---------------------------------------------

    def test_zone_filter_restricts_partitions_to_that_zone(self):
        """All emitted partitions for a CDC table use the configured zone_filter."""
        table = "price_history"
        opts = self._opts(table)
        configured_zone = opts.get("zone_filter")
        assert configured_zone, (
            "dev_table_config.json must pin price_history.zone_filter for this test"
        )

        end_offset = self.connector.latest_offset(table, opts)
        partitions = self.connector.get_partitions(
            table, opts, start_offset=None, end_offset=end_offset,
        )
        assert partitions, f"{table}: expected at least one partition"

        for p in partitions:
            assert str(p.get("id_zone")) == str(configured_zone), (
                f"{table}: partition id_zone={p.get('id_zone')!r} does not match "
                f"configured zone_filter={configured_zone!r}"
            )

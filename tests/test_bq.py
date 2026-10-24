import datetime as dt
from unittest.mock import MagicMock

from ingest import bq


def test_partition_decorator():
    assert bq.partition_decorator("p.d.t", dt.date(2026, 10, 5)) == "p.d.t$20261005"


def test_stats_sql_with_key():
    sql = bq.stats_sql("p.d.t", ["a", "b"])
    assert "`a` IS NULL OR `b` IS NULL" in sql and "GROUP BY `a`, `b` HAVING COUNT(*) > 1" in sql


def test_stats_sql_without_key_skips_key_checks():
    sql = bq.stats_sql("p.d.t", [])
    assert "NULL AS null_key_rows" in sql and "GROUP BY" not in sql


def test_staging_stats_parses_row():
    client = MagicMock()
    client.query.return_value.result.return_value = iter(
        [{"row_count": 5, "null_key_rows": None, "duplicate_key_groups": None}]
    )
    assert bq.staging_stats(client, "p.d.t", []) == {
        "row_count": 5,
        "null_key_rows": None,
        "duplicate_key_groups": None,
    }

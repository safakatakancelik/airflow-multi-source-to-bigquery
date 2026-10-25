import datetime as dt
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingest import pipeline
from ingest.config import DestinationConfig, SourceConfig

DEST = DestinationConfig(gcp_conn_id="g", project="proj", dataset="ds")
SRC = SourceConfig(name="demo", type="http_file", key=["id"], options={"url": "u", "format": "csv"})
LOAD_DATE = dt.date(2026, 10, 25)


@pytest.fixture
def fake_extract(monkeypatch):
    monkeypatch.setitem(
        pipeline.EXTRACTORS,
        "http_file",
        lambda opts: pd.DataFrame({"ID": [1, 2, 3], "Name": list("abc")}),
    )


def test_extract_transform_round_trip(tmp_path, fake_extract):
    m = pipeline.run_extract(SRC, run_id="manual__x", root=tmp_path)
    assert m["rows_extracted"] == 3
    m = pipeline.run_transform(
        SRC,
        m,
        run_id="manual__x",
        load_date=LOAD_DATE,
        ingested_at=dt.datetime(2026, 10, 25, tzinfo=dt.UTC),
        root=tmp_path,
    )
    clean = pd.read_parquet(m["clean_path"])
    assert m["rows_transformed"] == 3 and {"id", "name", "_load_date"} <= set(clean.columns)


def manifest(**over):
    return {"staging_table": "proj.ds.stg_demo", "rows_extracted": 3, "rows_transformed": 3, **over}


def test_check_passes_and_writes_audit(monkeypatch):
    audit = MagicMock()
    monkeypatch.setattr(
        pipeline.bq,
        "staging_stats",
        lambda *a: {"row_count": 3, "null_key_rows": 0, "duplicate_key_groups": 0},
    )
    monkeypatch.setattr(pipeline.bq, "write_audit", audit)
    out = pipeline.run_check(SRC, DEST, manifest(), MagicMock(), run_id="r", dag_id="d")
    assert out["quality"]["status"] == "passed"
    assert audit.call_args.args[2]["status"] == "passed"


def test_check_failure_is_audited_then_raises(monkeypatch):
    audit = MagicMock()
    monkeypatch.setattr(
        pipeline.bq,
        "staging_stats",
        lambda *a: {"row_count": 2, "null_key_rows": 0, "duplicate_key_groups": 0},
    )
    monkeypatch.setattr(pipeline.bq, "write_audit", audit)
    with pytest.raises(pipeline.DataQualityError, match="mismatch"):
        pipeline.run_check(SRC, DEST, manifest(), MagicMock(), run_id="r", dag_id="d")
    assert audit.call_args.args[2]["status"] == "failed"


def test_publish_targets_the_load_date_partition(monkeypatch):
    publish = MagicMock()
    monkeypatch.setattr(pipeline.bq, "publish_partition", publish)
    out = pipeline.run_publish(SRC, DEST, manifest(), MagicMock(), load_date=LOAD_DATE)
    publish.assert_called_once()
    assert publish.call_args.args[1:] == ("proj.ds.stg_demo", "proj.ds.raw_demo", LOAD_DATE)
    assert out["load_date"] == "2026-10-25"


def test_load_date_falls_back_when_no_logical_date():
    run_after = dt.datetime(2026, 10, 25, 23, 30, tzinfo=dt.UTC)
    assert pipeline.load_date_for(None, run_after) == LOAD_DATE
    assert pipeline.load_date_for(dt.datetime(2026, 1, 2, tzinfo=dt.UTC), run_after) == dt.date(
        2026, 1, 2
    )


def test_cleanup_removes_run_directory(tmp_path, fake_extract):
    pipeline.run_extract(SRC, run_id="manual__2026:10:25", root=tmp_path)
    assert pipeline.run_dir("manual__2026:10:25", tmp_path).exists()
    pipeline.run_cleanup("manual__2026:10:25", tmp_path)
    assert not pipeline.run_dir("manual__2026:10:25", tmp_path).exists()

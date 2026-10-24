import datetime as dt

import pandas as pd
import pytest

from ingest.config import SourceConfig
from ingest.transform import METADATA_COLUMNS, snake_columns, to_snake, transform_frame

RUN = dict(
    run_id="manual__1",
    load_date=dt.date(2026, 10, 25),
    ingested_at=dt.datetime(2026, 10, 25, 9, 0, tzinfo=dt.UTC),
)


def src(**kw):
    return SourceConfig(name="demo", type="http_file", **kw)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Avg Rent USD", "avg_rent_usd"),
        ("Population (millions)", "population_millions"),
        ("jobTitle", "job_title"),
        ("address.city", "address_city"),
        ("_id", "id"),
        ("2fast", "col_2fast"),
        ("***", "col"),
    ],
)
def test_to_snake(raw, expected):
    assert to_snake(raw) == expected


def test_snake_columns_dedupes_collisions():
    df = pd.DataFrame([[1, 2]], columns=["Job Title", "job_title"])
    assert list(snake_columns(df).columns) == ["job_title", "job_title_2"]


def test_adds_metadata_columns():
    out = transform_frame(pd.DataFrame({"A": [1, 2]}), src(), **RUN)
    assert all(c in out.columns for c in METADATA_COLUMNS)
    assert set(out["_source"]) == {"demo"}
    assert set(out["_run_id"]) == {"manual__1"}
    assert set(out["_load_date"]) == {dt.date(2026, 10, 25)}
    assert str(out["_ingested_at"].dt.tz) == "UTC"


def test_row_count_is_preserved():
    df = pd.DataFrame({"a": range(7)})
    assert len(transform_frame(df, src(), **RUN)) == 7


def test_drop_rename_cast_use_snake_names():
    df = pd.DataFrame(
        {"Skill": ["sql"], "Demand Score": ["63.8"], "Updated On": ["2026-10-01"], "Junk": [1]}
    )
    spec = {
        "drop": ["junk"],
        "rename": {"skill": "name"},
        "cast": {"demand_score": "float", "updated_on": "date"},
    }
    out = transform_frame(df, src(transform=spec), **RUN)
    assert "junk" not in out.columns and "name" in out.columns
    assert out["demand_score"].iloc[0] == 63.8
    assert out["updated_on"].iloc[0] == dt.date(2026, 10, 1)


def test_bad_values_cast_to_null_not_crash():
    out = transform_frame(
        pd.DataFrame({"n": ["1", "oops"]}), src(transform={"cast": {"n": "int"}}), **RUN
    )
    assert out["n"].isna().tolist() == [False, True]


def test_nested_values_become_json_strings():
    df = pd.DataFrame({"tags": [["a", "b"]], "funding": [{"rounds": 2}]})
    out = transform_frame(df, src(), **RUN)
    assert out["tags"].iloc[0] == '["a", "b"]'
    assert out["funding"].iloc[0] == '{"rounds": 2}'


def test_missing_key_column_is_an_error():
    with pytest.raises(ValueError, match="key column"):
        transform_frame(pd.DataFrame({"a": [1]}), src(key=["id"]), **RUN)


def test_cast_of_missing_column_is_an_error():
    with pytest.raises(ValueError, match="missing column"):
        transform_frame(pd.DataFrame({"a": [1]}), src(transform={"cast": {"zzz": "int"}}), **RUN)


def test_output_is_parquet_serialisable(tmp_path):
    df = pd.DataFrame({"d": ["2026-10-01"], "n": ["3"], "s": ["x"]})
    out = transform_frame(df, src(transform={"cast": {"d": "date", "n": "int"}}), **RUN)
    out.to_parquet(tmp_path / "x.parquet", index=False, coerce_timestamps="us")
    assert len(pd.read_parquet(tmp_path / "x.parquet")) == 1

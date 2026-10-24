import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingest import extractors


def fake_module(monkeypatch, dotted, **attrs):
    """Install a stand-in for an Airflow provider module so no provider/credentials are needed."""
    mod = ModuleType(dotted)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, dotted, mod)
    return mod


def test_every_configured_type_has_an_extractor():
    from ingest.config import REQUIRED_OPTIONS

    assert set(extractors.EXTRACTORS) == set(REQUIRED_OPTIONS)


def test_sql_extractors_pass_query_to_hook(monkeypatch):
    hook = MagicMock()
    hook.get_df.return_value = pd.DataFrame({"a": [1]})
    fake_module(
        monkeypatch, "airflow.providers.postgres.hooks.postgres", PostgresHook=lambda **kw: hook
    )
    fake_module(
        monkeypatch,
        "airflow.providers.databricks.hooks.databricks_sql",
        DatabricksSqlHook=lambda **kw: hook,
    )
    assert len(extractors.postgres({"conn_id": "c", "query": "select 1"})) == 1
    assert (
        len(extractors.databricks_sql({"conn_id": "c", "http_path": "/p", "query": "select 1"}))
        == 1
    )
    assert hook.get_df.call_count == 2


def test_google_sheets_pads_short_rows(monkeypatch):
    hook = MagicMock()
    hook.get_values.return_value = [["skill", "score", "trend"], ["sql", "1"], ["py", "2", "up"]]
    fake_module(
        monkeypatch, "airflow.providers.google.suite.hooks.sheets", GSheetsHook=lambda **kw: hook
    )
    df = extractors.google_sheets({"conn_id": "c", "spreadsheet_id": "s", "range": "A:C"})
    assert df.shape == (2, 3) and pd.isna(df.loc[0, "trend"])


def test_google_sheets_empty(monkeypatch):
    hook = MagicMock()
    hook.get_values.return_value = []
    fake_module(
        monkeypatch, "airflow.providers.google.suite.hooks.sheets", GSheetsHook=lambda **kw: hook
    )
    assert extractors.google_sheets({"conn_id": "c", "spreadsheet_id": "s", "range": "A:C"}).empty


def test_mongo_flattens_and_stringifies_object_id(monkeypatch):
    docs = [{"_id": object(), "name": "x", "address": {"city": "Berlin"}}]
    hook = MagicMock()
    hook.find.return_value = iter(docs)
    fake_module(monkeypatch, "airflow.providers.mongo.hooks.mongo", MongoHook=lambda **kw: hook)
    df = extractors.mongo({"conn_id": "c", "database": "d", "collection": "col"})
    assert "address.city" in df.columns and df.loc[0, "address.city"] == "Berlin"
    assert isinstance(df.loc[0, "_id"], str)


def test_http_file_csv_and_unknown_format(monkeypatch):
    resp = SimpleNamespace(content=b"a,b\n1,2\n", raise_for_status=lambda: None)
    monkeypatch.setattr("requests.get", lambda url, timeout: resp)
    assert extractors.http_file({"url": "u", "format": "csv"}).shape == (1, 2)
    with pytest.raises(ValueError, match="Unsupported"):
        extractors.http_file({"url": "u", "format": "xlsx"})


def test_rest_api_follows_next_links(monkeypatch):
    pages = [
        {"results": [{"name": "a"}], "next": "https://api/x?offset=1"},
        {"results": [{"name": "b"}], "next": None},
    ]
    calls = []

    class Session:
        def get(self, url, params=None, timeout=None):
            calls.append((url, params))
            body = pages[len(calls) - 1]
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: body)

    class Hook:
        def __init__(self, **kw): ...
        def get_conn(self):
            return Session()

        def url_from_endpoint(self, endpoint):
            return "https://api" + endpoint

    fake_module(monkeypatch, "airflow.providers.http.hooks.http", HttpHook=Hook)
    df = extractors.rest_api(
        {
            "conn_id": "c",
            "endpoint": "/x",
            "params": {"limit": 1},
            "records_path": "results",
            "next_path": "next",
        }
    )
    assert df["name"].tolist() == ["a", "b"]
    assert calls[0] == ("https://api/x", {"limit": 1}) and calls[1] == (
        "https://api/x?offset=1",
        None,
    )


def test_rest_api_respects_max_pages(monkeypatch):
    class Session:
        def get(self, url, params=None, timeout=None):
            return SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {"results": [{"n": 1}], "next": "https://api/more"},
            )

    class Hook:
        def __init__(self, **kw): ...
        def get_conn(self):
            return Session()

        def url_from_endpoint(self, endpoint):
            return "https://api" + endpoint

    fake_module(monkeypatch, "airflow.providers.http.hooks.http", HttpHook=Hook)
    df = extractors.rest_api(
        {
            "conn_id": "c",
            "endpoint": "/x",
            "records_path": "results",
            "next_path": "next",
            "max_pages": 3,
        }
    )
    assert len(df) == 3

import copy

import pytest

from ingest.config import ConfigError, MissingEnvError, load_config, parse_config, resolve

BASE = {
    "destination": {"gcp_conn_id": "g", "project": "p", "dataset": "d"},
    "sources": [{"name": "a_src", "type": "http_file", "url": "https://x/y.csv", "format": "csv"}],
}


def mutate(fn):
    data = copy.deepcopy(BASE)
    fn(data)
    return data


def test_repo_config_is_valid():
    cfg = load_config()
    assert cfg.sources and len({s.name for s in cfg.sources}) == len(cfg.sources)


def test_table_names():
    src = parse_config(BASE).sources[0]
    assert (src.staging_table, src.target_table) == ("stg_a_src", "raw_a_src")


@pytest.mark.parametrize("bad", ["Bad-Name", "1abc", "", "has space"])
def test_invalid_source_name(bad):
    with pytest.raises(ConfigError):
        parse_config(mutate(lambda d: d["sources"][0].update(name=bad)))


def test_unknown_type():
    with pytest.raises(ConfigError, match="unknown type"):
        parse_config(mutate(lambda d: d["sources"][0].update(type="ftp")))


def test_missing_required_option():
    with pytest.raises(ConfigError, match="missing options"):
        parse_config(mutate(lambda d: d["sources"][0].pop("url")))


def test_duplicate_names():
    with pytest.raises(ConfigError, match="Duplicate"):
        parse_config(mutate(lambda d: d["sources"].append(dict(d["sources"][0]))))


def test_destination_required():
    with pytest.raises(ConfigError):
        parse_config(mutate(lambda d: d["destination"].pop("project")))


def test_key_string_is_normalised_to_list():
    cfg = parse_config(mutate(lambda d: d["sources"][0].update(key="id")))
    assert cfg.sources[0].key == ["id"]


def test_resolve_env(monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    monkeypatch.delenv("NOPE", raising=False)
    assert resolve("x-${FOO}-${NOPE:-dflt}") == "x-bar-dflt"
    assert resolve({"a": ["${FOO}"]}) == {"a": ["bar"]}
    with pytest.raises(MissingEnvError):
        resolve("${NOPE}")

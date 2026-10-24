"""Load and validate config/sources.yaml.

Values may contain ``${VAR}`` or ``${VAR:-default}`` placeholders. They are expanded lazily
(``resolve``) when a task runs, never at DAG-parse time, so parsing the DAG needs no secrets.
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

# Options each extractor needs. Kept here (not in the extractor modules) so that validating the
# config never imports Airflow providers.
REQUIRED_OPTIONS: dict[str, list[str]] = {
    "databricks_sql": ["conn_id", "http_path", "query"],
    "rest_api": ["conn_id", "endpoint"],
    "postgres": ["conn_id", "query"],
    "mongo": ["conn_id", "database", "collection"],
    "s3_file": ["conn_id", "bucket", "object_key", "format"],
    "http_file": ["url", "format"],
}


class ConfigError(ValueError):
    pass


class MissingEnvError(ConfigError):
    pass


@dataclass(frozen=True)
class DestinationConfig:
    gcp_conn_id: str
    project: str
    dataset: str
    location: str = "US"
    staging_expiration_days: int = 7

    def table_id(self, table: str) -> str:
        return f"{self.project}.{self.dataset}.{table}"


@dataclass(frozen=True)
class SourceConfig:
    name: str
    type: str
    key: list[str] = field(default_factory=list)
    transform: dict[str, Any] = field(default_factory=dict)
    allow_empty: bool = False
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def staging_table(self) -> str:
        return f"stg_{self.name}"

    @property
    def target_table(self) -> str:
        return f"raw_{self.name}"


@dataclass(frozen=True)
class IngestConfig:
    destination: DestinationConfig
    sources: list[SourceConfig]


def resolve(value: Any) -> Any:
    """Recursively expand ${VAR} / ${VAR:-default} in strings, dicts and lists."""
    if isinstance(value, str):

        def _sub(match: re.Match) -> str:
            var, default = match.group(1), match.group(2)
            found = os.environ.get(var)
            if found:
                return found
            if default is not None:
                return default
            raise MissingEnvError(f"Environment variable {var} is not set")

        return _ENV_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v) for v in value]
    return value


def resolve_destination(dest: DestinationConfig) -> DestinationConfig:
    """Expand ${VAR} placeholders in the destination (call at task runtime, not parse time)."""
    return DestinationConfig(**{k: resolve(v) for k, v in asdict(dest).items()})


def _parse_source(raw: dict[str, Any]) -> SourceConfig:
    raw = dict(raw)
    name = raw.pop("name", None)
    if not name or not NAME_RE.match(name):
        raise ConfigError(f"Source name {name!r} must match {NAME_RE.pattern}")
    stype = raw.pop("type", None)
    if stype not in REQUIRED_OPTIONS:
        raise ConfigError(
            f"Source {name}: unknown type {stype!r}; expected one of {list(REQUIRED_OPTIONS)}"
        )
    key = raw.pop("key", [])
    key = [key] if isinstance(key, str) else list(key)
    transform = raw.pop("transform", {}) or {}
    allow_empty = bool(raw.pop("allow_empty", False))
    missing = [opt for opt in REQUIRED_OPTIONS[stype] if opt not in raw]
    if missing:
        raise ConfigError(f"Source {name} ({stype}) is missing options: {missing}")
    return SourceConfig(
        name=name, type=stype, key=key, transform=transform, allow_empty=allow_empty, options=raw
    )


def parse_config(data: dict[str, Any]) -> IngestConfig:
    dest = data.get("destination") or {}
    for required in ("gcp_conn_id", "project", "dataset"):
        if required not in dest:
            raise ConfigError(f"destination.{required} is required")
    destination = DestinationConfig(**{k: dest[k] for k in dest})
    sources = [_parse_source(s) for s in data.get("sources") or []]
    if not sources:
        raise ConfigError("No sources configured")
    names = [s.name for s in sources]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise ConfigError(f"Duplicate source names: {sorted(dupes)}")
    return IngestConfig(destination=destination, sources=sources)


def default_config_path() -> Path:
    override = os.environ.get("INGEST_CONFIG")
    if override:
        return Path(override)
    # Local checkout: <repo>/config/sources.yaml. In the container: /opt/airflow/config/sources.yaml.
    return Path(__file__).resolve().parents[2] / "config" / "sources.yaml"


def load_config(path: Path | None = None) -> IngestConfig:
    path = path or default_config_path()
    with open(path) as fh:
        return parse_config(yaml.safe_load(fh))

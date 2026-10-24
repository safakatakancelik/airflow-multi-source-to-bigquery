"""Light, deterministic transforms applied to every source before loading.

Order: snake_case columns -> drop -> rename -> cast -> stringify nested values -> metadata columns.
``drop`` / ``rename`` / ``cast`` therefore refer to the *snake_case* column names.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any

import pandas as pd

from .config import SourceConfig

METADATA_COLUMNS = ["_source", "_run_id", "_ingested_at", "_load_date"]
_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")


def to_snake(name: str) -> str:
    """BigQuery-safe snake_case: ``Job Title`` / ``jobTitle`` / ``job.title`` -> ``job_title``."""
    name = _CAMEL_1.sub(r"\1_\2", str(name))
    name = _CAMEL_2.sub(r"\1_\2", name)
    name = _NON_ALNUM.sub("_", name).strip("_").lower()
    if not name:
        return "col"
    if name[0].isdigit():
        name = f"col_{name}"
    return name[:300]


def snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    seen: dict[str, int] = {}
    new_cols = []
    for col in df.columns:
        base = to_snake(col)
        seen[base] = seen.get(base, 0) + 1
        new_cols.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    out = df.copy()
    out.columns = new_cols
    return out


def _stringify_nested(df: pd.DataFrame) -> pd.DataFrame:
    """Dicts/lists left after flattening become JSON strings so BigQuery can store them."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            has_nested = out[col].map(lambda v: isinstance(v, dict | list | tuple | set)).any()
            if has_nested:
                out[col] = out[col].map(
                    lambda v: (
                        json.dumps(sorted(v) if isinstance(v, set) else v, default=str)
                        if isinstance(v, dict | list | tuple | set)
                        else v
                    )
                )
    return out


def _cast_series(series: pd.Series, to: str) -> pd.Series:
    if to == "string":
        return series.astype("string")
    if to == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if to == "float":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if to == "bool":
        mapped = (
            series.astype("string")
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
        )
        return mapped.astype("boolean")
    if to == "date":
        return pd.to_datetime(series, errors="coerce").dt.date
    if to == "datetime":
        return pd.to_datetime(series, errors="coerce", utc=True)
    raise ValueError(f"Unsupported cast type {to!r}")


def transform_frame(
    df: pd.DataFrame,
    src: SourceConfig,
    *,
    run_id: str,
    load_date: dt.date,
    ingested_at: dt.datetime,
) -> pd.DataFrame:
    spec: dict[str, Any] = src.transform
    out = snake_columns(df)

    drop = spec.get("drop") or []
    out = out.drop(columns=[c for c in drop if c in out.columns])

    rename = spec.get("rename") or {}
    out = out.rename(columns=rename)

    for col, to in (spec.get("cast") or {}).items():
        if col not in out.columns:
            raise ValueError(f"{src.name}: cannot cast missing column {col!r}")
        out[col] = _cast_series(out[col], to)

    out = _stringify_nested(out)

    missing_keys = [k for k in src.key if k not in out.columns]
    if missing_keys:
        raise ValueError(f"{src.name}: key column(s) {missing_keys} not found after transform")

    out["_source"] = src.name
    out["_run_id"] = run_id
    out["_ingested_at"] = pd.Timestamp(ingested_at).tz_convert("UTC")
    out["_load_date"] = load_date
    return out.reset_index(drop=True)

"""The steps of one source's pipeline as plain functions.

``dags/ingest_all_sources.py`` wraps these in Airflow tasks. Keeping them Airflow-free means
they can be unit-tested with mocks. Steps hand a small "manifest" dict to each other (via XCom);
the data itself travels as Parquet files on local disk and then through BigQuery.
"""

from __future__ import annotations

import datetime as dt
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from . import bq
from .config import DestinationConfig, SourceConfig, resolve
from .extractors import EXTRACTORS
from .quality import evaluate
from .transform import transform_frame


class DataQualityError(Exception):
    """Raised when a quality check fails. Retrying will not help, so the DAG maps this to a
    non-retryable failure."""


def staging_root() -> Path:
    return Path(tempfile.gettempdir()) / "airflow-ingest"


def run_dir(run_id: str, root: Path | None = None) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)
    return (root or staging_root()) / safe


def load_date_for(logical_date: dt.datetime | None, run_after: dt.datetime | None) -> dt.date:
    """Daily partition a run writes to. Manually triggered runs may have no logical date in
    Airflow 3, so fall back to when the run was requested."""
    moment = logical_date or run_after or dt.datetime.now(dt.UTC)
    return moment.astimezone(dt.UTC).date()


def maybe_inject_failure(source: str, try_number: int, switch: str | None) -> None:
    """Demo chaos switch: when the Airflow Variable ``demo_fail_first_attempt`` names this source,
    its first attempt fails so a retry can be shown succeeding."""
    if switch and switch == source and try_number <= 1:
        raise RuntimeError(f"Injected demo failure for {source!r} on attempt {try_number}")


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)


def run_extract(
    src: SourceConfig,
    *,
    run_id: str,
    try_number: int = 1,
    fail_switch: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    maybe_inject_failure(src.name, try_number, fail_switch)
    df = EXTRACTORS[src.type](resolve(src.options))
    path = run_dir(run_id, root) / f"{src.name}.raw.parquet"
    _write_parquet(df, path)
    return {"source": src.name, "raw_path": str(path), "rows_extracted": len(df)}


def run_transform(
    src: SourceConfig,
    manifest: dict[str, Any],
    *,
    run_id: str,
    load_date: dt.date,
    ingested_at: dt.datetime,
    root: Path | None = None,
) -> dict[str, Any]:
    df = pd.read_parquet(manifest["raw_path"])
    out = transform_frame(df, src, run_id=run_id, load_date=load_date, ingested_at=ingested_at)
    path = run_dir(run_id, root) / f"{src.name}.clean.parquet"
    _write_parquet(out, path)
    return {**manifest, "clean_path": str(path), "rows_transformed": len(out)}


def run_stage(
    src: SourceConfig, dest: DestinationConfig, manifest: dict[str, Any], client
) -> dict[str, Any]:
    staging_id = dest.table_id(src.staging_table)
    bq.load_parquet_to_staging(
        client, manifest["clean_path"], staging_id, dest.staging_expiration_days
    )
    return {**manifest, "staging_table": staging_id}


def run_check(
    src: SourceConfig,
    dest: DestinationConfig,
    manifest: dict[str, Any],
    client,
    *,
    run_id: str,
    dag_id: str,
    try_number: int = 1,
) -> dict[str, Any]:
    stats = bq.staging_stats(client, manifest["staging_table"], src.key)
    report = evaluate(
        rows_extracted=manifest["rows_extracted"],
        rows_transformed=manifest["rows_transformed"],
        rows_staged=stats["row_count"],
        null_key_rows=stats["null_key_rows"],
        duplicate_key_groups=stats["duplicate_key_groups"],
        allow_empty=src.allow_empty,
    )
    bq.write_audit(
        client,
        dest,
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "source": src.name,
            "status": report.status,
            "try_number": try_number,
            "rows_extracted": report.rows_extracted,
            "rows_transformed": report.rows_transformed,
            "rows_staged": report.rows_staged,
            "null_key_rows": report.null_key_rows,
            "duplicate_key_groups": report.duplicate_key_groups,
            "failures": "; ".join(report.failures) or None,
            "recorded_at": dt.datetime.now(dt.UTC).isoformat(),
        },
    )
    if report.failures:
        raise DataQualityError(f"{src.name}: " + "; ".join(report.failures))
    return {**manifest, "quality": report.as_dict()}


def run_publish(
    src: SourceConfig,
    dest: DestinationConfig,
    manifest: dict[str, Any],
    client,
    *,
    load_date: dt.date,
) -> dict[str, Any]:
    target_id = dest.table_id(src.target_table)
    bq.publish_partition(client, manifest["staging_table"], target_id, load_date)
    return {**manifest, "target_table": target_id, "load_date": load_date.isoformat()}


def run_cleanup(run_id: str, root: Path | None = None) -> None:
    shutil.rmtree(run_dir(run_id, root), ignore_errors=True)

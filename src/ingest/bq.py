"""BigQuery helpers. Everything takes an already-built ``google.cloud.bigquery.Client`` so the
logic can be unit-tested with a mock and never hard-codes credentials."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from .config import DestinationConfig

AUDIT_TABLE = "ingestion_audit"
AUDIT_SCHEMA: list[tuple[str, str]] = [
    ("run_id", "STRING"),
    ("dag_id", "STRING"),
    ("source", "STRING"),
    ("status", "STRING"),
    ("try_number", "INT64"),
    ("rows_extracted", "INT64"),
    ("rows_transformed", "INT64"),
    ("rows_staged", "INT64"),
    ("null_key_rows", "INT64"),
    ("duplicate_key_groups", "INT64"),
    ("failures", "STRING"),
    ("recorded_at", "TIMESTAMP"),
]


def make_client(dest: DestinationConfig):
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    return BigQueryHook(gcp_conn_id=dest.gcp_conn_id).get_client(
        project_id=dest.project, location=dest.location
    )


def partition_decorator(table_id: str, load_date: dt.date) -> str:
    """`project.dataset.table$YYYYMMDD` — addresses exactly one daily partition."""
    return f"{table_id}${load_date:%Y%m%d}"


def stats_sql(table_id: str, key: list[str]) -> str:
    if not key:
        return f"SELECT COUNT(*) AS row_count, NULL AS null_key_rows, NULL AS duplicate_key_groups FROM `{table_id}`"
    cols = ", ".join(f"`{k}`" for k in key)
    any_null = " OR ".join(f"`{k}` IS NULL" for k in key)
    return (
        f"SELECT COUNT(*) AS row_count, "
        f"COUNTIF({any_null}) AS null_key_rows, "
        f"(SELECT COUNT(*) FROM (SELECT 1 FROM `{table_id}` GROUP BY {cols} HAVING COUNT(*) > 1)) "
        f"AS duplicate_key_groups "
        f"FROM `{table_id}`"
    )


def ensure_dataset(client, dest: DestinationConfig) -> None:
    from google.cloud import bigquery

    dataset = bigquery.Dataset(f"{dest.project}.{dest.dataset}")
    dataset.location = dest.location
    client.create_dataset(dataset, exists_ok=True)
    audit = bigquery.Table(
        dest.table_id(AUDIT_TABLE),
        schema=[bigquery.SchemaField(n, t) for n, t in AUDIT_SCHEMA],
    )
    client.create_table(audit, exists_ok=True)


def load_parquet_to_staging(client, path: str | Path, table_id: str, expiration_days: int) -> int:
    """Replace the staging table with the file's rows. Returns the row count BigQuery reports."""
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    with open(path, "rb") as fh:
        client.load_table_from_file(fh, table_id, job_config=job_config).result()
    table = client.get_table(table_id)
    table.expires = dt.datetime.now(dt.UTC) + dt.timedelta(days=expiration_days)
    client.update_table(table, ["expires"])
    return int(table.num_rows)


def staging_stats(client, table_id: str, key: list[str]) -> dict[str, Any]:
    row = next(iter(client.query(stats_sql(table_id, key)).result()))
    return {
        "row_count": int(row["row_count"]),
        "null_key_rows": None if row["null_key_rows"] is None else int(row["null_key_rows"]),
        "duplicate_key_groups": None
        if row["duplicate_key_groups"] is None
        else int(row["duplicate_key_groups"]),
    }


def publish_partition(client, staging_id: str, target_id: str, load_date: dt.date) -> None:
    """Atomically replace one daily partition of the target with the staging table's rows.

    Re-running a load for the same date overwrites that partition (never appends), which is what
    makes the pipeline idempotent.
    """
    from google.cloud import bigquery

    staging = client.get_table(staging_id)
    target = bigquery.Table(target_id, schema=staging.schema)
    target.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="_load_date"
    )
    client.create_table(target, exists_ok=True)

    job_config = bigquery.QueryJobConfig(
        destination=partition_decorator(target_id, load_date),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.query(f"SELECT * FROM `{staging_id}`", job_config=job_config).result()


def write_audit(client, dest: DestinationConfig, record: dict[str, Any]) -> None:
    """Append one audit row with a load job (not a streaming insert, so it stays free-tier-friendly
    and immediately queryable/deletable)."""
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(n, t) for n, t in AUDIT_SCHEMA],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    client.load_table_from_json(
        [record], dest.table_id(AUDIT_TABLE), job_config=job_config
    ).result()

"""Config-driven ingestion: many different sources -> BigQuery.

One task group per entry in ``config/sources.yaml``:

    extract -> transform -> load_staging -> quality_check -> publish

* **Idempotent**: ``publish`` overwrites the run's daily partition; re-running never duplicates rows.
* **Quality gate**: source vs. BigQuery row counts and NULL/duplicate keys, recorded in
  ``ingestion_audit``. A failed check blocks ``publish`` and is not retried.
* **Retries**: transient errors (network, API hiccups) retry automatically.
* **Credentials** come from Airflow Connections only.
"""

from __future__ import annotations

import datetime as dt
import logging

from airflow.exceptions import AirflowFailException
from airflow.sdk import Asset, dag, get_current_context, task, task_group

from ingest import bq, pipeline
from ingest.config import SourceConfig, load_config, resolve_destination

log = logging.getLogger(__name__)
CONFIG = load_config()
DEST = CONFIG.destination


def notify_failure(context) -> None:
    """Failure callback hook. Swap the log line for Slack/email in a real deployment."""
    ti = context["ti"]
    log.error("Task %s.%s failed (try %s)", ti.dag_id, ti.task_id, ti.try_number)


def _ctx():
    """(run_id, try_number, load_date) for the currently executing task."""
    ctx = get_current_context()
    ti, dag_run = ctx["ti"], ctx["dag_run"]
    load_date = pipeline.load_date_for(ctx.get("logical_date"), getattr(dag_run, "run_after", None))
    return ti.run_id, ti.try_number, load_date


def source_pipeline(src: SourceConfig):
    @task_group(group_id=src.name)
    def _group():
        @task(task_id="extract")
        def extract() -> dict:
            run_id, _, _ = _ctx()
            return pipeline.run_extract(src, run_id=run_id)

        @task(task_id="transform")
        def transform(manifest: dict) -> dict:
            run_id, _, load_date = _ctx()
            return pipeline.run_transform(
                src,
                manifest,
                run_id=run_id,
                load_date=load_date,
                ingested_at=dt.datetime.now(dt.UTC),
            )

        @task(task_id="load_staging")
        def load_staging(manifest: dict) -> dict:
            dest = resolve_destination(DEST)
            return pipeline.run_stage(src, dest, manifest, bq.make_client(dest))

        @task(task_id="quality_check")
        def quality_check(manifest: dict) -> dict:
            run_id, try_number, _ = _ctx()
            dest = resolve_destination(DEST)
            try:
                return pipeline.run_check(
                    src,
                    dest,
                    manifest,
                    bq.make_client(dest),
                    run_id=run_id,
                    dag_id="ingest_all_sources",
                    try_number=try_number,
                )
            except pipeline.DataQualityError as exc:
                raise AirflowFailException(str(exc)) from exc  # retrying cannot fix bad data

        @task(
            task_id="publish", outlets=[Asset(name=f"bigquery.{DEST.dataset}.{src.target_table}")]
        )
        def publish(manifest: dict) -> dict:
            _, _, load_date = _ctx()
            dest = resolve_destination(DEST)
            return pipeline.run_publish(
                src, dest, manifest, bq.make_client(dest), load_date=load_date
            )

        publish(quality_check(load_staging(transform(extract()))))

    return _group()


@dag(
    dag_id="ingest_all_sources",
    schedule=None,  # triggered manually; this project demonstrates the pattern, not a cron job
    start_date=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": dt.timedelta(seconds=20),
        "on_failure_callback": notify_failure,
    },
    tags=["ingestion", "bigquery", "portfolio"],
    doc_md=__doc__,
)
def ingest_all_sources():
    @task
    def prepare_destination() -> None:
        dest = resolve_destination(DEST)
        bq.ensure_dataset(bq.make_client(dest), dest)

    @task(trigger_rule="all_done")
    def cleanup_local_files() -> None:
        pipeline.run_cleanup(_ctx()[0])

    prepare = prepare_destination()
    groups = [source_pipeline(src) for src in CONFIG.sources]
    cleanup = cleanup_local_files()
    for group in groups:
        prepare >> group >> cleanup


ingest_all_sources()

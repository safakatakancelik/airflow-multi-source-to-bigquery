"""Structural checks on the DAG. These need Airflow installed but no credentials or network."""

from pathlib import Path

import pytest

from ingest.config import load_config

DAGS = Path(__file__).resolve().parents[1] / "dags"
STEPS = ["extract", "transform", "load_staging", "quality_check", "publish"]


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), bundle_path=DAGS, bundle_name="tests")
    assert not bag.import_errors, bag.import_errors
    return bag.dags["ingest_all_sources"]


def test_dag_has_no_import_errors_and_is_manual(dag):
    assert dag.schedule is None or str(dag.schedule).lower() in {"none", "null"}
    assert dag.catchup is False and dag.max_active_runs == 1


def test_every_task_retries(dag):
    assert all(t.retries >= 1 for t in dag.tasks)


def test_one_group_per_source_with_ordered_steps(dag):
    for src in load_config().sources:
        ids = [f"{src.name}.{s}" for s in STEPS]
        assert all(i in dag.task_ids for i in ids), src.name
        for upstream, downstream in zip(ids, ids[1:], strict=False):
            assert downstream in dag.get_task(upstream).downstream_task_ids


def test_prepare_runs_first_and_cleanup_last(dag):
    prepare, cleanup = dag.get_task("prepare_destination"), dag.get_task("cleanup_local_files")
    for src in load_config().sources:
        assert f"{src.name}.extract" in prepare.downstream_task_ids
        assert "cleanup_local_files" in dag.get_task(f"{src.name}.publish").downstream_task_ids
    assert cleanup.trigger_rule == "all_done"


def test_publish_declares_an_asset_outlet(dag):
    for src in load_config().sources:
        assert dag.get_task(f"{src.name}.publish").outlets


def test_failure_callback_is_configured(dag):
    assert all(t.on_failure_callback for t in dag.tasks)


def test_asset_names_have_no_unresolved_placeholders(dag):
    for src in load_config().sources:
        (outlet,) = dag.get_task(f"{src.name}.publish").outlets
        assert "${" not in outlet.name

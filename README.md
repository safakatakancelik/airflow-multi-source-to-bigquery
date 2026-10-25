# Airflow multi-source to BigQuery

[![CI](https://github.com/safakatakancelik/airflow-connectors-to-bigquery/actions/workflows/ci.yml/badge.svg)](https://github.com/safakatakancelik/airflow-connectors-to-bigquery/actions/workflows/ci.yml)

Config-driven Airflow DAG that ingests five source types (REST API, Postgres, MongoDB, S3, CSV) into BigQuery. Idempotent loads, data-quality gate, audit table, tests and CI.

The focus of this project is moving data using Airflow across different sources.

## Demo video

> **Demo video coming soon.**

## Architecture

![Architecture: five sources flow through Apache Airflow into BigQuery](docs/architecture.svg)

| Source | Type | Data it holds |
|---|---|---|
| PokeAPI | Public REST API, JSON over HTTP | Pokémon names and URLs, paginated (150 rows) |
| Supabase | Managed PostgreSQL, relational database | Songs joined to artists and genres (100 rows) |
| MongoDB Atlas | NoSQL document database | Nested playlist documents (30 documents) |
| Amazon S3 | Object storage, Parquet file | Daily song stream counts (200 rows) |
| GitHub raw URL | Public CSV file over HTTPS | S&P 500 company list (about 500 rows) |

## A successful run's structure

![Airflow graph view of a successful run: five source task groups between prepare_destination and cleanup_local_files, all green](docs/successful-run.webp)

The DAG runs its tasks in this order:
1. `prepare_destination`
2. One task group per source, each with the same five tasks: `extract`, `transform`, `load_staging`, `quality_check`, `publish`
3. `cleanup_local_files`

### What each task does

| Task | Purpose |
|---|---|
| `prepare_destination` | Runs once, before any source. Creates the BigQuery dataset and the `ingestion_audit` table if they don't exist yet, so the per-source tasks can assume they are there. Safe to repeat. |
| `extract` | Reads from the source and writes the rows to a local Parquet file. |
| `transform` | Light, deterministic cleanup: snake_case column names, per-source `drop` / `rename` / `cast`, nested values turned into JSON strings, plus `_source`, `_run_id`, `_ingested_at` and `_load_date` columns. It never changes the row count. |
| `load_staging` | Loads the Parquet file into `stg_<source>`. The table is replaced on every run and expires after 7 days. |
| `quality_check` | Compares row counts (extracted → transformed → in BigQuery) and checks key columns for NULLs and duplicates. Writes a row to `ingestion_audit` whether it passes or fails. A failure stops that source and is **not retried**, because bad data won't fix itself. |
| `publish` | Overwrites that day's partition of `raw_<source>` from the staging table. |
| `cleanup_local_files` | Runs once, after all sources, even if some of them failed. Deletes the run's temporary Parquet files from the Airflow container. |

## Project layout

```
config/sources.yaml        sources and destination (structure only, no secrets)
dags/ingest_all_sources.py the DAG: builds one task group per source
src/ingest/                config, extractors, transforms, quality checks, BigQuery helpers
seed/                      dummy-data generator, SQL seed, Mongo loader, Parquet sample
tests/                     unit tests, DAG integrity tests, secret guard rails
docs/                      architecture diagram and run screenshot
docker-compose.yaml        Airflow with LocalExecutor and Postgres (version pinned in the Dockerfile)
```

## Requirements

| Tool | Version | Used for |
|---|---|---|
| Docker with Compose v2 | any recent | running Airflow locally (about 3 GB of free RAM) |
| Git | any | cloning |
| Python and [uv](https://docs.astral.sh/uv/) | 3.13 | only to run the tests or seed scripts outside Docker |
| Accounts | | Google Cloud (BigQuery), Supabase, MongoDB Atlas, AWS (S3) |

## Quick start

1. **Clone and create `.env`.** The script copies `.env.example` to `.env` and generates the local Airflow secrets:
   ```bash
   git clone https://github.com/safakatakancelik/airflow-connectors-to-bigquery.git
   cd airflow-connectors-to-bigquery
   ./scripts/init-env.sh
   ```
2. **BigQuery.** Create a Google Cloud project and a service account with the *BigQuery Data Editor* and *BigQuery Job User* roles. Save its JSON key as `secrets/gcp-sa.json`, then set `GCP_PROJECT_ID` and the connection in `.env`.
3. **Seed and connect the sources** (each connection is a line in `.env`; see `.env.example`):
   - **Supabase:** run [`seed/supabase_seed.sql`](seed/supabase_seed.sql) in the SQL editor. Use the *Session pooler* connection details.
   - **MongoDB Atlas:** `MONGO_URI='mongodb+srv://USER:PASS@HOST/' uv run --with pymongo python seed/load_mongo.py`
   - **Amazon S3:** upload [`seed/s3_song_streams.parquet`](seed/s3_song_streams.parquet) to `s3://<your-bucket>/ingest-demo/song_streams.parquet` and give an IAM user read access to it.
   - **PokeAPI and the public CSV:** nothing to set up.
4. **Start Airflow:**
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```
5. Open http://localhost:8080 (user `airflow`, password printed by `init-env.sh` and stored in `.env`), unpause `ingest_all_sources` and click **Trigger**.

Stop with `docker compose down` (add `-v` to also wipe Airflow's metadata database).

## Engineering choices

- **Idempotent load.** `publish` replaces the `_load_date` partition (`WRITE_TRUNCATE` on a partition decorator) instead of appending, so re-running a task or the whole DAG never duplicates rows.
- **Staging before publishing.** Nothing reaches the `raw_*` tables until it passes the quality gate.
- **Retries.** Every task retries twice for transient errors. The quality check is deliberately non-retryable (`AirflowFailException`).
- **Config-driven.** One DAG builds a task group per entry in [`config/sources.yaml`](config/sources.yaml). Adding a source is a YAML block, not a code change.
- **Assets.** Each `publish` task declares an Airflow Asset, so the lineage from source task to BigQuery table shows in the UI.
- **No secrets in code.** See [Configuration & secrets](#configuration--secrets).
- **Testable core.** The pipeline logic is plain Python in [`src/ingest`](src/ingest). Airflow imports live inside the functions that need them, so most of the code is unit-tested with no Airflow, credentials or network.

## Data and scope

- Supabase, MongoDB and S3 hold dummy music data generated by [`seed/generate_seed_data.py`](seed/generate_seed_data.py). PokeAPI is a live public API.
- The S&P 500 list is public domain (ODC-PDDL) from [datasets/s-and-p-500-companies](https://github.com/datasets/s-and-p-500-companies).
- The DAG is triggered manually on purpose. Scheduling is out of scope for this project.

## Known limitations

- **Full snapshot per run.** Every run reloads the whole source. There are no incremental loads or merges.
- **In-memory processing.** Each source is read into a pandas DataFrame on the Airflow worker, so very large tables need a different approach (for example loading through object storage).
- **No schema evolution.** If a source adds or renames a column, publishing into the existing `raw_*` table fails until the table is migrated.
- **Single machine.** `LocalExecutor` shares local temp files between tasks. A multi-worker executor would need shared storage.
- **Basic alerting.** The failure callback only writes to the task log.

## Configuration & secrets

Nothing environment-specific or secret is in the repository.

- [`config/sources.yaml`](config/sources.yaml) describes structure only. Every project id, host, bucket and table name is a `${VAR}` placeholder resolved from the environment when a task runs.
- Credentials are Airflow Connections defined as `AIRFLOW_CONN_*` variables.
- Both live in **`.env`**, which is git-ignored and excluded from the Docker build context. [`.env.example`](.env.example) is the committed template with placeholders only.
- `tests/test_no_secrets.py` runs in CI. It fails if a credential-shaped string is committed, if `.env` or a key file becomes tracked, or if a literal project, bucket or host value appears in the config.

## Adding a source

- **Another source of an existing type:** add a block to `config/sources.yaml`.
- **A new kind of source** (for example Azure Blob or GCS): write one function in [`src/ingest/extractors/__init__.py`](src/ingest/extractors/__init__.py), register it in `EXTRACTORS`, and list its required options in `REQUIRED_OPTIONS` in [`config.py`](src/ingest/config.py).

## Tests & CI

The test suite covers transforms, quality rules, config validation, every extractor (with mocked hooks), the
pipeline steps (with a mocked BigQuery layer), DAG integrity (parses cleanly, one correctly ordered
task group per configured source, retries set, asset outlets present) and the secret guard rails.
[GitHub Actions](.github/workflows/ci.yml) runs lint and tests on every push and pull request.

The cloud paths (BigQuery, Supabase, MongoDB, S3) were also exercised end to end in a real run, shown above.

```bash
AIRFLOW_VERSION=$(sed -n 's/^ARG AIRFLOW_VERSION=//p' Dockerfile | head -1)
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt -r requirements-dev.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.13.txt"
.venv/bin/pytest -q
```

## License

MIT. See [LICENSE](LICENSE).

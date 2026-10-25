# Multi-source ingestion with Apache Airflow → BigQuery

One config-driven Airflow DAG that moves data from **five different kinds of source** into
**BigQuery**, with retries, an idempotent load, a data-quality gate and an audit trail.

It is a single manually-triggered DAG on purpose: the goal is to show the *pattern* working end to
end, not to run on a cron. Run it, watch the graph go green.

```
                          ┌──────────────── per source (identical task group) ────────────────┐
 prepare_destination ───▶ │ extract ─▶ transform ─▶ load_staging ─▶ quality_check ─▶ publish │ ───▶ cleanup_local_files
 (dataset + audit table)  └────────────────────────────────────────────────────────────────────┘
```

| Source | Kind | What it demonstrates |
|---|---|---|
| PokeAPI | Public REST API | Following `next` links across pages; public, so no credentials (`base_url` in config). Authenticated APIs use `conn_id` + `HttpHook`. |
| Supabase | Postgres | `PostgresHook`, a join in the source query |
| MongoDB Atlas | Documents | Flattening nested sub-documents, `ObjectId` handling |
| Amazon S3 | Object storage | `S3Hook` reading a Parquet object (also works with S3-compatible stores such as MinIO) |
| Public CSV over HTTPS | S&P 500 constituents (public domain) | No credentials; plain HTTP + pandas, header cleanup and typed casts |

Adding a source = adding a block to [`config/sources.yaml`](config/sources.yaml). No DAG changes.
A brand-new *kind* of source (e.g. Azure Blob, GCS) is one small function in
[`src/ingest/extractors/__init__.py`](src/ingest/extractors/__init__.py), one entry in `EXTRACTORS`, and its
required options in `REQUIRED_OPTIONS` ([`config.py`](src/ingest/config.py)). That is exactly how S3 was added.

## Requirements

| Tool | Version | Used for |
|---|---|---|
| Docker + Docker Compose v2 | any recent | running Airflow locally |
| Git | any | cloning |
| Python + [uv](https://docs.astral.sh/uv/) | 3.13 | only for running the tests outside Docker |
| Accounts (free tiers) | — | Google Cloud (BigQuery), Supabase, MongoDB Atlas, AWS (S3) |

About 3 GB of free RAM while Airflow runs.

## What each step does

| Step | Purpose |
|---|---|
| `extract` | Calls the source and writes the rows to a local Parquet file. |
| `transform` | Light, deterministic cleanup: snake_case columns, per-source `drop`/`rename`/`cast`, nested values → JSON strings, plus `_source`, `_run_id`, `_ingested_at`, `_load_date` columns. Never changes the row count. |
| `load_staging` | Loads the Parquet into `stg_<source>` (replaced each run, auto-expires after 7 days). |
| `quality_check` | Compares row counts (extracted → transformed → in BigQuery) and checks key columns for NULLs and duplicates. Writes a row to `ingestion_audit` either way. A failure stops the group **without retrying**, because bad data won't fix itself. |
| `publish` | Overwrites that day's partition of `raw_<source>` from staging. |
| `cleanup_local_files` | Removes the run's temp files (runs even if something failed). |

### Engineering choices worth calling out

- **Idempotent load** — `publish` replaces the `_load_date` partition (`WRITE_TRUNCATE` on a partition decorator) rather than appending, so re-running a task or the whole DAG never duplicates rows.
- **Staging before publishing** — nothing reaches the `raw_*` tables until it passes the quality gate.
- **Retries** — every task retries twice; the quality check is deliberately non-retryable (`AirflowFailException`).
- **No secrets in code** — everything sensitive is an Airflow Connection (`AIRFLOW_CONN_*` in `.env`, git-ignored). The config only names connection ids.
- **Assets** — each `publish` task declares an Airflow Asset, so the lineage is visible in the UI.
- **Testable core** — the pipeline logic is plain Python in [`src/ingest`](src/ingest) with Airflow imports kept inside the functions that need them, so most of it is unit-tested with no Airflow, credentials or network.

## Configuration & secrets

Nothing environment-specific or secret is in the repository.

- [`config/sources.yaml`](config/sources.yaml) describes *structure only*. Every project id, host, bucket and table name is a `${VAR}` placeholder resolved from the environment when a task runs.
- Credentials are Airflow Connections defined as `AIRFLOW_CONN_*` variables.
- Both live in **`.env`**, which is git-ignored and excluded from the Docker build context. [`.env.example`](.env.example) is the committed template with placeholders only.
- `tests/test_no_secrets.py` runs in CI and fails if a credential-shaped string is committed, if `.env` or a key file becomes tracked, or if a literal project/bucket/host value appears in the config.

## Tests & CI

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python "apache-airflow==3.3.2" -r requirements.txt -r requirements-dev.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt
.venv/bin/pytest -q
```

The suite covers transforms, quality rules, config validation, every extractor (with mocked hooks),
the pipeline steps (with a mocked BigQuery layer), and DAG integrity (parses cleanly, one correctly
ordered task group per configured source, retries set, asset outlets present).
[GitHub Actions](.github/workflows/ci.yml) runs lint + tests on every push and pull request.

## Working notes (temporary — to be removed before the repo goes public)

### What I need from you

1. ~~Create the GitHub repo~~ — done. The public-CSV source needs no setup.
2. Run `./scripts/init-env.sh`, then put **all** your real values in `.env` (git-ignored): GCP project id, Supabase,
   Mongo, S3 bucket and keys — see `.env.example` for every variable. Put the Google
   service-account key at `secrets/gcp-sa.json` (also git-ignored; roles: BigQuery Data Editor + BigQuery Job User).
   Never edit real values into `config/sources.yaml`; the tests will fail if you do.
3. Supabase: the source query currently matches the seed schema in `seed/supabase_seed.sql` (table names are not secrets, so editing that query is fine).
4. Create the S3 bucket + an IAM user with read access, and upload the seed Parquet file (see below).

### Not yet verified

Everything that needs real credentials (BigQuery load / quality check / publish, Supabase,
MongoDB, S3) is only covered by mocked unit tests. The first real run may need small fixes.

## Run it yourself

Everything below uses free tiers; see [`.env.example`](.env.example) for each connection.

1. **Secrets and connections** — `./scripts/init-env.sh`, then fill in `.env`. Put the Google service-account key at `secrets/gcp-sa.json`.
2. **Seed the demo data** (all dummy data, generated by [`seed/generate_seed_data.py`](seed/generate_seed_data.py)):
   - Supabase: run [`seed/supabase_seed.sql`](seed/supabase_seed.sql) in the SQL editor
   - MongoDB: `MONGO_URI=... python seed/load_mongo.py`
   - S3: upload [`seed/s3_song_streams.parquet`](seed/s3_song_streams.parquet) to your bucket at `S3_OBJECT_KEY`
     (e.g. `aws s3 cp seed/s3_song_streams.parquet s3://$S3_BUCKET/ingest-demo/song_streams.parquet`)
   - Public CSV over HTTPS: nothing to do
3. **Start Airflow**
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```
   Open http://localhost:8080 (login from `.env`, default `airflow` / `airflow`), unpause `ingest_all_sources` and trigger it.

Airflow needs roughly 3 GB of RAM while running. `docker compose down` stops it (`-v` also wipes its metadata DB).

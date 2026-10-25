# Airflow multi-source to BigQuery

[![CI](https://github.com/safakatakancelik/airflow-multi-source-to-bigquery/actions/workflows/ci.yml/badge.svg)](https://github.com/safakatakancelik/airflow-multi-source-to-bigquery/actions/workflows/ci.yml)

Config-driven Airflow DAG that ingests different source types (REST API, Postgres, MongoDB, S3, CSV) into BigQuery. Idempotent loads, data-quality gate, audit table, tests and CI.

The focus of this project is moving data using Airflow across different sources.

## Demo video

[![Demo video: Airflow DAG ingesting five source types into BigQuery](https://img.youtube.com/vi/vZmEvGCyg8U/maxresdefault.jpg)](https://youtu.be/vZmEvGCyg8U)

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

## Setup guide

Everything here uses free tiers. Allow about 45 minutes the first time. Run every `docker compose` command from the repository root.

### 1. Clone and create `.env`

```bash
git clone https://github.com/safakatakancelik/airflow-multi-source-to-bigquery.git
cd airflow-multi-source-to-bigquery
./scripts/init-env.sh
```

`init-env.sh` copies [`.env.example`](.env.example) to `.env` and generates the local Airflow secrets and the Airflow UI password (printed once, also stored in `.env`). Every credential in the next steps goes into `.env`, which is git-ignored.

### 2. Google Cloud (BigQuery)

1. Create a Google Cloud project, or pick an existing one. The BigQuery API is enabled by default on new projects.
2. **IAM & Admin → Service accounts → Create service account.** Grant it two roles: **BigQuery Data Editor** (creates the dataset and tables and writes to them) and **BigQuery Job User** (runs load and query jobs).
3. Open the service account → **Keys → Add key → Create new key → JSON**. Save the file as `secrets/gcp-sa.json`. The `secrets/` folder is git-ignored and mounted read-only into the containers.
4. In `.env`, set `GCP_PROJECT_ID` and the same project id inside `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` (the `project` field).

The pipeline stores a few hundred rows and runs a handful of small queries, well inside BigQuery's monthly free tier.

### 3. Supabase (PostgreSQL)

1. Create a project and choose a **database password**. This is not your Supabase account password. If you forget it, reset it under *Project Settings → Database*.
2. Open the **SQL Editor**, paste the contents of [`seed/supabase_seed.sql`](seed/supabase_seed.sql) and run it. It creates `genres` (8 rows), `artists` (20) and `songs` (100).
3. Click **Connect** and copy the **Session pooler** details. Use the pooler, not the direct connection: the direct host (`db.<ref>.supabase.co`) only resolves over IPv6, which Docker usually can't reach.
4. Fill in `AIRFLOW_CONN_SUPABASE_DEFAULT` in `.env`:

   | Field | Value |
   |---|---|
   | `host` | the pooler host, like `aws-0-REGION.pooler.supabase.com` |
   | `login` | `postgres.<project-ref>` (with the dot and the project ref, not just `postgres`) |
   | `password` | the database password from step 1 |
   | `port`, `schema` | `5432`, `postgres` |

### 4. MongoDB Atlas

1. Sign in at [cloud.mongodb.com](https://cloud.mongodb.com) and create a free **M0** cluster. No payment method is needed.
2. **Security → Database & Network Access → Database Users → Add New Database User.** Choose password authentication with the role *Read and write to any database*. Use letters and digits only so the password needs no URL-encoding.
3. **IP Access List → Add IP Address → Add Current IP Address.** Your IP changes between networks, so redo this when you move.
4. Click **Connect → Drivers** on the cluster and copy the host, like `cluster0.xxxxx.mongodb.net`.
5. Load the 30 playlist documents (the loader replaces the collection, so it is safe to re-run):

   ```bash
   MONGO_URI='mongodb+srv://USER:PASS@cluster0.xxxxx.mongodb.net/' uv run --with pymongo python seed/load_mongo.py
   ```

   It prints `Loaded 30 documents into ingest_demo.playlists`. Run this **before** the first DAG run, otherwise the Mongo source finds nothing to ingest.
6. Fill in `AIRFLOW_CONN_MONGO_DEFAULT` in `.env`. The `host` is only the cluster host, without `mongodb+srv://` and without credentials. Keep `"extra":{"srv":true}`.

### 5. Amazon S3

The S3 source reads one Parquet file. Airflow authenticates as an IAM user, and that user must be allowed to read the file. The policy below gives it read access to a single bucket and nothing else, so a leaked key cannot write, delete or touch any other bucket.

1. **S3 → Create bucket.** Choose a globally unique name and a region, and keep *Block all public access* on.
2. Upload [`seed/s3_song_streams.parquet`](seed/s3_song_streams.parquet) to the path `ingest-demo/song_streams.parquet` inside the bucket:

   ```bash
   aws s3 cp seed/s3_song_streams.parquet s3://YOUR_BUCKET/ingest-demo/song_streams.parquet
   ```

   The web console's *Upload* button works too. To use another path, set `S3_OBJECT_KEY` in `.env`.
3. **IAM → Policies → Create policy → JSON**, replacing `YOUR_BUCKET`:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::YOUR_BUCKET"},
       {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::YOUR_BUCKET/*"}
     ]
   }
   ```

   There are two statements because the two permissions apply to different resources: `s3:ListBucket` applies to the bucket itself, and `s3:GetObject` applies to the objects inside it (`/*`). Without `ListBucket`, S3 answers a request for a missing file with `AccessDenied` instead of `NoSuchKey`, which hides typos in the path. The managed `AmazonS3ReadOnlyAccess` policy also works, but it grants read access to every bucket in the account.
4. **IAM → Users → Create user** (no console access), attach the policy, then **Security credentials → Create access key → Application running outside AWS**. Copy the key id and secret when they are shown, because the secret cannot be displayed again.
5. In `.env`, set `S3_BUCKET` and fill in `AIRFLOW_CONN_AWS_DEFAULT` with `login` = access key id, `password` = secret key and `region_name` = the bucket's region.

### 6. PokeAPI and the public CSV

Nothing to set up. PokeAPI needs no credentials, and the CSV is fetched from a public URL.

### 7. Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow reads `.env` when its containers start. After you change `.env`, apply it with:

```bash
docker compose up -d --force-recreate
```

### 8. Run it and check the result

1. Open http://localhost:8080 and sign in as `airflow` with the password from step 1.
2. DAGs are paused when created. Unpause `ingest_all_sources`, then click **Trigger**. All 27 tasks should turn green.
3. In BigQuery, the dataset `ingest_demo` should now hold `stg_<source>` and `raw_<source>` tables for each source, plus `ingestion_audit`. Expected row counts: PokeAPI 150, Supabase 100, MongoDB 30, S3 200, CSV about 500.

   ```sql
   SELECT source, status, rows_extracted, rows_staged, failures
   FROM `YOUR_PROJECT.ingest_demo.ingestion_audit`
   ORDER BY recorded_at DESC;
   ```

### 9. Tear down

Stop Airflow and remove its containers, volumes and images:

```bash
docker compose down -v --rmi all --remove-orphans
```

Then delete what you created in each service so nothing stays active: the S3 bucket and the IAM access key, user and policy; the Atlas database user, IP entry and cluster; the Supabase project; the Google Cloud service account and key, and the `ingest_demo` dataset.

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

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `no configuration file provided: not found` | `docker compose` was run outside the repository root. `cd` into it first. |
| A change to `.env` has no effect | Containers read `.env` only when they start. Run `docker compose up -d --force-recreate`. |
| `The conn_id ... isn't defined` | The matching `AIRFLOW_CONN_<ID>` line is missing from `.env`, or its name is misspelled. A public API needs no connection: set `base_url` in `config/sources.yaml` instead. |
| The DAG is missing, or shows an import error | Run `docker compose exec airflow-scheduler airflow dags list-import-errors`. |
| The DAG does not start | New DAGs are paused. Unpause it, then trigger it. |
| Supabase connection fails | Use the Session pooler host, not the IPv6-only direct host. The login is `postgres.<project-ref>`, and the password is the database password, not the account password. |
| MongoDB: `SSL handshake failed ... TLSV1_ALERT_INTERNAL_ERROR` on every server | Atlas is refusing the connection before it checks credentials. This typically means your current IP is not in the Atlas IP Access List. Add it, and make sure `host` has no `mongodb+srv://` prefix. |
| MongoDB source fails with 0 rows | The collection is empty. Run `seed/load_mongo.py` first. |
| S3 `AccessDenied` | The IAM policy is missing `s3:ListBucket` or `s3:GetObject`, names the wrong bucket, or the object path is wrong. Without `ListBucket`, a wrong path also shows as `AccessDenied`. |
| S3 `NoSuchKey`, or a region error | Check that `S3_OBJECT_KEY` matches the uploaded path exactly and that `region_name` matches the bucket's region. |
| BigQuery `403` or permission errors | The service account lacks *BigQuery Data Editor* or *BigQuery Job User*, or `GCP_PROJECT_ID` differs from the `project` inside the connection. |
| A `quality_check` task fails and is not retried | This is intended. Read the `failures` column in `ingestion_audit` for the reason (row-count mismatch, NULL or duplicate keys). |
| Where are the task logs? | In the Airflow UI under the task's *Logs* tab, and in the local `logs/` folder. |

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

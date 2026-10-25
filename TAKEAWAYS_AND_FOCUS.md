# Takeaways & focus

Personal study notes for this project. Kept up to date as the project changes (see the log at the bottom).
Not part of the reviewer-facing README. Decide before going public whether to keep, trim or remove this file.

---

## 1. Key learnings to note down

### Airflow concepts
- **A DAG is just Python.** Airflow's dag-processor reads files in `dags/`. The YAML here is *our* addition: the DAG file reads it and generates tasks from it. Airflow never sees the YAML.
- **`schedule=None` means nothing starts the DAG.** You trigger it (UI button or `airflow dags trigger`). `>>` only defines *order after it starts*.
- **TaskFlow (`@task`)**: a function becomes a task. Calling `b(a())` creates the dependency *and* passes `a`'s return value to `b` through XCom.
- **Task groups** are a UI/organisation feature (a box in the Graph). Dependencies still work between groups (`prepare >> group >> cleanup`).
- **XCom is for small metadata only.** Here each step passes a small "manifest" dict (paths, row counts). The data itself travels as Parquet files, then through BigQuery.
- **Assets** (`outlets=[Asset(...)]`) record "this task produces this table". The Assets page graph shows lineage, not a schedule. Nothing is scheduled unless another DAG is triggered by an asset.
- **Audit Logs / Jobs pages** show Airflow's own activity (user actions, scheduler/dag-processor heartbeats), not your DAG runs. DAG runs are under the DAG's Runs tab.
- **LocalExecutor** runs tasks as processes inside the scheduler container: no Redis or workers. Fine for one machine; local temp files only work because of this (see Focus Q on scaling).
- **Airflow 3 specifics we hit:** `logical_date` can be `None` for manual runs (we fall back to `run_after`); `DagBag` has a new signature; imports come from `airflow.sdk`.

### Data engineering patterns used
- **Idempotent load**: write to staging, then *overwrite* the day's partition (`WRITE_TRUNCATE` on `table$YYYYMMDD`). Re-running never duplicates rows. Append-only loads are the usual source of duplicates.
- **Stage → check → publish**: bad data never reaches the `raw_*` tables.
- **Quality failures are not retried** (`AirflowFailException`): retrying cannot fix bad data. Transient errors (network) are retried.
- **Audit table**: every check writes pass/fail and counts to `ingestion_audit`, even when it fails.
- **Config-driven**: adding a source is a YAML block; a new *kind* of source is one function + registry entries (this is exactly how S3 was added).
- **Pure core, thin Airflow wrapper**: logic in `src/ingest`, Airflow imports inside functions, so most of it is unit-testable without Airflow or credentials.

### Practical lessons
- **Check an API's terms before building on it.** Remotive required attribution and banned redistribution, so it was swapped for PokeAPI.
- **Free-tier traps:** Supabase's direct DB host is IPv6-only, so use the session pooler; Google service accounts need explicit roles; S3 needs an AWS account.
- **pandas 3 changes behaviour:** missing values are `NaN` not `None`; the default string dtype is `str`. Tests must use `pd.isna`.
- **Parquet timestamps:** coerce to microseconds before loading into BigQuery.
- **Mocked tests ≠ working integration.** 61 tests pass, but BigQuery/Supabase/Mongo/S3 paths are only proven by a real run. Expect first-run fixes.
- **Public-repo hygiene:** audit history (not just current files), git-ignore `.env`/keys, keep the config to `${VAR}` placeholders, and add a test that fails on leaks. Security that isn't automated decays.
- **Tooling gotchas:** a push touching `.github/workflows` needs the token's *Workflows* permission; reading CI results needs *Actions/Checks: Read*. `git checkout <file>` throws away *uncommitted* edits too.
- **Commit identity** is public: check `user.name` / `user.email` before publishing.

---

## 2. Focus: what to understand in depth to own this project

Be able to explain each of these without looking at the code. Roughly ordered by how likely they are to be asked.

### A. Design decisions ("why did you…")
1. **Why Airflow, not cron, dbt, Fivetran or Databricks Workflows?** Airflow = orchestration with dependencies, retries, history, UI. dbt only transforms inside a warehouse; Fivetran is a paid connector product; Databricks Workflows only orchestrates inside Databricks. The honest answer: Airflow is the right tool when you coordinate *many heterogeneous systems*.
2. **Why staging then publish?** So a failed or wrong load never corrupts the published table, and the quality gate has something to inspect.
3. **Why partition overwrite instead of MERGE/append?** Simple, atomic per day, naturally idempotent. Trade-off: it's snapshot-style, not incremental, and a late fix to a past day needs a re-run of that date.
4. **Why one config-driven DAG instead of one DAG per source?** Less duplication, one graph, one run proves all sources; trade-off: one source's parse error affects the whole DAG.
5. **Why Parquet files between steps instead of XCom?** XCom lives in the metadata DB and is meant for small values.

### B. Real problems that happen at work (and what this repo does or doesn't do)
| Problem | Question you'll get | This repo today |
|---|---|---|
| **Schema drift** (source adds/renames a column) | "What happens when a column changes?" | Staging replaces itself, but `publish` writes into an existing table with the old schema, so it can fail. No schema evolution yet. Know this limitation and the fix (`ALLOW_FIELD_ADDITION`, or explicit column lists). |
| **Duplicate runs / retries** | "Can I safely re-run?" | Yes, per-date partition overwrite. Audit table logs each attempt (so an audit row per retry is expected). |
| **One source fails** | "Do the others still load?" | Yes: groups are independent; cleanup uses `trigger_rule="all_done"`. The DAG run is red overall. |
| **Large tables** | "What if it's 500M rows?" | `pandas` loads everything in memory and writes a local file, so it doesn't scale. Real fix: push-down (Databricks → GCS → BigQuery load job, or the Spark BigQuery connector), chunked reads, incremental loads by watermark. |
| **Incremental loads / backfills** | "How do you load only new data?" | Full snapshot per run. Know the pattern: watermark column, `data_interval_start/end`, `airflow dags backfill`. |
| **Late-arriving / bad data** | "What if the source sends nulls or duplicates?" | Key checks (null/dup) and row counts; no value-level validation, no quarantine table. |
| **Secrets** | "Where do credentials live? How do you rotate?" | Airflow Connections via env in `.env`. In production: a secrets backend (Secret Manager/Vault), least-privilege roles, rotation. |
| **Alerting** | "How do you know it failed at 3am?" | Failure callback only logs. Real: Slack/email/PagerDuty, SLAs, retry-exhausted alerts. |
| **Scaling the orchestrator** | "LocalExecutor in production?" | No. Know Celery/Kubernetes executors, and that local temp files would break across workers (use shared storage like GCS). |
| **Cost** | "What does this cost on BigQuery?" | Staging tables auto-expire (7 days); load jobs are free, queries cost by bytes scanned; partitioning limits scan size. Set budget alerts. |
| **Time zones / logical date** | "Which date does a manual run write to?" | `logical_date`, falling back to `run_after`, in UTC. A midnight-boundary run can land on the "wrong" local date. |
| **PII / compliance** | "Does any personal data move?" | Demo uses dummy/public data. Know what you'd do: masking, column-level access, regional datasets. |
| **Observability** | "How do you debug a bad run?" | Task logs, the audit table, row counts per stage. |

### C. Airflow-specific depth
- Scheduler vs dag-processor vs api-server: what each does.
- What `catchup`, `max_active_runs`, `retries`, `trigger_rule`, `default_args` do, and why each is set the way it is here.
- XCom size limits and why files are used; what "dynamic task mapping" would change.
- Airflow 3 changes vs 2 (the SDK, `logical_date` optional, assets).
- How Connections resolve (env var `AIRFLOW_CONN_*` → hook).

### D. Testing & CI
- What the unit tests prove, and what they **can't** (real network, real credentials, BigQuery semantics).
- What the DAG-integrity tests catch (import errors, wiring, retries, outlets).
- Why `test_no_secrets.py` exists, and its limit (it only knows credential *formats*, not arbitrary IDs).

### E. Honest limitations to volunteer (it reads as maturity)
No schema evolution · full-snapshot only · single-machine, in-memory · log-only alerting · mocked-only tests for cloud paths until a real run · runs only when triggered.

### F. Practice questions to answer out loud
1. Walk me through what happens from the moment you click Trigger until a row is in BigQuery.
2. A re-run happens at 23:59 and again at 00:01. What's in the tables?
3. The Supabase source adds a column. What breaks, and how do you fix it?
4. How would you make this load only changed rows?
5. How would you run this for 200 sources on a team?
6. Why does the quality check not retry?
7. Where could a credential leak from this repo, and what stops it?
8. What would you change first if this went to production?

---

## 3. Update log

| Date | Change |
|---|---|
| 2026-10-25 | REST source takes `base_url` in config instead of an Airflow Connection (a public API has no secret, and a missing connection caused the first PokeAPI failure). `conn_id` stays for APIs needing auth. Lesson: only use a Connection for things that are secret. |
| 2026-10-25 | Databricks source removed (it moves to another project) and the intentional retry-failure demo switch removed; 5 sources remain. Retries are still configured. |
| 2026-10-25 | Seed data switched to a music theme (genres/artists/songs in Supabase, nested playlists in Mongo, daily song streams as Parquet in S3); SQL verified against real Postgres. |
| 2026-10-25 | Created. Covers: config-driven DAG, S3 replacing Google Sheets, public CSV + Parquet-on-S3 sources, secret-guard tests. |

> **Rule for updating:** whenever the code, design or a decision changes, add a row above and amend the relevant section.

# Orders ETL Pipeline (Airflow + SQLite)

CSV → transform/validate → SQLite warehouse, orchestrated by an Airflow DAG.
Stdlib-only at runtime; Airflow is needed only to schedule the DAG.

## Architecture

```
data/sample_orders.csv
  │ extract (src/pipeline.py::extract) — batch gates: file exists, header
  │   present, required columns present. Row-level problems are NOT fatal here.
  ▼
transform — strip/uppercase, float(amount), ISO-8601 date check, PK numeric,
  customer non-empty, amount > 0, 3-letter currency, known status
  (empty → pending), in-batch dedupe. Bad rows → rejected with reason.
  ▼
validate — required cols, nulls, duplicate PKs. Bad rows → rejected.
  ▼
select_incremental — against ACTUAL target state (see below)
  ▼
load — one BEGIN IMMEDIATE txn: UPSERT on order_id + composite watermark
  cursor (etl_state) committed atomically; stale/identical rows skipped
  ▼
audit/audit_log.csv — one canonical row per task run (auto-migrates old files)
```

DAG `orders_etl` (`dags/orders_etl_dag.py`, `@daily`, retries=3, 5-min delay,
30-min timeout, `catchup=False`, `max_active_runs=1`): tasks
`extract → transform_validate → load` exchange plain-dict payloads via XCom
(JSON-serializable); each task writes its own audit row (incl. failures), so
a failed `extract` is still auditable. Retries are safe: extract/transform are
deterministic re-reads, load is an idempotent UPSERT.

## Setup & run

```powershell
pip install -r requirements.txt   # only for Airflow; pipeline + tests are stdlib-only
python -m unittest discover -s tests -v
python dags/orders_etl_dag.py     # local run without Airflow
```

Airflow deployment (≥2.9): point `AIRFLOW__CORE__DAGS_FOLDER` at `./dags`
(or copy the file there), unpause `orders_etl`. No connections needed
(local CSV/SQLite). For larger-than-memory batches, replace XCom payloads
with a staging table/object store — noted, not implemented.

## Configuration (all via env, see `.env.example`)

`ETL_SOURCE_CSV`, `ETL_TARGET_DB`, `ETL_WATERMARK_FILE`, `ETL_AUDIT_FILE`
(defaults in `config.py`); `ETL_FULL_LOAD=1` forces full reload on next run.
No secrets exist in this backend; keep credentials in env/secrets manager.

## Incremental loading

Selection compares the validated batch against target state, ordered
deterministically by `(updated_at, order_id)` (PK breaks timestamp ties):

- unseen PK → load (new **and late-arriving** rows, any timestamp)
- changed content → load (corrections/updates, any timestamp)
- identical to stored → skip (reruns/overlaps are no-ops, `processed=0`)
- fast path: `updated_at > watermark` loads without further checks

No lookback window is needed: state comparison subsumes timestamp windows,
and overlapping batches stay idempotent by construction. Out-of-order rows
older than stored state are counted `skipped_stale` and never clobber newer
data. The cursor is composite `(max updated_at, order_id at that ts)`,
committed inside the load transaction (`etl_state` table); the JSON file is a
mirror for observability (read prefers DB, falls back to file).
Deletes: out of scope — upsert-only, no tombstones in a CSV snapshot.

## Data quality & rejection rules

Batch-fatal (whole run fails, audited): missing file, missing header,
missing required columns, DB/transaction errors. Source is read as UTF-8
with BOM stripped, so Excel-style BOM exports load normally. Row-level
(rejected with reason, run continues): non-numeric/duplicate-in-batch
`order_id`, empty customer, `amount ≤ 0` or non-numeric, bad currency,
bad ISO date, unknown status; plus validate-stage nulls and duplicate PKs.
Empty file (header only) succeeds with zero counts; a 0-byte file fails as
"no header row" (indistinguishable from truncation). Invalid data never
reaches the target: rejected rows are excluded before selection, and the
load transaction rolls back wholly on any error.

## Audit log columns

`run_id, dag_id, task_id, started_at, completed_at, status, source, target,`
`extracted, processed, inserted, updated, skipped_unchanged, skipped_stale,`
`rejected, error, attempt` (`attempt` = Airflow try_number; `run_id` =
`dag_run.run_id` under Airflow, generated otherwise). Failures keep the
original exception chain (`RuntimeError(...) from e`).

## Tests

`python -m unittest discover -s tests -v` → 23 tests OK: original 8
(happy path, rejections, incremental filter, idempotency, empty/missing
input, watermark) + late arrivals, updates, stale out-of-order guard,
duplicate-timestamp ordering, overlap idempotency, rollback with watermark
preservation, batch-level header failure, audit schema/counts, DB-over-file
watermark precedence, 2-thread concurrent runs, BOM-prefixed source,
mirror-crash self-heal, and DAG structure (tasks, deps, retries/timeouts,
`max_active_runs`) plus an XCom end-to-end run via stubbed `airflow` modules.

## Verified (this environment, 2026-10-16)

- 23/23 tests pass; local DAG run: extracted=8, processed=6→0 on rerun,
  inserted=6, rejected=2; `etl_state` = `2026-10-07T12:00:00`/pk 8.
- Separate-process concurrency: 3 concurrent `run_etl` processes against one
  SQLite DB — all exited 0, final row count exact, no duplicate PKs, cursor
  correct (`BEGIN IMMEDIATE` serializes writers; UPSERT absorbs the overlap).
- Crash between DB commit and mirror write (simulated): data + DB cursor
  intact, rerun self-heals with `processed=0` and restores the mirror.
- XCom payload verified JSON round-trip safe (tuple cursor → list, restored
  via `tuple()` in `t_load`).
- Airflow was NOT run live (not installed; Airflow has no Windows support —
  deploy via Linux/Docker): API surface (`schedule=`, `max_active_runs`,
  `default_args execution_timeout`, `airflow.operators.python`) checked
  compatible with 2.9–3.x by inspection, DAG passes stub-import structural
  checks, XCom path unit-tested. Live-scheduler execution remains unverified.

## Deployment checklist (prioritized)

1. Deploy the DAG on Linux/Docker with `apache-airflow>=2.9,<4`; unpause
   `orders_etl`; confirm one live scheduled run + one retry (kill a task
   mid-run) before relying on it.
2. Keep single-writer (`max_active_runs=1`, `catchup=False`): cross-process
   audit migration (read-modify-write on header mismatch) and the audit
   lock are same-process only — safe under one scheduler, not under
   concurrent writers.
3. Monitor: alert on audit `status=failed`, on `rejected > 0` spikes, and on
   runs where `processed=0` for an unexpectedly long stretch (upstream stall).
   The audit CSV is the current sink — ship it to your log aggregator.
4. Restrict file permissions on `warehouse.db`, `state/`, `audit/` on shared
   hosts (created with default umask); credentials (when a real warehouse
   replaces SQLite) go in env/secrets manager, never in code.
5. Scale-out staging: XCom carries the whole batch (fine here). Past ~10k
   rows/task or multi-MB payloads, write the validated batch to a staging
   table/file in `transform_validate` and pass only `{run_id, batch_ref}`
   via XCom; `load` then reads the staging ref.
6. SQLite → Postgres/BigQuery when leaving single-node: keep the cursor in
   the same warehouse transaction (already the pattern via `etl_state`).

# Orders ETL Pipeline (Python + SQL + Airflow)

CSV → validate/transform → SQLite (default) or PostgreSQL, orchestrated by an
Airflow DAG. Stage logic is plain Python + SQL with no ORM; the pipeline runs
on stdlib alone with SQLite and needs only `psycopg` for PostgreSQL.

## Architecture

```
data/sample_orders.csv
  │ src/extract.py — batch gates: file exists, header present, required
  │   columns present, BOM-tolerant read. Row problems are NOT fatal here.
  ▼
src/transform.py — strip/uppercase, float(amount), ISO-8601 check, PK numeric,
  customer non-empty, amount > 0, 3-letter currency, known status
  (empty → pending), in-batch dedupe. Bad rows → rejected with reason.
  ▼
src/validate.py — required cols, nulls, duplicate PKs. Bad rows → rejected.
  ▼
src/load.py::select_incremental — against ACTUAL target state (see below)
  ▼
src/load.py::load — one transaction: UPSERT on order_id + composite watermark
  cursor (etl_state) committed atomically; stale/identical rows skipped
  ▼
src/audit.py — one canonical row per task run → CSV log + etl_audit table
```

`src/database.py` owns the backend abstraction (`ETL_DB_BACKEND=sqlite |
postgres`): connection handling, `:named` → `%(name)s` translation, and the
`sql/` DDL scripts shared by both backends. `src/pipeline.py::run_etl` wires
the stages for local runs and tests. The DAG (`dags/orders_etl_dag.py`,
`@daily`, retries=3, 5-min delay, 30-min timeout, `catchup=False`,
`max_active_runs=1`) runs `extract → transform → load → audit`, exchanging
small plain-dict payloads via XCom; each task writes its own failure audit
row, so a failed `extract` is still auditable. Retries are safe:
extract/transform are deterministic re-reads, load is an idempotent UPSERT,
audit rows are idempotent on `(run_id, task_id)`.

Pandas is deliberately not used: rows stream through the stages, so memory
stays proportional to the batch; loading the whole dataset into a DataFrame
would contradict the scalability constraint below. Likewise psycopg (DBAPI)
is used instead of SQLAlchemy — the two backends share one small SQL
surface, and an ORM would add churn without benefit here.

## Project layout

```
dags/orders_etl_dag.py      Airflow DAG (4 tasks, 2.x/3.x compatible imports)
src/extract|transform|validate|load|audit|database|pipeline.py
sql/create_tables.sql       orders + etl_state (runs on SQLite AND PostgreSQL)
sql/create_audit_table.sql  etl_audit (same)
tests/                      unittest suite (stdlib only; PG tests skip if offline)
config.py / .env.example    env-based config, no secrets in code
docker-compose.yaml         optional local PostgreSQL (port 5434)
data/sample_orders.csv      8-row fixture (2 bad rows)
```

## Setup & run

Prerequisites: Python 3.10+. SQLite path needs nothing else.

```powershell
# 1. default SQLite run (zero dependencies)
python -m unittest discover -s tests -v
python dags/orders_etl_dag.py

# 2. PostgreSQL via compose (needs Docker daemon running)
docker compose up -d postgres
$env:ETL_DB_BACKEND='postgres'; $env:ETL_PG_PORT='5434'
$env:ETL_PG_DBNAME='etl'; $env:ETL_PG_USER='etl'; $env:ETL_PG_PASSWORD='etl'
pip install "psycopg[binary]"
python -m unittest discover -s tests -v   # PG tests run when reachable
docker compose down -v

# 3. Airflow (Linux/Docker/WSL2 only — Airflow has no Windows support)
#    point AIRFLOW__CORE__DAGS_FOLDER at ./dags (Airflow 2.9–3.x), unpause orders_etl.
```

Append new rows to `data/sample_orders.csv` with newer `updated_at` and
re-run: only new rows load (`processed` counts just them, no dup growth).

## Configuration (env, see `.env.example`)

`ETL_SOURCE_CSV`, `ETL_TARGET_DB`, `ETL_WATERMARK_FILE`, `ETL_AUDIT_FILE`,
`ETL_FULL_LOAD=1` (force full reload), `ETL_DB_BACKEND=sqlite|postgres`,
`ETL_PG_HOST/PORT/DBNAME/USER/PASSWORD` (password read only at connect time,
never logged; audit `target` records `postgresql://host:port/dbname` only).

## Incremental loading

Selection compares the validated batch against target state, ordered
deterministically by `(updated_at, order_id)` (PK breaks timestamp ties):

- unseen PK → load (new **and late-arriving** rows, any timestamp)
- changed content → load (corrections/updates, any timestamp)
- identical to stored → skip (reruns/overlaps are no-ops)
- fast path: `updated_at > watermark` loads without further checks

No lookback window: state comparison subsumes timestamp windows, and
overlapping batches stay idempotent by construction. Out-of-order rows older
than stored state are counted `skipped_stale` and never clobber newer data.
The cursor is composite `(max updated_at, order_id at that ts)`, committed
inside the load transaction (`etl_state` table); the JSON file is a mirror
(read prefers DB, falls back to file). Failed runs change nothing: the
transaction rolls back, the cursor is untouched, the audit row says `failed`,
and the next run reprocesses from the last committed cursor — late rows are
picked up because selection never relies on timestamps alone.
Deletes are out of scope — upsert-only, no tombstones in a CSV snapshot.

## Data quality, errors, audit

Batch-fatal (whole run fails, audited `failed`, original exception chained):
missing file, missing header, missing required columns, DB/transaction
errors. Row-level (rejected with reason, run continues): non-numeric or
duplicate-in-batch `order_id`, empty customer, `amount ≤ 0` or non-numeric,
bad currency, bad ISO date, unknown status; plus validate-stage nulls and
duplicate PKs. Header-only file succeeds with zero counts; 0-byte file fails
as "no header row". Task retries (3×, 5-min delay) cover transient failures;
rejected rows are data, never a task failure — the two are audited
separately (`rejected` count vs `status=failed`).

Audit columns (CSV log + `etl_audit` table, same schema): `run_id, dag_id,
task_id, started_at, completed_at, status, source, target, extracted,
processed, inserted, updated, skipped_unchanged, skipped_stale, rejected,
error, attempt` (`attempt` = Airflow try_number; `run_id` = `dag_run.run_id`
under Airflow, generated otherwise).

## Tests

`python -m unittest discover -s tests -v` — SQLite suite always runs;
`tests/test_postgres.py` runs against PostgreSQL when reachable (isolated
database per test, created from `postgres` admin DB) and skips otherwise.

## Verified (this environment, 2026-10-16)

- SQLite: 26/26 pass (3 PG tests skip offline); local DAG run
  extracted=8 → inserted=6, rejected=2, rerun `processed=0`.
- PostgreSQL 18 (via `docker compose up`, PGDATA fix applied): 3/3 PG
  backend tests pass — full→incremental→update→late-arrival→stale guard,
  CSV/DB audit parity, rollback with cursor preserved. Manual demo on a
  scratch DB: full `extracted=8, processed=6, inserted=6, rejected=2`,
  rerun `processed=0`; `etl_state=(2026-10-07T12:00:00, pk 8)`; `etl_audit`
  holds one row per run. Separate-process concurrency (3× `run_etl`, prior
  assessment): all rc=0, no dup PKs.
- Crash between DB commit and mirror write (simulated): data + DB cursor
  intact, rerun self-heals with `processed=0` and restores the mirror.
- XCom payload verified JSON round-trip safe (tuple cursor → list, restored
  via `tuple()` in `t_load`).
- Airflow was NOT run live (installed 3.3.2 copy on this Windows box is
  itself broken — `from airflow import DAG` fails inside the package, so the
  DAG correctly degrades to local-run mode; Airflow has no Windows support
  regardless). Compatibility with 2.9–3.x established by dual-path imports
  (`providers.standard` → `operators.python` fallback) + stub-import
  structural tests + XCom unit run only.
- `docker-compose.yaml` executed here for the Postgres service (fresh
  container, healthy, then torn down); Airflow services intentionally not
  included — scheduler runs on Linux/Docker/WSL2.

## Troubleshooting

- `source missing required columns ...` → header typo or BOM-less mismatch;
  BOM-prefixed files are handled.
- `database is locked` (SQLite) → another writer holds the DB; single-writer
  only (`max_active_runs=1`); PostgreSQL removes this limit.
- PG `connection failed` → daemon/container down, wrong `ETL_PG_PORT`
  (compose maps 5434), or wrong password (compose default `etl`).
- `ETL_DB_BACKEND=postgres` + `RuntimeError: ... requires psycopg` →
  `pip install "psycopg[binary]"`.
- Airflow DAG not appearing → `DAGS_FOLDER` path, `.py` syntax
  (`python -m py_compile dags/orders_etl_dag.py`), Airflow ≥2.9 on Linux.

"""Airflow DAG: extract -> transform_validate -> load.

Each task pushes/pulls plain-dict payloads via XCom (JSON-serializable), so
tasks work across processes under a real scheduler. The module-global TI is
only a fallback for `python dags/orders_etl_dag.py` local runs.
Importing this module never runs the pipeline (no calls at module scope).
"""
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as cfg
from src.pipeline import (extract, fetch_existing, load, read_watermark,
                          select_incremental, transform, validate, write_audit,
                          write_watermark)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:  # pragma: no cover - allows local run/test without Airflow
    DAG = PythonOperator = None
    AIRFLOW_AVAILABLE = False

DEFAULT_ARGS = {"owner": "data-eng", "retries": 3, "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(minutes=30), "email_on_failure": False}

TI = {}  # local-run fallback only; never used when a real TaskInstance exists


def _push(ctx, task, key, value):
    ti = (ctx or {}).get("ti")
    if ti is not None and hasattr(ti, "xcom_push"):
        ti.xcom_push(key=key, value=value)
    else:
        TI[(task, key)] = value


def _pull(ctx, task, key, default=None):
    ti = (ctx or {}).get("ti")
    if ti is not None and hasattr(ti, "xcom_pull"):
        return ti.xcom_pull(task_ids=task, key=key)
    return TI.get((task, key), default)


def _run_id(ctx):
    dr = (ctx or {}).get("dag_run")
    if dr is not None and getattr(dr, "run_id", None):
        return dr.run_id
    rid = TI.get(("__ctx", "run_id"))
    if not rid:
        rid = uuid.uuid4().hex[:12]
        TI[("__ctx", "run_id")] = rid
    return rid


def _attempt(ctx):
    ti = (ctx or {}).get("ti")
    n = getattr(ti, "try_number", None)
    return n if isinstance(n, int) else ""


def _fail_audit(ctx, task_id, run_id, started, err, extra=None):
    write_audit(os.environ.get("ETL_AUDIT_FILE", cfg.AUDIT_FILE),
                {"run_id": run_id, "dag_id": "orders_etl", "task_id": task_id,
                 "started_at": started,
                 "completed_at": datetime.now(timezone.utc).isoformat(),
                 "status": "failed",
                 "source": os.environ.get("ETL_SOURCE_CSV", cfg.SOURCE_CSV),
                 "target": os.environ.get("ETL_TARGET_DB", cfg.TARGET_DB),
                 "extracted": 0, "processed": 0, "inserted": 0, "updated": 0,
                 "skipped_unchanged": 0, "skipped_stale": 0, "rejected": 0,
                 "error": err, "attempt": _attempt(ctx), **(extra or {})})


def t_extract(**ctx):
    started = datetime.now(timezone.utc).isoformat()
    run_id = _run_id(ctx)
    try:
        rows = extract(os.environ.get("ETL_SOURCE_CSV", cfg.SOURCE_CSV))
    except Exception as e:
        _fail_audit(ctx, "extract", run_id, started, f"{type(e).__name__}: {e}")
        raise
    _push(ctx, "extract", "raw", rows)
    _push(ctx, "extract", "run_id", run_id)
    return len(rows)


def t_transform_validate(**ctx):
    started = datetime.now(timezone.utc).isoformat()
    run_id = _pull(ctx, "extract", "run_id") or _run_id(ctx)
    try:
        raw = _pull(ctx, "extract", "raw", default=[])
        db = os.environ.get("ETL_TARGET_DB", cfg.TARGET_DB)
        wm_file = os.environ.get("ETL_WATERMARK_FILE", cfg.WATERMARK_FILE)
        full = os.environ.get("ETL_FULL_LOAD", "") == "1"
        wm = None if full else read_watermark(wm_file, db_path=db)
        clean, rej_t = transform(raw or [])
        valid, rej_v = validate(clean)
        selected = select_incremental(valid, wm, fetch_existing(db))
        new_wm = None
        if valid:
            top = max(valid, key=lambda r: (r["updated_at"], r["order_id"]))
            new_wm = (top["updated_at"], top["order_id"])
        _push(ctx, "transform_validate", "payload",
              {"rows": selected, "new_wm": new_wm, "extracted": len(raw or []),
               "rejected": len(rej_t) + len(rej_v), "run_id": run_id})
    except Exception as e:
        _fail_audit(ctx, "transform_validate", run_id, started,
                    f"{type(e).__name__}: {e}")
        raise
    return len(selected)


def t_load(**ctx):
    started = datetime.now(timezone.utc).isoformat()
    payload = _pull(ctx, "transform_validate", "payload", default={}) or {}
    rows = payload.get("rows", [])
    run_id = payload.get("run_id") or _run_id(ctx)
    status, err = "success", ""
    inserted = updated = unchanged = stale = 0
    try:
        inserted, updated, unchanged, stale = load(
            rows, os.environ.get("ETL_TARGET_DB", cfg.TARGET_DB),
            watermark=tuple(payload["new_wm"]) if payload.get("new_wm") else None)
        if payload.get("new_wm"):
            write_watermark(os.environ.get("ETL_WATERMARK_FILE", cfg.WATERMARK_FILE),
                            payload["new_wm"][0])  # mirror; DB cursor already committed
    except Exception as e:
        status, err = "failed", f"{type(e).__name__}: {e}"
        raise
    finally:
        write_audit(os.environ.get("ETL_AUDIT_FILE", cfg.AUDIT_FILE),
                    {"run_id": run_id, "dag_id": "orders_etl", "task_id": "load",
                     "started_at": started,
                     "completed_at": datetime.now(timezone.utc).isoformat(),
                     "status": status,
                     "source": os.environ.get("ETL_SOURCE_CSV", cfg.SOURCE_CSV),
                     "target": os.environ.get("ETL_TARGET_DB", cfg.TARGET_DB),
                     "extracted": payload.get("extracted", 0), "processed": len(rows),
                     "inserted": inserted, "updated": updated,
                     "skipped_unchanged": unchanged, "skipped_stale": stale,
                     "rejected": payload.get("rejected", 0), "error": err,
                     "attempt": _attempt(ctx)})


if AIRFLOW_AVAILABLE:
    with DAG(dag_id="orders_etl", default_args=DEFAULT_ARGS, schedule="@daily",
             start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
             tags=["etl", "orders"]) as dag:
        e = PythonOperator(task_id="extract", python_callable=t_extract)
        t = PythonOperator(task_id="transform_validate", python_callable=t_transform_validate)
        l = PythonOperator(task_id="load", python_callable=t_load)
        e >> t >> l
else:
    dag = None

if __name__ == "__main__":  # local run without Airflow: python dags/orders_etl_dag.py
    import logging
    logging.basicConfig(level=logging.INFO)
    t_extract(), t_transform_validate(), t_load()
    print("done")

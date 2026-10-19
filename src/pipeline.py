"""Pipeline orchestration (local/test entry point).

Stage logic lives in src/extract.py, src/transform.py, src/validate.py,
src/load.py, src/audit.py and src/database.py; this module wires the stages
together and is re-exported for backward compatibility (existing tests and
the DAG import these names from here).
"""
import logging
import uuid
from datetime import datetime, timezone

from src import database
from src.audit import AUDIT_FIELDS, write_audit, write_audit_db  # noqa: F401
from src.extract import extract  # noqa: F401
from src.load import (fetch_existing, filter_incremental, load,  # noqa: F401
                      read_watermark, select_incremental, write_watermark)
from src.transform import REQUIRED_COLUMNS, VALID_STATUSES  # noqa: F401
from src.transform import transform  # noqa: F401
from src.validate import validate  # noqa: F401

log = logging.getLogger("etl")


def run_etl(source_csv, db_path, watermark_path, audit_file,
            dag_id="orders_etl", task_id="load", full_load=False,
            run_id=None, attempt=None):
    """Full pipeline: extract -> transform -> validate -> select -> load -> audit.

    Watermark advances (in the load transaction + JSON mirror) only on
    success; failures leave target data and cursor untouched and are
    recorded in the audit log before the original error is re-raised.
    The audit row goes to the CSV log and, best-effort, to the etl_audit
    table (a failing audit-table write is logged, never masks the run status).
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc).isoformat()
    status, error, counts = "success", "", {}
    try:
        database.ensure_schema(db_path)
        raw = extract(source_csv)
        counts["extracted"] = len(raw)
        wm = None if full_load else read_watermark(watermark_path, db_path=db_path)
        # Transform before filtering so malformed rows are rejected per-row, not fatal.
        clean, rej_transform = transform(raw)
        valid, rej_validate = validate(clean)
        existing = fetch_existing(db_path)
        selected = select_incremental(valid, wm, existing)
        counts.update({"rejected_transform": len(rej_transform),
                       "rejected_validate": len(rej_validate),
                       "processed": len(selected)})
        new_wm = None
        if valid:
            top = max(valid, key=lambda r: (r["updated_at"], r["order_id"]))
            new_wm = (top["updated_at"], top["order_id"])
        inserted, updated, unchanged, stale = load(
            selected, db_path, watermark=new_wm)
        counts.update({"inserted": inserted, "updated": updated,
                       "skipped_unchanged": unchanged, "skipped_stale": stale,
                       "rejected": len(rej_transform) + len(rej_validate)})
        if new_wm is not None:
            write_watermark(watermark_path, new_wm[0])  # mirror; DB cursor already committed
        for rej in rej_transform + [{"row": {}, "error": e["error"]} for e in rej_validate]:
            log.warning("rejected: %s | %s", rej.get("error"), rej.get("row"))
    except Exception as e:  # cursor untouched: DB txn rolled back, mirror unwritten
        status, error = "failed", f"{type(e).__name__}: {e}"
        log.exception("ETL failed")
        raise RuntimeError(error) from e
    finally:
        audit_record = {"run_id": run_id, "dag_id": dag_id, "task_id": task_id,
                        "started_at": started,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                        "status": status, "source": source_csv,
                        "target": database.target_label(db_path),
                        **{k: counts.get(k, 0) for k in
                           ("extracted", "processed", "inserted", "updated",
                            "skipped_unchanged", "skipped_stale", "rejected")},
                        "error": error, "attempt": attempt if attempt is not None else ""}
        write_audit(audit_file, audit_record)
        try:
            write_audit_db(audit_record, db_path)
        except Exception as e:
            log.error("audit-table write failed (CSV row kept): %s", e)
    log.info("ETL %s: %s", status, counts)
    return {"status": status, "run_id": run_id, **counts}

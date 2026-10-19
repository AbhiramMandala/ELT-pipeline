"""Audit stage: execution records to CSV log (local mirror) + etl_audit table."""
import csv
import logging
import os
import threading

from src import database

log = logging.getLogger("etl")

# Canonical audit schema. write_audit backfills older CSV files to this header.
AUDIT_FIELDS = ["run_id", "dag_id", "task_id", "started_at", "completed_at", "status",
                "source", "target", "extracted", "processed", "inserted", "updated",
                "skipped_unchanged", "skipped_stale", "rejected", "error", "attempt"]
_AUDIT_LOCK = threading.Lock()
_COUNT_FIELDS = ("extracted", "processed", "inserted", "updated",
                 "skipped_unchanged", "skipped_stale", "rejected")


def normalize(record):
    row = {k: record.get(k, "") for k in AUDIT_FIELDS}
    for k in _COUNT_FIELDS:
        try:
            row[k] = int(row[k] or 0)
        except (TypeError, ValueError):
            row[k] = 0
    return row


def write_audit(audit_file, record):
    """Append one canonical audit row to the CSV log; transparently migrates
    older files with a different header instead of corrupting them.
    Same-process safe (threading lock)."""
    row = normalize(record)
    path = os.path.abspath(audit_file)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with _AUDIT_LOCK:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, newline="", encoding="utf-8") as f:
                old_header = next(csv.reader(f), None)
            with open(path, newline="", encoding="utf-8") as f:
                old = list(csv.DictReader(f))
            if old_header != AUDIT_FIELDS:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=AUDIT_FIELDS)
                    w.writeheader()
                    for o in old:
                        w.writerow(normalize(o))
        else:
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=AUDIT_FIELDS).writeheader()
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=AUDIT_FIELDS).writerow(row)


def write_audit_db(record, db_path):
    """Insert one audit row into etl_audit (idempotent on (run_id, task_id)).

    Raises on DB errors — callers decide whether that is fatal (DAG audit
    task: yes) or best-effort (run_etl keeps the CSV row authoritative
    locally and only logs a warning).
    """
    row = normalize(record)
    conn = database.connect(db_path)
    try:
        cur = conn.cursor()
        if database.backend() == "sqlite":
            stmt = ("INSERT OR IGNORE INTO etl_audit ("
                    + ",".join(AUDIT_FIELDS) + ") VALUES ("
                    + ",".join(f":{k}" for k in AUDIT_FIELDS) + ")")
        else:
            stmt = ("INSERT INTO etl_audit ("
                    + ",".join(AUDIT_FIELDS) + ") VALUES ("
                    + ",".join(f":{k}" for k in AUDIT_FIELDS) + ")"
                    " ON CONFLICT DO NOTHING")
        cur.execute(database.adapt(stmt), row)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

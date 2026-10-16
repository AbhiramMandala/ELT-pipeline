"""Extract / Transform / Validate / Load / State / Audit — stdlib only."""
import csv
import json
import logging
import os
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

log = logging.getLogger("etl")
REQUIRED_COLUMNS = ["order_id", "customer_name", "amount", "currency", "updated_at", "status"]
VALID_STATUSES = {"completed", "pending", "cancelled"}
_TABLE_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")

# Canonical audit schema. write_audit backfills older files to this header.
AUDIT_FIELDS = ["run_id", "dag_id", "task_id", "started_at", "completed_at", "status",
                "source", "target", "extracted", "processed", "inserted", "updated",
                "skipped_unchanged", "skipped_stale", "rejected", "error", "attempt"]
_AUDIT_LOCK = threading.Lock()


def _table(name):
    if not _TABLE_RE.fullmatch(name or ""):
        raise ValueError(f"invalid table name: {name!r}")
    return name


def extract(source_csv):
    """Read CSV into list of dicts.

    Batch-level failures (missing file, missing header, missing required
    columns) raise and fail the whole run. Individual bad rows are handled
    later by transform() and only rejected, not fatal.
    """
    if not os.path.exists(source_csv):
        raise FileNotFoundError(f"Source file not found: {source_csv}")
    # utf-8-sig transparently strips a BOM (typical Excel export) and reads
    # plain UTF-8 identically, so BOM-prefixed files don't fail the batch.
    with open(source_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"source has no header row: {source_csv}")
        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"source missing required columns {missing}: {source_csv}")
        rows = list(reader)
    log.info("extracted=%d from %s", len(rows), source_csv)
    return rows


def transform(rows):
    """Clean, normalize, type-convert, dedupe. Returns (clean_rows, rejected)."""
    clean, rejected, seen = [], [], set()
    for r in rows:
        try:
            oid = str(r.get("order_id", "")).strip()
            if not oid or not oid.isdigit():
                raise ValueError(f"invalid order_id: {r.get('order_id')!r}")
            if oid in seen:
                raise ValueError(f"duplicate order_id in batch: {oid}")
            seen.add(oid)
            customer = str(r.get("customer_name", "")).strip()
            if not customer:
                raise ValueError(f"order {oid}: empty customer_name")
            amount = float(str(r.get("amount", "")).strip())
            if amount <= 0:
                raise ValueError(f"order {oid}: amount must be > 0, got {amount}")
            currency = str(r.get("currency", "")).strip().upper()
            if len(currency) != 3:
                raise ValueError(f"order {oid}: invalid currency {r.get('currency')!r}")
            updated_at = str(r.get("updated_at", "")).strip()
            datetime.fromisoformat(updated_at)  # validates ISO-8601
            status = str(r.get("status", "")).strip().lower() or "pending"
            if status not in VALID_STATUSES:
                raise ValueError(f"order {oid}: invalid status {r.get('status')!r}")
            clean.append({"order_id": int(oid), "customer_name": customer,
                          "amount": amount, "currency": currency,
                          "updated_at": updated_at, "status": status})
        except (ValueError, TypeError) as e:
            rejected.append({"row": r, "error": str(e)})
    return clean, rejected


def validate(rows):
    """Final gate: required cols, nulls, dup PKs. Returns (valid, errors)."""
    valid, errors = [], []
    seen = set()
    for r in rows:
        missing = [c for c in REQUIRED_COLUMNS if r.get(c) in (None, "")]
        if missing:
            errors.append({"order_id": r.get("order_id"), "error": f"missing/null: {missing}"})
            continue
        if r["order_id"] in seen:
            errors.append({"order_id": r["order_id"], "error": "duplicate primary key"})
            continue
        seen.add(r["order_id"])
        valid.append(r)
    return valid, errors


def read_watermark(path, db_path=None, table="orders"):
    """Effective watermark timestamp (or None). Prefers the transactional
    DB cursor, falls back to the JSON mirror file (legacy format kept)."""
    if db_path and os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path, timeout=30) as con:
                cols = {r[1] for r in con.execute("PRAGMA table_info(etl_state)").fetchall()}
                if {"k", "v"} <= cols:
                    row = con.execute("SELECT v FROM etl_state WHERE k='watermark_ts'").fetchone()
                    if row and row[0]:
                        return row[0]
        except sqlite3.Error:
            pass  # corrupt/unreadable DB must not break the read path
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("last_updated_at")
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError):
        return None


def write_watermark(path, value):
    """Rewrite the JSON mirror file (observability/back-compat only; the
    authoritative cursor lives in the DB transaction — see load())."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_updated_at": value}, f)


def _changed(new, stored):
    return any(new[c] != stored[c] for c in REQUIRED_COLUMNS if c != "order_id")


def fetch_existing(db_path, table="orders"):
    """Current target state {order_id: row}. Missing DB/table -> {}."""
    table = _table(table)
    if not os.path.exists(db_path):
        return {}
    try:
        with sqlite3.connect(db_path, timeout=30) as con:
            cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}
            if not {"order_id", "customer_name", "amount", "currency",
                    "updated_at", "status"} <= cols:
                return {}
            return {r[0]: {"order_id": r[0], "customer_name": r[1], "amount": r[2],
                           "currency": r[3], "updated_at": r[4], "status": r[5]}
                    for r in con.execute(
                        f"SELECT order_id,customer_name,amount,currency,updated_at,status"
                        f" FROM {table}")}
    except sqlite3.Error:
        return {}


def filter_incremental(rows, watermark):
    """Timestamp-only pre-filter (kept for back-compat). Prefer
    select_incremental(), which also covers late-arriving/changed rows."""
    if not watermark:
        return rows
    return [r for r in rows if r["updated_at"] > watermark]


def select_incremental(valid_rows, watermark, existing):
    """Incremental selection against actual target state, deterministic by
    (updated_at, order_id) so duplicate timestamps behave identically.

    - unseen PK -> include (new + late-arriving rows, any timestamp)
    - changed content -> include (corrections/updates, any timestamp)
    - identical to stored -> skip (reruns and overlapping batches are no-ops)
    - fast path: updated_at > watermark -> include without further checks
    """
    selected = []
    for r in sorted(valid_rows, key=lambda x: (x["updated_at"], x["order_id"])):
        if watermark is not None and r["updated_at"] > watermark:
            selected.append(r)
            continue
        stored = existing.get(r["order_id"])
        if stored is None or _changed(r, stored):
            selected.append(r)
    return selected


def load(rows, db_path, table="orders", watermark=None):
    """Idempotent UPSERT on order_id PK in one BEGIN IMMEDIATE transaction.

    - identical rows are skipped (unchanged), older-than-stored rows are
      skipped (stale: out-of-order update protection) — neither is rewritten
    - inserted = new PKs, updated = existing PKs with changed content
    - watermark (ts, pk) cursor is committed in the SAME transaction;
      any failure rolls everything back, so the cursor never advances
      past uncommitted data
    Returns (inserted, updated, skipped_unchanged, skipped_stale).
    """
    table = _table(table)
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent or ".", exist_ok=True)
    con = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            order_id INTEGER PRIMARY KEY, customer_name TEXT NOT NULL,
            amount REAL NOT NULL, currency TEXT NOT NULL,
            updated_at TEXT NOT NULL, status TEXT NOT NULL)""")
        con.execute("""CREATE TABLE IF NOT EXISTS etl_state (
            k TEXT PRIMARY KEY, v TEXT NOT NULL)""")
        existing = {r[0]: {"customer_name": r[1], "amount": r[2], "currency": r[3],
                           "updated_at": r[4], "status": r[5]}
                    for r in con.execute(
                        f"SELECT order_id,customer_name,amount,currency,updated_at,status"
                        f" FROM {table}")}
        inserted = updated = unchanged = stale = 0
        for r in sorted(rows, key=lambda x: (x["updated_at"], x["order_id"])):
            stored = existing.get(r["order_id"])
            if stored is not None and not any(
                    r[c] != stored[c] for c in
                    ("customer_name", "amount", "currency", "updated_at", "status")):
                unchanged += 1
                continue
            if stored is not None and r["updated_at"] < stored["updated_at"]:
                stale += 1  # out-of-order: never let an older record clobber newer state
                continue
            con.execute(f"""INSERT INTO {table}
                (order_id,customer_name,amount,currency,updated_at,status)
                VALUES (:order_id,:customer_name,:amount,:currency,:updated_at,:status)
                ON CONFLICT(order_id) DO UPDATE SET customer_name=excluded.customer_name,
                amount=excluded.amount, currency=excluded.currency,
                updated_at=excluded.updated_at, status=excluded.status""", r)
            if stored is None:
                inserted += 1
            else:
                updated += 1
        if watermark is not None:
            ts, pk = watermark
            con.execute("INSERT INTO etl_state(k,v) VALUES('watermark_ts',?)"
                        " ON CONFLICT(k) DO UPDATE SET v=excluded.v", (ts,))
            con.execute("INSERT INTO etl_state(k,v) VALUES('watermark_pk',?)"
                        " ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(pk),))
        con.commit()
    except Exception:
        try:
            con.rollback()
        except sqlite3.Error:
            pass
        raise
    finally:
        con.close()
    return inserted, updated, unchanged, stale


def write_audit(audit_file, record):
    """Append one canonical audit row; transparently migrates older files
    with a different header instead of corrupting them. Same-process safe."""
    row = {k: record.get(k, "") for k in AUDIT_FIELDS}
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
                        w.writerow({k: o.get(k, "") for k in AUDIT_FIELDS})
        else:
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=AUDIT_FIELDS).writeheader()
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=AUDIT_FIELDS).writerow(row)


def run_etl(source_csv, db_path, watermark_path, audit_file,
            dag_id="orders_etl", task_id="load", full_load=False,
            run_id=None, attempt=None):
    """Full pipeline: extract -> transform -> validate -> select -> load -> audit.

    Watermark advances (in the load transaction + JSON mirror) only on
    success; failures leave target data and cursor untouched and are
    recorded in the audit log before the original error is re-raised.
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc).isoformat()
    status, error, counts = "success", "", {}
    try:
        raw = extract(source_csv)
        counts["extracted"] = len(raw)
        wm = None if full_load else read_watermark(watermark_path, db_path=db_path)
        # Transform before filtering so malformed rows are rejected per-row,
        # never fatal to the batch. (why comment)
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
        write_audit(audit_file, {"run_id": run_id, "dag_id": dag_id, "task_id": task_id,
                                 "started_at": started,
                                 "completed_at": datetime.now(timezone.utc).isoformat(),
                                 "status": status, "source": source_csv, "target": db_path,
                                 **{k: counts.get(k, 0) for k in
                                    ("extracted", "processed", "inserted", "updated",
                                     "skipped_unchanged", "skipped_stale", "rejected")},
                                 "error": error, "attempt": attempt if attempt is not None else ""})
    log.info("ETL %s: %s", status, counts)
    return {"status": status, "run_id": run_id, **counts}

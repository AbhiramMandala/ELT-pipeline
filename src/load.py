"""Loading stage: incremental selection, transactional UPSERT, watermark cursor."""
import logging
import os
import re

from src import database
from src.transform import REQUIRED_COLUMNS

log = logging.getLogger("etl")
_TABLE_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _table(name):
    if not _TABLE_RE.fullmatch(name or ""):
        raise ValueError(f"invalid table name: {name!r}")
    return name


def read_watermark(path, db_path=None, table="orders"):
    """Effective watermark timestamp (or None). Prefers the transactional
    DB cursor, falls back to the JSON mirror file (legacy format kept)."""
    if db_path:
        try:
            conn = database.connect(db_path)
        except Exception:
            conn = None
        if conn is not None:
            try:
                cur = conn.cursor()
                try:
                    row = cur.execute(database.adapt(
                        "SELECT v FROM etl_state WHERE k='watermark_ts'")).fetchone()
                    if row and row[0]:
                        return row[0]
                except Exception:
                    pass  # missing/corrupt state table must not break the read path
            finally:
                conn.close()
    try:
        import json
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("last_updated_at")
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError):
        return None


def write_watermark(path, value):
    """Rewrite the JSON mirror file (observability/back-compat only; the
    authoritative cursor lives in the DB transaction — see load())."""
    import json
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_updated_at": value}, f)


def _changed(new, stored):
    return any(new[c] != stored[c] for c in REQUIRED_COLUMNS if c != "order_id")


def fetch_existing(db_path, table="orders"):
    """Current target state {order_id: row}. Missing DB/table -> {}."""
    table = _table(table)
    if database.backend() == "sqlite" and not os.path.exists(db_path):
        return {}
    try:
        conn = database.connect(db_path)
    except Exception:
        return {}
    try:
        cur = conn.cursor()
        try:
            cur.execute(database.adapt(
                f"SELECT order_id,customer_name,amount,currency,updated_at,status"
                f" FROM {table}"))
            rows = cur.fetchall()
        except Exception:
            return {}  # missing/incompatible table (incl. the rollback-test schema)
        cols = ("order_id", "customer_name", "amount", "currency", "updated_at", "status")
        if len(rows) and len(rows[0]) != len(cols):
            return {}
        return {r[0]: dict(zip(cols, r)) for r in rows}
    except Exception:
        return {}
    finally:
        conn.close()


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


_UPSERT = """INSERT INTO {table}
    (order_id,customer_name,amount,currency,updated_at,status)
    VALUES (:order_id,:customer_name,:amount,:currency,:updated_at,:status)
    ON CONFLICT(order_id) DO UPDATE SET customer_name=excluded.customer_name,
    amount=excluded.amount, currency=excluded.currency,
    updated_at=excluded.updated_at, status=excluded.status"""


def load(rows, db_path, table="orders", watermark=None):
    """Idempotent UPSERT on order_id PK in a single transaction.

    - identical rows are skipped (unchanged), older-than-stored rows are
      skipped (stale: out-of-order update protection) — neither is rewritten
    - inserted = new PKs, updated = existing PKs with changed content
    - watermark (ts, pk) cursor is committed in the SAME transaction;
      any failure rolls everything back, so the cursor never advances
      past uncommitted data
    Returns (inserted, updated, skipped_unchanged, skipped_stale).
    """
    table = _table(table)
    conn = database.connect(db_path)
    try:
        database.begin(conn)
        cur = conn.cursor()
        cur.execute(database.adapt(f"""CREATE TABLE IF NOT EXISTS {table} (
            order_id INTEGER PRIMARY KEY, customer_name TEXT NOT NULL,
            amount REAL NOT NULL, currency TEXT NOT NULL,
            updated_at TEXT NOT NULL, status TEXT NOT NULL)"""))
        cur.execute(database.adapt("""CREATE TABLE IF NOT EXISTS etl_state (
            k TEXT PRIMARY KEY, v TEXT NOT NULL)"""))
        cur.execute(database.adapt(
            f"SELECT order_id,customer_name,amount,currency,updated_at,status"
            f" FROM {table}"))
        existing = {r[0]: {"customer_name": r[1], "amount": r[2], "currency": r[3],
                           "updated_at": r[4], "status": r[5]}
                    for r in cur.fetchall()}
        inserted = updated = unchanged = stale = 0
        stmt = database.adapt(_UPSERT.format(table=table))
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
            cur.execute(stmt, r)
            if stored is None:
                inserted += 1
            else:
                updated += 1
        if watermark is not None:
            ts, pk = watermark
            cur.execute(database.adapt(
                "INSERT INTO etl_state(k,v) VALUES('watermark_ts',:ts)"
                " ON CONFLICT(k) DO UPDATE SET v=excluded.v"), {"ts": ts})
            cur.execute(database.adapt(
                "INSERT INTO etl_state(k,v) VALUES('watermark_pk',:pk)"
                " ON CONFLICT(k) DO UPDATE SET v=excluded.v"), {"pk": str(pk)})
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()
    return inserted, updated, unchanged, stale

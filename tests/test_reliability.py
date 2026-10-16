"""Regression tests: incremental correctness, reliability, auditability."""
import csv
import os
import sqlite3
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.pipeline import (AUDIT_FIELDS, fetch_existing, load, read_watermark,
                          run_etl, select_incremental)

HDR = "order_id,customer_name,amount,currency,updated_at,status\n"


def row(oid, ts, **kw):
    d = {"order_id": str(oid), "customer_name": "Ann", "amount": "10.00",
         "currency": "USD", "updated_at": ts, "status": "completed"}
    d.update({k: str(v) for k, v in kw.items()})
    return ",".join(d[c] for c in
                    ["order_id", "customer_name", "amount", "currency", "updated_at", "status"]) + "\n"


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "w.db")
        self.wm = os.path.join(self.tmp.name, "wm.json")
        self.audit = os.path.join(self.tmp.name, "audit.csv")
        self.src = os.path.join(self.tmp.name, "in.csv")

    def tearDown(self):
        try:
            self.tmp.cleanup()
        except Exception:
            pass

    def write_src(self, *lines):
        with open(self.src, "w", encoding="utf-8") as f:
            f.write(HDR)
            for ln in lines:
                f.write(ln)

    def db_rows(self):
        with sqlite3.connect(self.db) as con:
            return {r[0]: r for r in con.execute(
                "SELECT order_id,customer_name,amount,currency,updated_at,status FROM orders")}

    def audit_rows(self):
        with open(self.audit, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))


class TestIncremental(unittest.TestCase):
    def test_late_arriving_row_loaded(self):
        existing = {1: {"order_id": 1, "customer_name": "Ann", "amount": 10.0,
                        "currency": "USD", "updated_at": "2026-10-09T00:00:00",
                        "status": "completed"}}
        late = {"order_id": 2, "customer_name": "Bob", "amount": 5.0,
                "currency": "USD", "updated_at": "2026-10-01T00:00:00",  # older than watermark
                "status": "completed"}
        sel = select_incremental([late], "2026-10-09T00:00:00", existing)
        self.assertEqual([r["order_id"] for r in sel], [2])

    def test_updated_row_loaded_and_stale_guarded(self):
        base = _Base()
        base.setUp()
        try:
            base.write_src(row(1, "2026-10-01T10:00:00"))
            run_etl(base.src, base.db, base.wm, base.audit, full_load=True)
            # correction with NEWER timestamp applies
            base.write_src(row(1, "2026-10-05T10:00:00", amount="99.00"))
            r = run_etl(base.src, base.db, base.wm, base.audit)
            self.assertEqual(r["updated"], 1)
            self.assertEqual(base.db_rows()[1][2], 99.0)
            # out-of-order older record must NOT clobber newer state
            ins, upd, unch, stale = load(
                [{"order_id": 1, "customer_name": "Ann", "amount": 1.0,
                  "currency": "USD", "updated_at": "2026-10-02T10:00:00",
                  "status": "completed"}], base.db)
            self.assertEqual((ins, upd, unch, stale), (0, 0, 0, 1))
            self.assertEqual(base.db_rows()[1][2], 99.0)
        finally:
            base.tearDown()

    def test_duplicate_timestamps_deterministic(self):
        rows = [{"order_id": 2, "customer_name": "A", "amount": 1.0, "currency": "USD",
                 "updated_at": "2026-10-01T00:00:00", "status": "completed"},
                {"order_id": 1, "customer_name": "B", "amount": 2.0, "currency": "USD",
                 "updated_at": "2026-10-01T00:00:00", "status": "completed"}]
        sel = select_incremental(rows, None, {})
        self.assertEqual([r["order_id"] for r in sel], [1, 2])  # PK tiebreak

    def test_overlapping_batch_idempotent(self):
        base = _Base()
        base.setUp()
        try:
            base.write_src(row(1, "2026-10-01T10:00:00"), row(2, "2026-10-02T10:00:00"))
            run_etl(base.src, base.db, base.wm, base.audit, full_load=True)
            r = run_etl(base.src, base.db, base.wm, base.audit)  # exact rerun
            self.assertEqual(r["processed"], 0)
            self.assertEqual(len(base.db_rows()), 2)
            self.assertEqual((r["inserted"], r["updated"]), (0, 0))
        finally:
            base.tearDown()


class TestReliability(_Base):
    def test_failed_load_rolls_back_and_preserves_watermark(self):
        self.write_src(row(1, "2026-10-01T10:00:00"))
        run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        wm_before = read_watermark(self.wm, db_path=self.db)
        # corrupt the target schema so the next load fails mid-transaction
        with sqlite3.connect(self.db) as con:
            con.execute("DROP TABLE orders")
            con.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_name TEXT)")
            con.execute("INSERT INTO orders VALUES (1, 'Ann')")
        self.write_src(row(1, "2026-10-02T10:00:00"), row(2, "2026-10-03T10:00:00"))
        with self.assertRaises(RuntimeError):
            run_etl(self.src, self.db, self.wm, self.audit)
        self.assertEqual(read_watermark(self.wm, db_path=self.db), wm_before)
        with sqlite3.connect(self.db) as con:  # partial writes rolled back
            self.assertEqual(con.execute("SELECT * FROM orders").fetchall(), [(1, "Ann")])
        fails = [a for a in self.audit_rows() if a["status"] == "failed"]
        self.assertEqual(len(fails), 1)
        self.assertIn("OperationalError", fails[0]["error"])
        self.assertTrue(fails[0]["run_id"])

    def test_missing_columns_fail_batch(self):
        with open(self.src, "w", encoding="utf-8") as f:
            f.write("order_id,customer_name\n1,Ann\n")
        with self.assertRaises(RuntimeError) as cm:
            run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        self.assertIn("missing required columns", str(cm.exception))
        self.assertEqual(self.audit_rows()[-1]["status"], "failed")

    def test_audit_schema_and_counts(self):
        self.write_src(row(1, "2026-10-01T10:00:00"),
                       row(2, "2026-10-02T10:00:00", amount="-1"),  # rejected
                       row(1, "2026-10-03T10:00:00", amount="5.00"))  # dup in batch
        res = run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        a = self.audit_rows()[-1]
        self.assertEqual(list(a.keys()), AUDIT_FIELDS)
        for k in ("run_id", "started_at", "completed_at"):
            self.assertTrue(a[k], k)
        self.assertEqual(a["status"], "success")
        self.assertEqual(int(a["extracted"]), 3)
        self.assertEqual(int(a["rejected"]), 2)
        self.assertEqual(int(a["inserted"]), 1)
        self.assertEqual(res["run_id"], a["run_id"])

    def test_db_watermark_preferred_over_stale_file(self):
        self.write_src(row(1, "2026-10-05T00:00:00"))
        run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        import json
        with open(self.wm, "w", encoding="utf-8") as f:  # file regresses; DB cursor wins
            json.dump({"last_updated_at": "2020-01-01T00:00:00"}, f)
        self.assertEqual(read_watermark(self.wm, db_path=self.db), "2026-10-05T00:00:00")

    def test_concurrent_runs_no_corruption(self):
        self.write_src(row(1, "2026-10-01T10:00:00"), row(2, "2026-10-02T10:00:00"))
        errors = []

        def work(audit):
            try:
                run_etl(self.src, self.db, self.wm, audit, full_load=True)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=work, args=(self.audit,)) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)
        self.assertFalse([t for t in threads if t.is_alive()], "threads hung")
        self.assertEqual(errors, [])
        self.assertEqual(len(self.db_rows()), 2)  # no duplicates, no partial writes

    def test_fetch_existing_missing_db(self):
        self.assertEqual(fetch_existing(os.path.join(self.tmp.name, "nope.db")), {})

    def test_bom_prefixed_source_loads(self):
        with open(self.src, "w", encoding="utf-8-sig") as f:  # Excel-style BOM export
            f.write(HDR)
            f.write(row(1, "2026-10-01T10:00:00"))
        res = run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        self.assertEqual(res["status"], "success")
        self.assertEqual(res["inserted"], 1)

    def test_mirror_write_crash_self_heals(self):
        from unittest import mock
        from src import pipeline as pl
        self.write_src(row(1, "2026-10-01T10:00:00"))
        with mock.patch.object(pl, "write_watermark", side_effect=OSError("disk gone")):
            with self.assertRaises(RuntimeError):
                run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        # data + DB cursor committed; only the mirror was lost
        self.assertEqual(len(self.db_rows()), 1)
        self.assertFalse(os.path.exists(self.wm))
        rerun = run_etl(self.src, self.db, self.wm, self.audit, full_load=True)
        self.assertEqual(rerun["status"], "success")
        self.assertEqual((rerun["processed"], rerun["inserted"]), (0, 0))
        self.assertTrue(os.path.exists(self.wm))


if __name__ == "__main__":
    unittest.main()

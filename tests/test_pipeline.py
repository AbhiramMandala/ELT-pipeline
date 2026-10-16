import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.pipeline import (extract, filter_incremental, load, run_etl, transform,
                          validate, write_watermark, read_watermark)

SAMPLE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "data", "sample_orders.csv")
GOOD = {"order_id": "9", "customer_name": "Gina", "amount": "10.5",
        "currency": "usd", "updated_at": "2026-10-08T10:00:00", "status": "completed"}


class TestETL(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "w.db")
        self.wm = os.path.join(self.tmp.name, "wm.json")
        self.audit = os.path.join(self.tmp.name, "audit.csv")

    def tearDown(self):
        import shutil
        try:
            shutil.rmtree(self.tmp.name, ignore_errors=True)
        except Exception:
            pass
        try:
            self.tmp.cleanup()
        except Exception:
            pass

    def test_transform_happy_path(self):
        clean, rej = transform([GOOD])
        self.assertEqual(len(clean), 1)
        self.assertEqual(rej, [])
        self.assertEqual(clean[0]["currency"], "USD")
        self.assertEqual(clean[0]["order_id"], 9)

    def test_transform_rejects_bad_amount_empty_name_dup(self):
        rows = [dict(GOOD), dict(GOOD, order_id="10", amount="-5"),
                dict(GOOD, order_id="11", customer_name=" ")]
        clean, rej = transform(rows)
        self.assertEqual(len(clean), 1)
        self.assertEqual(len(rej), 2)

    def test_validate_missing_and_dup_pk(self):
        clean, _ = transform([dict(GOOD, order_id="20"), dict(GOOD, order_id="21")])
        clean[1]["order_id"] = 20
        valid, errors = validate(clean)
        self.assertEqual(len(valid), 1)
        self.assertIn("duplicate", errors[0]["error"])

    def test_incremental_only_new_rows(self):
        clean, _ = transform([dict(GOOD, order_id="30", updated_at="2026-10-01T00:00:00"),
                              dict(GOOD, order_id="31", updated_at="2026-10-09T00:00:00")])
        self.assertEqual(len(filter_incremental(clean, "2026-10-05T00:00:00")), 1)
        self.assertEqual(len(filter_incremental(clean, None)), 2)

    def _count(self):
        with sqlite3.connect(self.db) as con:
            return con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

    def test_full_run_then_incremental_no_dup(self):
        r1 = run_etl(SAMPLE, self.db, self.wm, self.audit, full_load=True)
        self.assertEqual(r1["status"], "success")
        n1 = self._count()
        r2 = run_etl(SAMPLE, self.db, self.wm, self.audit)  # incremental: nothing new
        n2 = self._count()
        self.assertEqual(n1, n2)
        self.assertEqual(r2["processed"], 0)
        # rerun full load is idempotent (upsert, no dup growth)
        run_etl(SAMPLE, self.db, self.wm, self.audit, full_load=True)
        n3 = self._count()
        self.assertEqual(n1, n3)
        self.assertGreater(r1["rejected"], 0)  # fixture has bad rows

    def test_empty_input(self):
        empty = os.path.join(self.tmp.name, "empty.csv")
        with open(empty, "w") as f:
            f.write("order_id,customer_name,amount,currency,updated_at,status\n")
        res = run_etl(empty, self.db, self.wm, self.audit, full_load=True)
        self.assertEqual(res["processed"], 0)

    def test_missing_source_raises_and_audits_failure(self):
        with self.assertRaises(RuntimeError):
            run_etl(os.path.join(self.tmp.name, "nope.csv"), self.db, self.wm, self.audit)
        with open(self.audit) as f:
            self.assertIn("failed", f.read())

    def test_watermark_persists(self):
        write_watermark(self.wm, "2026-10-09T00:00:00")
        self.assertEqual(read_watermark(self.wm), "2026-10-09T00:00:00")


if __name__ == "__main__":
    unittest.main()

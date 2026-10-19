"""PostgreSQL backend integration tests (SQLite suite stays the default).

Needs a reachable PostgreSQL (docker-compose.yaml provides one on
localhost:5434, or point ETL_PG_* at your own). Each test uses an isolated
database created from the template `etl` database, so no test touches
shared state. Skipped entirely when PostgreSQL is unreachable.
"""
import csv
import os
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import psycopg
    _NO_PSYCOPG = None
except ImportError as e:
    psycopg = None
    _NO_PSYCOPG = e

from src import database

HDR = "order_id,customer_name,amount,currency,updated_at,status\n"


def row(oid, ts, **kw):
    d = {"order_id": str(oid), "customer_name": "Ann", "amount": "10.00",
         "currency": "USD", "updated_at": ts, "status": "completed"}
    d.update({k: str(v) for k, v in kw.items()})
    return ",".join(d[c] for c in
                    ["order_id", "customer_name", "amount", "currency", "updated_at", "status"]) + "\n"


def pg_available():
    if psycopg is None:
        return False
    try:
        kw = dict(database.pg_kwargs())
        kw.pop("dbname", None)
        with psycopg.connect(dbname="postgres", **kw) as con:
            con.execute("SELECT 1")
        return True
    except Exception:
        return False


@unittest.skipUnless(pg_available(), "PostgreSQL unreachable (see README for docker compose)")
class TestPostgres(unittest.TestCase):
    def setUp(self):
        self.old_env = dict(os.environ)
        os.environ["ETL_DB_BACKEND"] = "postgres"
        self.dbname = "etl_test_" + uuid.uuid4().hex[:10]
        kw = dict(database.pg_kwargs())
        kw.pop("dbname", None)
        self.admin = psycopg.connect(dbname="postgres", **kw)
        self.admin.autocommit = True
        self.admin.execute(f'CREATE DATABASE "{self.dbname}"')
        os.environ["ETL_PG_DBNAME"] = self.dbname
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "in.csv")
        self.wm = os.path.join(self.tmp.name, "wm.json")
        self.audit = os.path.join(self.tmp.name, "audit.csv")

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.old_env)
        try:
            self.tmp.cleanup()
        except Exception:
            pass
        try:
            self.admin.execute(f'DROP DATABASE "{self.dbname}"')
        except Exception:
            pass
        self.admin.close()

    def q(self, sql, params=None):
        with psycopg.connect(**database.pg_kwargs()) as con:
            rows = con.execute(sql, params or {}).fetchall()
        return rows

    def write_src(self, *lines):
        with open(self.src, "w", encoding="utf-8") as f:
            f.write(HDR)
            for ln in lines:
                f.write(ln)

    def test_full_then_incremental_update_and_stale(self):
        from src.pipeline import run_etl
        self.write_src(row(1, "2026-10-01T10:00:00"), row(2, "2026-10-02T10:00:00"))
        r1 = run_etl(self.src, "ignored", self.wm, self.audit, full_load=True)
        self.assertEqual((r1["status"], r1["inserted"]), ("success", 2))
        # rerun: no-op
        r2 = run_etl(self.src, "ignored", self.wm, self.audit)
        self.assertEqual((r2["processed"], r2["inserted"], r2["updated"]), (0, 0, 0))
        # update one row with newer ts + add late-arriving row with older ts
        self.write_src(row(1, "2026-10-01T10:00:00"),
                       row(2, "2026-10-05T10:00:00", amount="99.00"),
                       row(3, "2026-09-01T10:00:00"))
        r3 = run_etl(self.src, "ignored", self.wm, self.audit)
        self.assertEqual(r3["updated"], 1)
        self.assertEqual(r3["inserted"], 1)
        amounts = {r[0]: r[1] for r in
                   self.q("SELECT order_id, amount FROM orders ORDER BY 1")}
        self.assertEqual(amounts, {1: 10.0, 2: 99.0, 3: 10.0})
        # stale out-of-order write is ignored
        from src.pipeline import load
        self.assertEqual(load(
            [{"order_id": 2, "customer_name": "Ann", "amount": 1.0,
              "currency": "USD", "updated_at": "2026-10-03T10:00:00",
              "status": "completed"}], "ignored"), (0, 0, 0, 1))
        self.assertEqual(self.q("SELECT amount FROM orders WHERE order_id=2"),
                         [(99.0,)])
        # watermark cursor committed in-DB
        self.assertEqual(
            dict(self.q("SELECT k, v FROM etl_state"))["watermark_ts"],
            "2026-10-05T10:00:00")

    def test_audit_table_mirrors_csv(self):
        from src.pipeline import run_etl
        self.write_src(row(1, "2026-10-01T10:00:00"),
                       row(2, "2026-10-02T10:00:00", amount="-1"))
        res = run_etl(self.src, "ignored", self.wm, self.audit, full_load=True)
        with open(self.audit, newline="", encoding="utf-8") as f:
            csv_rows = list(csv.DictReader(f))
        db_rows = self.q("SELECT run_id, status, extracted, inserted, rejected"
                         " FROM etl_audit")
        self.assertEqual(len(csv_rows), 1)
        self.assertEqual(len(db_rows), 1)
        self.assertEqual(db_rows[0][0], csv_rows[0]["run_id"])
        self.assertEqual(db_rows[0][0], res["run_id"])
        self.assertEqual(db_rows[0][1:], ("success", 2, 1, 1))

    def test_failed_run_rolls_back_and_keeps_cursor(self):
        from src.pipeline import run_etl
        self.write_src(row(1, "2026-10-01T10:00:00"))
        run_etl(self.src, "ignored", self.wm, self.audit, full_load=True)
        before = self.q("SELECT k, v FROM etl_state")
        with open(self.src, "w", encoding="utf-8") as f:
            f.write("order_id,customer_name\n1,Ann\n")  # batch-fatal header
        with self.assertRaises(RuntimeError):
            run_etl(self.src, "ignored", self.wm, self.audit)
        self.assertEqual(self.q("SELECT k, v FROM etl_state"), before)
        self.assertEqual(len(self.q("SELECT * FROM orders")), 1)
        self.assertIn(("failed",), self.q("SELECT status FROM etl_audit"))


if __name__ == "__main__":
    unittest.main()

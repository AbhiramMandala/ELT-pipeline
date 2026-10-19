"""DAG validation without a live scheduler: stub `airflow` modules, import the
DAG file, assert structure/retries/deps, then drive the tasks end-to-end
through the real XCom code path with a fake TaskInstance."""
import csv
import importlib.util
import os
import sys
import tempfile
import types
import unittest
from datetime import timedelta
from types import SimpleNamespace

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

_STACK = []


class _DAG:
    def __init__(self, dag_id, **kw):
        self.dag_id = dag_id
        self.kwargs = kw
        self.tasks = []

    def __enter__(self):
        _STACK.append(self)
        return self

    def __exit__(self, *a):
        _STACK.pop()
        return False


class _Op:
    def __init__(self, task_id, python_callable, **kw):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kw = kw
        self.downstream, self.upstream = [], []
        _STACK[-1].tasks.append(self)

    def __rshift__(self, other):
        self.downstream.append(other.task_id)
        other.upstream.append(self.task_id)
        return other


def _load_dag():
    mods = {"airflow": types.ModuleType("airflow"),
            "airflow.operators": types.ModuleType("airflow.operators"),
            "airflow.operators.python": types.ModuleType("airflow.operators.python"),
            "airflow.providers": types.ModuleType("airflow.providers"),
            "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
            "airflow.providers.standard.operators":
                types.ModuleType("airflow.providers.standard.operators"),
            "airflow.providers.standard.operators.python":
                types.ModuleType("airflow.providers.standard.operators.python")}
    for name, mod in mods.items():
        sys.modules[name] = mod
    sys.modules["airflow"].DAG = _DAG
    sys.modules["airflow"].__path__ = []
    # Airflow 3.x primary path (DAG tries this first) + 2.x fallback path
    sys.modules["airflow.providers.standard.operators.python"].PythonOperator = _Op
    sys.modules["airflow.operators.python"].PythonOperator = _Op
    sys.modules["airflow.operators"].python = sys.modules["airflow.operators.python"]
    for m in list(sys.modules):
        if m == "orders_etl_dag" or m.startswith("orders_etl_dag."):
            del sys.modules[m]
    spec = importlib.util.spec_from_file_location(
        "orders_etl_dag", os.path.join(REPO, "dags", "orders_etl_dag.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["orders_etl_dag"] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeTI:
    def __init__(self, store, task):
        self.store, self.task, self.try_number = store, task, 2

    def xcom_push(self, key, value):
        self.store[(self.task, key)] = value

    def xcom_pull(self, task_ids, key):
        return self.store.get((task_ids, key))


class TestDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = _load_dag()

    def test_structure(self):
        dag = self.mod.dag
        self.assertEqual(dag.dag_id, "orders_etl")
        self.assertEqual(dag.kwargs["schedule"], "@daily")
        self.assertFalse(dag.kwargs["catchup"])
        self.assertEqual(dag.kwargs["max_active_runs"], 1)
        self.assertEqual([t.task_id for t in dag.tasks],
                         ["extract", "transform", "load", "audit"])
        by_id = {t.task_id: t for t in dag.tasks}
        self.assertEqual(by_id["extract"].downstream, ["transform"])
        self.assertEqual(by_id["transform"].downstream, ["load"])
        self.assertEqual(by_id["load"].downstream, ["audit"])
        self.assertEqual(by_id["audit"].upstream, ["load"])
        args = dag.kwargs["default_args"]
        self.assertEqual(args["retries"], 3)
        self.assertEqual(args["retry_delay"], timedelta(minutes=5))
        self.assertEqual(args["execution_timeout"], timedelta(minutes=30))

    def test_import_has_no_side_effects(self):
        # importing must not run the pipeline: no warehouse/audit writes at import
        self.assertTrue(self.mod.AIRFLOW_AVAILABLE)
        self.assertEqual(self.mod.TI, {})

    def test_xcom_end_to_end(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            src = os.path.join(tmp.name, "in.csv")
            with open(src, "w", encoding="utf-8") as f:
                f.write("order_id,customer_name,amount,currency,updated_at,status\n"
                        "1,Ann,10.00,USD,2026-10-01T10:00:00,completed\n"
                        "2,Bo,5.00,USD,2026-10-02T10:00:00,completed\n")
            env = {"ETL_SOURCE_CSV": src,
                   "ETL_TARGET_DB": os.path.join(tmp.name, "w.db"),
                   "ETL_WATERMARK_FILE": os.path.join(tmp.name, "wm.json"),
                   "ETL_AUDIT_FILE": os.path.join(tmp.name, "audit.csv")}
            old = {k: os.environ.get(k) for k in env}
            os.environ.update(env)
            try:
                store = {}
                ctx = lambda task: {"ti": FakeTI(store, task),  # noqa: E731
                                    "dag_run": SimpleNamespace(run_id="test-run-1")}
                self.assertEqual(self.mod.t_extract(**ctx("extract")), 2)
                self.assertEqual(self.mod.t_transform(**ctx("transform")), 2)
                self.mod.t_load(**ctx("load"))
                self.assertEqual(self.mod.t_audit(**ctx("audit")), "success")
            finally:
                for k, v in old.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v
            with open(os.path.join(tmp.name, "audit.csv"), newline="",
                      encoding="utf-8") as f:
                audits = list(csv.DictReader(f))
            self.assertEqual(len(audits), 1)
            self.assertEqual(audits[0]["run_id"], "test-run-1")
            self.assertEqual(audits[0]["task_id"], "audit")
            self.assertEqual(audits[0]["attempt"], "2")
            self.assertEqual(audits[0]["status"], "success")
            self.assertEqual(int(audits[0]["inserted"]), 2)
            import sqlite3
            with sqlite3.connect(os.path.join(tmp.name, "w.db")) as con:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 2)
                db_audit = con.execute(
                    "SELECT run_id,task_id,status,inserted FROM etl_audit").fetchall()
            self.assertEqual(db_audit, [("test-run-1", "audit", "success", 2)])
        finally:
            try:
                tmp.cleanup()
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()

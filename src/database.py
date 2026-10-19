"""Database backend abstraction: SQLite (default) + PostgreSQL (optional).

DBAPI only (stdlib ``sqlite3`` / ``psycopg``) — no ORM. SQL is written once
with SQLite-style ``:named`` parameters; :func:`adapt` rewrites them to
``%(name)s`` for PostgreSQL. Always use ``conn.cursor()`` (psycopg
connections have no ``.execute``); always pass row values as dicts.
"""
import os
import re
import sqlite3

import config as cfg

_PARAM = re.compile(r":([A-Za-z_]\w*)")  # our SQL has no other colons (no casts/slices)


def _env(name, default):
    return os.environ.get(name, getattr(cfg, name, default))


def backend():
    b = str(_env("ETL_DB_BACKEND", "sqlite")).lower()
    if b in ("postgres", "postgresql"):
        return "postgres"
    if b == "sqlite":
        return "sqlite"
    raise ValueError(f"unsupported ETL_DB_BACKEND: {b!r} (use 'sqlite' or 'postgres')")


def pg_kwargs():
    kw = {"host": _env("ETL_PG_HOST", "localhost"),
          "port": int(_env("ETL_PG_PORT", "5432")),
          "dbname": _env("ETL_PG_DBNAME", "etl"),
          "user": _env("ETL_PG_USER", "etl"),
          "connect_timeout": 10}
    pwd = os.environ.get("ETL_PG_PASSWORD", "")  # never logged, never in labels
    if pwd:
        kw["password"] = pwd
    return kw


def connect(db_path=None):
    """Open a DBAPI connection for the configured backend."""
    if backend() == "postgres":
        try:
            import psycopg
        except ImportError as e:
            raise RuntimeError(
                "ETL_DB_BACKEND=postgres requires psycopg: "
                "pip install 'psycopg[binary]'") from e
        return psycopg.connect(**pg_kwargs())
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent or ".", exist_ok=True)
    return sqlite3.connect(db_path, timeout=30, isolation_level=None)


def adapt(sql):
    """Translate :named params to %(name)s on PostgreSQL; identity on SQLite."""
    if backend() == "postgres":
        return _PARAM.sub(r"%(\1)s", sql)
    return sql


def begin(conn):
    """Start a write transaction: BEGIN IMMEDIATE on SQLite (writer lock up
    front so concurrent processes serialize instead of dying mid-batch);
    no-op on PostgreSQL (implicit transaction per connection)."""
    if backend() == "sqlite":
        conn.execute("BEGIN IMMEDIATE")


def target_label(db_path):
    if backend() == "postgres":
        kw = pg_kwargs()
        return f"postgresql://{kw['host']}:{kw['port']}/{kw['dbname']}"
    return db_path


def _statements(filename):
    # Minimal script runner: strip -- line comments, then split on ";".
    # Statements must not contain interior semicolons (no functions/triggers here).
    path = os.path.join(cfg.SQL_DIR, filename)
    with open(path, encoding="utf-8") as f:
        code = "\n".join(
            line.split("--", 1)[0] for line in f.read().splitlines())
        return [s.strip() for s in code.split(";") if s.strip()]


def ensure_schema(db_path):
    """Create target + state + audit tables if missing (runs the sql/ scripts)."""
    conn = connect(db_path)
    try:
        cur = conn.cursor()
        for stmt in _statements("create_tables.sql") + _statements("create_audit_table.sql"):
            cur.execute(adapt(stmt))
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

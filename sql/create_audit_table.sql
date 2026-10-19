-- Execution audit table for the orders ETL pipeline.
-- Portable DDL: runs unchanged on SQLite and PostgreSQL.
-- One row per (run_id, task_id). The composite primary key makes audit
-- writes idempotent: a retried task re-reports the same row instead of
-- duplicating it (writers use ON CONFLICT DO NOTHING / INSERT OR IGNORE).

CREATE TABLE IF NOT EXISTS etl_audit (
    run_id            TEXT NOT NULL,
    dag_id            TEXT NOT NULL,
    task_id           TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    completed_at      TEXT NOT NULL,
    status            TEXT NOT NULL,
    source            TEXT NOT NULL,
    target            TEXT NOT NULL,
    extracted         INTEGER NOT NULL DEFAULT 0,
    processed         INTEGER NOT NULL DEFAULT 0,
    inserted          INTEGER NOT NULL DEFAULT 0,
    updated           INTEGER NOT NULL DEFAULT 0,
    skipped_unchanged INTEGER NOT NULL DEFAULT 0,
    skipped_stale     INTEGER NOT NULL DEFAULT 0,
    rejected          INTEGER NOT NULL DEFAULT 0,
    error             TEXT NOT NULL DEFAULT '',
    attempt           TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, task_id)
);

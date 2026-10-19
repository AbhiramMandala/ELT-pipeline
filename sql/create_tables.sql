-- Target tables for the orders ETL pipeline.
-- Portable DDL: runs unchanged on SQLite and PostgreSQL.
-- order_id is always supplied by the pipeline (never generated), so no
-- AUTOINCREMENT / SERIAL is needed on either backend.
-- etl_state holds the composite watermark cursor (watermark_ts,
-- watermark_pk); it is written inside the same transaction as the orders
-- UPSERT, so the cursor can never advance past uncommitted data.

CREATE TABLE IF NOT EXISTS orders (
    order_id     INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    amount       REAL NOT NULL,
    currency     TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    status       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_state (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

"""Central config. All overridable via env vars. No secrets hardcoded."""
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("ETL_DATA_DIR", os.path.join(BASE_DIR, "data"))
STATE_DIR = os.environ.get("ETL_STATE_DIR", os.path.join(BASE_DIR, "state"))
AUDIT_DIR = os.environ.get("ETL_AUDIT_DIR", os.path.join(BASE_DIR, "audit"))

SOURCE_CSV = os.environ.get("ETL_SOURCE_CSV", os.path.join(DATA_DIR, "sample_orders.csv"))
TARGET_DB = os.environ.get("ETL_TARGET_DB", os.path.join(BASE_DIR, "warehouse.db"))
WATERMARK_FILE = os.environ.get("ETL_WATERMARK_FILE", os.path.join(STATE_DIR, "watermark.json"))
AUDIT_FILE = os.environ.get("ETL_AUDIT_FILE", os.path.join(AUDIT_DIR, "audit_log.csv"))

WATERMARK_COLUMN = "updated_at"
PK_COLUMN = "order_id"

SQL_DIR = os.path.join(BASE_DIR, "sql")

# Database backend: 'sqlite' (default, zero-dependency local) or 'postgres'.
# All other settings are read live from the environment (see src/database.py),
# so tests and tasks can switch backends per run. No secrets here.
DB_BACKEND = os.environ.get("ETL_DB_BACKEND", "sqlite")
PG_HOST = os.environ.get("ETL_PG_HOST", "localhost")
try:
    PG_PORT = int(os.environ.get("ETL_PG_PORT", "5432"))
except ValueError:
    PG_PORT = 5432
PG_DBNAME = os.environ.get("ETL_PG_DBNAME", "etl")
PG_USER = os.environ.get("ETL_PG_USER", "etl")
# ETL_PG_PASSWORD is read only inside src/database.py and never logged.

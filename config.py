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

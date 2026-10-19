"""Transformation stage: cleaning, normalization, type conversion, dedupe."""
from datetime import datetime

REQUIRED_COLUMNS = ["order_id", "customer_name", "amount", "currency", "updated_at", "status"]
VALID_STATUSES = {"completed", "pending", "cancelled"}


def transform(rows):
    """Clean, normalize, type-convert, dedupe. Returns (clean_rows, rejected)."""
    clean, rejected, seen = [], [], set()
    for r in rows:
        try:
            oid = str(r.get("order_id", "")).strip()
            if not oid or not oid.isdigit():
                raise ValueError(f"invalid order_id: {r.get('order_id')!r}")
            if oid in seen:
                raise ValueError(f"duplicate order_id in batch: {oid}")
            seen.add(oid)
            customer = str(r.get("customer_name", "")).strip()
            if not customer:
                raise ValueError(f"order {oid}: empty customer_name")
            amount = float(str(r.get("amount", "")).strip())
            if amount <= 0:
                raise ValueError(f"order {oid}: amount must be > 0, got {amount}")
            currency = str(r.get("currency", "")).strip().upper()
            if len(currency) != 3:
                raise ValueError(f"order {oid}: invalid currency {r.get('currency')!r}")
            updated_at = str(r.get("updated_at", "")).strip()
            datetime.fromisoformat(updated_at)  # validates ISO-8601
            status = str(r.get("status", "")).strip().lower() or "pending"
            if status not in VALID_STATUSES:
                raise ValueError(f"order {oid}: invalid status {r.get('status')!r}")
            clean.append({"order_id": int(oid), "customer_name": customer,
                          "amount": amount, "currency": currency,
                          "updated_at": updated_at, "status": status})
        except (ValueError, TypeError) as e:
            rejected.append({"row": r, "error": str(e)})
    return clean, rejected

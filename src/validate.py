"""Validation stage: final gate before loading (required cols, nulls, dup PKs)."""
from src.transform import REQUIRED_COLUMNS  # single definition lives in transform

VALIDATE_COLUMNS = REQUIRED_COLUMNS


def validate(rows):
    """Final gate: required cols, nulls, dup PKs. Returns (valid, errors)."""
    valid, errors = [], []
    seen = set()
    for r in rows:
        missing = [c for c in REQUIRED_COLUMNS if r.get(c) in (None, "")]
        if missing:
            errors.append({"order_id": r.get("order_id"), "error": f"missing/null: {missing}"})
            continue
        if r["order_id"] in seen:
            errors.append({"order_id": r["order_id"], "error": "duplicate primary key"})
            continue
        seen.add(r["order_id"])
        valid.append(r)
    return valid, errors

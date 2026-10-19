"""Extraction stage: CSV source -> list of raw row dicts."""
import csv
import logging
import os

from src.transform import REQUIRED_COLUMNS

log = logging.getLogger("etl")


def extract(source_csv):
    """Read CSV into list of dicts.

    Batch-level failures (missing file, missing header, missing required
    columns) raise and fail the whole run. Individual bad rows are handled
    later by transform() and only rejected, not fatal.
    """
    if not os.path.exists(source_csv):
        raise FileNotFoundError(f"Source file not found: {source_csv}")
    # utf-8-sig transparently strips a BOM (typical Excel export) and reads
    # plain UTF-8 identically, so BOM-prefixed files don't fail the batch.
    # Rows stream from the reader; only the final list is materialized, so
    # memory stays proportional to the batch, not the source file size.
    with open(source_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"source has no header row: {source_csv}")
        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"source missing required columns {missing}: {source_csv}")
        rows = list(reader)
    log.info("extracted=%d from %s", len(rows), source_csv)
    return rows

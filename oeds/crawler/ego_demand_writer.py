#!/usr/bin/env python3
# SPDX-FileCopyrightText: ASSUME/OEDS Developers
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

import csv
import sys

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, ContinuousCrawler, load_config

csv.field_size_limit(min(sys.maxsize, 1024 * 1024 * 1024))# increase the python csv field size limit
log = logging.getLogger("ego_demand")
log.setLevel(logging.INFO)

# Columns required to keep get_demand_in_area() working unchanged
REQUIRED_COLS = [
    "nuts",
    "sector_consumption_residential",
    "sector_consumption_retail",
    "sector_consumption_industrial",
    "sector_consumption_agricultural",
]

# Columns we use for idempotency
KEY_COLS = ["version", "id"]

# We accept either a version column in the CSV or inject it
DEFAULT_VERSION = "v0.4.5"


DDL = """
CREATE TABLE IF NOT EXISTS demand (
    version TEXT NOT NULL,
    id BIGINT NOT NULL,
    subst_id BIGINT,
    nuts TEXT,
    sector_consumption_residential DOUBLE PRECISION,
    sector_consumption_retail      DOUBLE PRECISION,
    sector_consumption_industrial  DOUBLE PRECISION,
    sector_consumption_agricultural DOUBLE PRECISION,
    source TEXT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT demand_pk PRIMARY KEY (version, id)
);
"""

INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS demand_version_nuts_like_idx
    ON demand (version, nuts text_pattern_ops);
    """,
    """
    CREATE INDEX IF NOT EXISTS demand_nuts_like_idx
    ON demand (nuts text_pattern_ops);
    """,
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Keep original casing from OEP exports, but normalize whitespace of column name
    df.columns = [c.strip() for c in df.columns]
    return df


def _validate_and_prepare_chunk(
    df: pd.DataFrame, default_version: str = DEFAULT_VERSION
) -> pd.DataFrame:
    df = _normalize_columns(df)

    # Ensure version(Not sure if we need this )
    if "version" not in df.columns:
        df["version"] = default_version
    else:
        # Fill missing version entries
        df["version"] = df["version"].fillna(default_version)
        df.loc[df["version"].astype(str).str.strip() == "", "version"] = default_version

    # Must have id for upsert key
    if "id" not in df.columns:
        raise ValueError("CSV must contain column 'id' (unique identifier).")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")

    # Coerce types
    df["id"] = pd.to_numeric(df["id"], errors="raise").astype("int64")

    # Optional columns
    if "subst_id" in df.columns:
        df["subst_id"] = pd.to_numeric(df["subst_id"], errors="coerce")
    else:
        df["subst_id"] = None

    df["nuts"] = df["nuts"].astype(str).where(df["nuts"].notna(), None)

    # sector consumption: floats; treat NaN as 0.0 (safe for sums)
    for c in [
        "sector_consumption_residential",
        "sector_consumption_retail",
        "sector_consumption_industrial",
        "sector_consumption_agricultural",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Optional bookkeeping
    if "source" not in df.columns:
        df["source"] = "local_csv"

    # Keep only the columns we actually write (plus key)
    keep = [
        "version",
        "id",
        "subst_id",
        "nuts",
        "sector_consumption_residential",
        "sector_consumption_retail",
        "sector_consumption_industrial",
        "sector_consumption_agricultural",
        "source",
    ]
    return df[keep]


def _upsert_chunk(conn, df: pd.DataFrame) -> Tuple[int, int]:
    """
    Upsert into demand using (version,id) PK.
    Returns (inserted_or_updated_rows, updated_rows_estimate).
    Postgres doesn't easily return split counts without extra work; we provide total affected.
    """

    # Create a temp staging table for this transaction
    conn.execute(text("""
        CREATE TEMP TABLE IF NOT EXISTS demand_stage (
            version TEXT NOT NULL,
            id BIGINT NOT NULL,
            subst_id BIGINT,
            nuts TEXT,
            sector_consumption_residential DOUBLE PRECISION,
            sector_consumption_retail      DOUBLE PRECISION,
            sector_consumption_industrial  DOUBLE PRECISION,
            sector_consumption_agricultural DOUBLE PRECISION,
            source TEXT
        ) ON COMMIT DROP;
    """))

    # Bulk insert stage via pandas to_sql (fast enough for typical CSV sizes; can be swapped for COPY later)
    df.to_sql("demand_stage", conn, if_exists="append", index=False, method="multi", chunksize=10_000)

    # Upsert from stage to target
    res = conn.execute(text("""
        INSERT INTO demand (
            version, id, subst_id, nuts,
            sector_consumption_residential,
            sector_consumption_retail,
            sector_consumption_industrial,
            sector_consumption_agricultural,
            source
        )
        SELECT
            version, id, subst_id, nuts,
            sector_consumption_residential,
            sector_consumption_retail,
            sector_consumption_industrial,
            sector_consumption_agricultural,
            source
        FROM demand_stage
        ON CONFLICT (version, id) DO UPDATE SET
            subst_id = EXCLUDED.subst_id,
            nuts = EXCLUDED.nuts,
            sector_consumption_residential = EXCLUDED.sector_consumption_residential,
            sector_consumption_retail      = EXCLUDED.sector_consumption_retail,
            sector_consumption_industrial  = EXCLUDED.sector_consumption_industrial,
            sector_consumption_agricultural = EXCLUDED.sector_consumption_agricultural,
            source = EXCLUDED.source,
            imported_at = now();
    """))

    # rowcount is total affected (inserted + updated)
    affected = res.rowcount if res.rowcount is not None else len(df)
    return int(affected), 0


class EgoDemandWriter(ContinuousCrawler):
    """
    OEDS-style writer that imports OpenEGO ego_dp_loadarea CSV into table 'demand'
    so ASSUME's get_demand_in_area() works unchanged.
    """

    def __init__(self, schema_name: str, config):
        super().__init__(schema_name, config)

    def init_db(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(DDL))
            for idx in INDEXES:
                conn.execute(text(idx))
        log.info("demand table ready")

    def import_csv(
        self,
        csv_path: str | Path,
        version: str = DEFAULT_VERSION,
        chunksize: int = 50_000,
    ) -> None:
        csv_path = Path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        self.init_db()

        total_rows = 0
        total_affected = 0

        log.info("importing demand CSV: %s", csv_path)

        # Stream read for large files
        reader = pd.read_csv(
            csv_path,
            chunksize=chunksize, #seperate the csv into chunks with given number of rows to reduce the memory usage while writing into sql,
            engine="python",# force pandas to use "python" csv engine to avoid segmentation fault caused by C parser

        )

        for chunk in reader:
            with self.engine.begin() as conn:
                prepared = _validate_and_prepare_chunk(chunk, default_version=version)
                affected, _ = _upsert_chunk(conn, prepared)
                total_rows += len(prepared)
                total_affected += affected
                log.info("processed chunk rows=%d affected=%d", len(prepared), affected)

        log.info(
            "finished demand import: total_rows=%d total_affected(insert+update)=%d",
            total_rows,
            total_affected,
        )

    def crawl_temporal(self, *args, **kwargs):
        raise NotImplementedError("This writer is not temporal. Call import_csv().")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    config = load_config(DEFAULT_CONFIG_LOCATION)

    # Example:
    # python crawler/ego_demand_writer.py /data/ego_dp_loadarea.csv


    csv_path = Path(__file__).parent.parent/"data"/ "ego_dp_loadarea.csv"
    version = DEFAULT_VERSION

    writer = EgoDemandWriter("oep", config=config)
    writer.import_csv(csv_path, version=version)

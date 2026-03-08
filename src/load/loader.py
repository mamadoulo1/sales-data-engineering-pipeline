"""
Loader module — persists transformed DataFrames to Parquet files and SQLite.
"""

import os
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine


class Loader:
    """Writes processed DataFrames to Parquet (or CSV) and a SQLite database."""

    def __init__(self, config: dict):
        self.processed_path = config["paths"]["processed_data"]
        self.output_path = config["paths"]["output_data"]
        self.load_cfg = config["load"]
        os.makedirs(self.processed_path, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, transformed: dict[str, pd.DataFrame]) -> None:
        for table_name, df in transformed.items():
            self._write_file(df, table_name)
            self._write_db(df, table_name)
        logger.info("Load phase complete.")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _write_file(self, df: pd.DataFrame, name: str) -> None:
        fmt = self.load_cfg["output_format"]
        if fmt == "parquet":
            path = os.path.join(self.processed_path, f"{name}.parquet")
            df.to_parquet(
                path,
                index=False,
                compression=self.load_cfg["parquet_compression"],
            )
        else:
            path = os.path.join(self.processed_path, f"{name}.csv")
            df.to_csv(path, index=False)
        logger.info(f"  Written {len(df)} rows -> {path}")

    def _write_db(self, df: pd.DataFrame, name: str) -> None:
        db_path = self.load_cfg["db_path"]
        engine = create_engine(f"sqlite:///{db_path}")
        df_db = df.copy()
        # SQLite doesn't support Period dtype — convert to str
        period_cols = [col for col in df_db.columns if hasattr(df_db[col].dtype, "freq")]
        for col in period_cols:
            df_db[col] = df_db[col].astype(str)
        df_db.to_sql(name, engine, if_exists="replace", index=False)
        logger.info(f"  Loaded table '{name}' into SQLite ({db_path})")

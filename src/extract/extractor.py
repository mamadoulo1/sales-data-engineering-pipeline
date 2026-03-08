"""
Extractor module — reads raw CSV source files into pandas DataFrames.
"""

import os
import pandas as pd
from loguru import logger


class Extractor:
    """Loads raw CSV files from the configured raw data directory."""

    def __init__(self, config: dict):
        self.raw_path = config["paths"]["raw_data"]
        self.ext_cfg = config["extract"]

    def _read_csv(self, filename: str) -> pd.DataFrame:
        filepath = os.path.join(self.raw_path, filename)
        logger.info(f"Extracting: {filepath}")
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Source file not found: {filepath}")
        df = pd.read_csv(
            filepath,
            encoding=self.ext_cfg["encoding"],
            delimiter=self.ext_cfg["delimiter"],
        )
        logger.info(f"  -> {len(df)} rows loaded from {filename}")
        return df

    def extract_orders(self) -> pd.DataFrame:
        return self._read_csv(self.ext_cfg["orders_file"])

    def extract_customers(self) -> pd.DataFrame:
        return self._read_csv(self.ext_cfg["customers_file"])

    def extract_products(self) -> pd.DataFrame:
        return self._read_csv(self.ext_cfg["products_file"])

    def extract_all(self) -> dict[str, pd.DataFrame]:
        return {
            "orders": self.extract_orders(),
            "customers": self.extract_customers(),
            "products": self.extract_products(),
        }

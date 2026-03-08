"""
Pipeline orchestrator — wires Extract → Transform → Load together.
"""

import argparse
import time
import yaml
from loguru import logger

from extract.extractor import Extractor
from transform.transformer import Transformer
from load.loader import Loader


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class Pipeline:
    """Orchestrates the full ETL pipeline."""

    def __init__(self, config_path: str = "config/config.yaml"):
        self.config = load_config(config_path)
        self.extractor = Extractor(self.config)
        self.transformer = Transformer(self.config)
        self.loader = Loader(self.config)

    def run(self) -> dict:
        name = self.config["pipeline"]["name"]
        version = self.config["pipeline"]["version"]
        logger.info(f"=== Pipeline '{name}' v{version} starting ===")
        start = time.perf_counter()

        # --- Extract ---
        logger.info("--- EXTRACT ---")
        raw = self.extractor.extract_all()

        # --- Transform ---
        logger.info("--- TRANSFORM ---")
        transformed = self.transformer.transform(raw)

        # --- Load ---
        logger.info("--- LOAD ---")
        self.loader.load(transformed)

        elapsed = time.perf_counter() - start
        logger.success(f"=== Pipeline finished in {elapsed:.2f}s ===")
        return transformed


def main():
    """Entry point for Databricks Python Wheel task."""
    parser = argparse.ArgumentParser(description="Sales Data Engineering Pipeline")
    parser.add_argument("--config", default="config/config.yaml")
    args, _ = parser.parse_known_args()

    pipeline = Pipeline(config_path=args.config)
    pipeline.run()

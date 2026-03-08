"""
Entry point — run the sales data engineering pipeline.

Usage:
    python main.py
    python main.py --config config/config.yaml
"""

import argparse
import sys
from loguru import logger

from pipeline import Pipeline


def setup_logger() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
        colorize=True,
        level="DEBUG",
    )
    logger.add(
        "data/output/pipeline.log",
        rotation="10 MB",
        retention="7 days",
        level="INFO",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sales Data Engineering Pipeline")
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to the YAML configuration file",
    )
    return parser.parse_args()


def main() -> None:
    setup_logger()
    args = parse_args()

    pipeline = Pipeline(config_path=args.config)
    results = pipeline.run()

    # Print a quick preview of each output table
    for table_name, df in results.items():
        logger.info(f"\n--- Preview: {table_name} ({len(df)} rows) ---")
        print(df.head(5).to_string(index=False))
        print()


if __name__ == "__main__":
    main()

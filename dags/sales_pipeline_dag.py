"""
Sales Data Engineering Pipeline — Airflow DAG

Architecture:
  extract_task → transform_task → validate_task → load_task

Data is passed between tasks via intermediate pickle files stored in
/tmp/airflow_sales/{run_id}/, so each task can be retried independently
without re-running the whole pipeline.
"""

from __future__ import annotations

import os
import pickle
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Resolve project root so imports work whether run inside Docker or locally.
# The PYTHONPATH env var set in docker-compose also handles this, but this
# fallback guarantees it even for bare-metal setups.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

CONFIG_PATH = str(PROJECT_ROOT / "config" / "config.yaml")

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Helper — each run gets its own isolated temp directory
# ---------------------------------------------------------------------------
def _tmp_dir(run_id: str) -> Path:
    path = Path(f"/tmp/airflow_sales/{run_id}")
    path.mkdir(parents=True, exist_ok=True)
    return path


# ===========================================================================
# DAG definition (TaskFlow API — Airflow 2.x)
# ===========================================================================
@dag(
    dag_id="sales_data_pipeline",
    description="ETL pipeline: CSV → Parquet / SQLite via pandas",
    schedule_interval="@daily",          # run every day at midnight
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl", "data-engineering"],
    default_args=default_args,
    doc_md=__doc__,
)
def sales_data_pipeline():

    # -----------------------------------------------------------------------
    # TASK 1 — EXTRACT
    # -----------------------------------------------------------------------
    @task(task_id="extract")
    def extract_task(**context) -> str:
        """
        Read raw CSV files (orders, customers, products) and persist them as
        a pickle file so the transform task can load them back independently.
        Returns the path to the pickle file (stored in XCom as a string).
        """
        import yaml
        from extract.extractor import Extractor

        run_id = context["run_id"]
        tmp = _tmp_dir(run_id)

        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Make paths absolute relative to the project root
        for key in ("raw_data", "processed_data", "output_data"):
            config["paths"][key] = str(PROJECT_ROOT / config["paths"][key])

        extractor = Extractor(config)
        raw = extractor.extract_all()

        out_path = str(tmp / "raw_data.pkl")
        with open(out_path, "wb") as f:
            pickle.dump(raw, f, protocol=pickle.HIGHEST_PROTOCOL)

        # Log shape of each frame
        for name, df in raw.items():
            print(f"[EXTRACT] {name}: {df.shape[0]} rows × {df.shape[1]} cols")

        return out_path

    # -----------------------------------------------------------------------
    # TASK 2 — TRANSFORM
    # -----------------------------------------------------------------------
    @task(task_id="transform")
    def transform_task(raw_data_path: str, **context) -> str:
        """
        Load the raw DataFrames produced by the extract task, apply all
        cleaning / enrichment / aggregation steps, and persist the result.
        Returns the path to the transformed pickle file.
        """
        import yaml
        from transform.transformer import Transformer

        run_id = context["run_id"]
        tmp = _tmp_dir(run_id)

        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        with open(raw_data_path, "rb") as f:
            raw = pickle.load(f)

        transformer = Transformer(config)
        transformed = transformer.transform(raw)

        out_path = str(tmp / "transformed_data.pkl")
        with open(out_path, "wb") as f:
            pickle.dump(transformed, f, protocol=pickle.HIGHEST_PROTOCOL)

        for name, df in transformed.items():
            print(f"[TRANSFORM] {name}: {df.shape[0]} rows × {df.shape[1]} cols")

        return out_path

    # -----------------------------------------------------------------------
    # TASK 3 — VALIDATE
    # -----------------------------------------------------------------------
    @task(task_id="validate")
    def validate_task(transformed_data_path: str, **context) -> str:
        """
        Run lightweight data-quality checks before writing to storage:
          - orders_enriched must have rows
          - net_revenue must be non-negative
          - sales_summary must have rows
        Raises ValueError on any failure, which marks the task (and its
        downstream load task) as failed without touching output storage.
        """
        with open(transformed_data_path, "rb") as f:
            transformed = pickle.load(f)

        orders = transformed.get("orders_enriched")
        summary = transformed.get("sales_summary")

        errors = []

        if orders is None or orders.empty:
            errors.append("orders_enriched is empty")
        else:
            if (orders["net_revenue"] < 0).any():
                neg_count = (orders["net_revenue"] < 0).sum()
                errors.append(f"{neg_count} rows have negative net_revenue")

        if summary is None or summary.empty:
            errors.append("sales_summary is empty")

        if errors:
            raise ValueError(f"[VALIDATE] Data quality check FAILED: {errors}")

        print(f"[VALIDATE] All checks passed — "
              f"{len(orders)} orders, {len(summary)} summary rows")

        return transformed_data_path   # pass through to load task

    # -----------------------------------------------------------------------
    # TASK 4 — LOAD
    # -----------------------------------------------------------------------
    @task(task_id="load")
    def load_task(transformed_data_path: str, **context) -> None:
        """
        Persist the transformed DataFrames to Parquet files and SQLite.
        Cleans up the temporary pickle files after a successful load.
        """
        import yaml
        from load.loader import Loader

        run_id = context["run_id"]
        tmp = _tmp_dir(run_id)

        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Make paths absolute relative to the project root
        for key in ("raw_data", "processed_data", "output_data"):
            config["paths"][key] = str(PROJECT_ROOT / config["paths"][key])

        # Also fix the db_path
        config["load"]["db_path"] = str(
            PROJECT_ROOT / config["load"]["db_path"]
        )

        with open(transformed_data_path, "rb") as f:
            transformed = pickle.load(f)

        loader = Loader(config)
        loader.load(transformed)

        # Clean up temp files for this run
        for pkl in tmp.glob("*.pkl"):
            pkl.unlink(missing_ok=True)
        print(f"[LOAD] Finished — temp files cleaned up in {tmp}")

    # -----------------------------------------------------------------------
    # Wire tasks together
    # -----------------------------------------------------------------------
    raw_path = extract_task()
    transformed_path = transform_task(raw_path)
    validated_path = validate_task(transformed_path)
    load_task(validated_path)


# Instantiate the DAG
dag_instance = sales_data_pipeline()

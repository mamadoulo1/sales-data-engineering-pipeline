import os
import pytest
import pandas as pd
import tempfile
from load.loader import Loader


def make_config(tmp_dir):
    return {
        "paths": {
            "processed_data": os.path.join(tmp_dir, "processed"),
            "output_data": os.path.join(tmp_dir, "output"),
        },
        "load": {
            "output_format": "parquet",
            "db_path": os.path.join(tmp_dir, "output", "sales.db"),
            "parquet_compression": "snappy",
        },
    }


@pytest.fixture
def sample_data():
    return {
        "orders_enriched": pd.DataFrame({
            "order_id": ["O001", "O002"],
            "net_revenue": [100.0, 200.0],
            "status": ["delivered", "shipped"],
        }),
        "sales_summary": pd.DataFrame({
            "order_year_month": ["2024-01", "2024-02"],
            "category": ["Electronics", "Furniture"],
            "total_orders": [5, 3],
            "net_revenue": [5000.0, 1200.0],
        }),
    }


class TestLoader:
    def test_parquet_files_created(self, sample_data):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            loader = Loader(config)
            loader.load(sample_data)

            for table in sample_data:
                path = os.path.join(tmp, "processed", f"{table}.parquet")
                assert os.path.exists(path), f"Missing parquet: {path}"

    def test_sqlite_db_created(self, sample_data):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            loader = Loader(config)
            loader.load(sample_data)

            db_path = os.path.join(tmp, "output", "sales.db")
            assert os.path.exists(db_path)

    def test_parquet_roundtrip(self, sample_data):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            loader = Loader(config)
            loader.load(sample_data)

            path = os.path.join(tmp, "processed", "orders_enriched.parquet")
            df = pd.read_parquet(path)
            assert len(df) == 2
            assert "order_id" in df.columns

    def test_csv_output_format(self, sample_data):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            config["load"]["output_format"] = "csv"
            loader = Loader(config)
            loader.load(sample_data)

            path = os.path.join(tmp, "processed", "orders_enriched.csv")
            assert os.path.exists(path)
            df = pd.read_csv(path)
            assert len(df) == 2

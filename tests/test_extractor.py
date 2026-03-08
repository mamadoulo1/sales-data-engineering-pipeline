import os
import pytest
import pandas as pd
from unittest.mock import patch
from extract.extractor import Extractor

BASE_CONFIG = {
    "paths": {"raw_data": "data/raw"},
    "extract": {
        "orders_file": "orders.csv",
        "customers_file": "customers.csv",
        "products_file": "products.csv",
        "encoding": "utf-8",
        "delimiter": ",",
    },
}


class TestExtractor:
    def setup_method(self):
        self.extractor = Extractor(BASE_CONFIG)

    def test_extract_orders_returns_dataframe(self):
        df = self.extractor.extract_orders()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_orders_expected_columns(self):
        df = self.extractor.extract_orders()
        expected = {"order_id", "customer_id", "product_id", "quantity", "order_date", "status"}
        assert expected.issubset(set(df.columns))

    def test_customers_expected_columns(self):
        df = self.extractor.extract_customers()
        expected = {"customer_id", "first_name", "last_name", "email", "country"}
        assert expected.issubset(set(df.columns))

    def test_products_expected_columns(self):
        df = self.extractor.extract_products()
        expected = {"product_id", "name", "category", "unit_price", "stock"}
        assert expected.issubset(set(df.columns))

    def test_extract_all_returns_three_keys(self):
        result = self.extractor.extract_all()
        assert set(result.keys()) == {"orders", "customers", "products"}

    def test_missing_file_raises_error(self):
        bad_config = {
            "paths": {"raw_data": "data/raw"},
            "extract": {
                "orders_file": "nonexistent.csv",
                "customers_file": "customers.csv",
                "products_file": "products.csv",
                "encoding": "utf-8",
                "delimiter": ",",
            },
        }
        extractor = Extractor(bad_config)
        with pytest.raises(FileNotFoundError):
            extractor.extract_orders()

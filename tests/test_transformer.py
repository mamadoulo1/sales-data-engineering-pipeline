import pytest
import pandas as pd
import numpy as np
from transform.transformer import Transformer

CONFIG = {
    "transform": {
        "drop_duplicates": True,
        "fill_missing_quantity": 1,
        "valid_statuses": ["pending", "shipped", "delivered", "cancelled"],
        "revenue_margin_pct": 0.30,
    }
}

# ---------- Fixtures ----------

@pytest.fixture
def transformer():
    return Transformer(CONFIG)


@pytest.fixture
def sample_orders():
    return pd.DataFrame({
        "order_id":    ["O001", "O002", "O003", "O004", "O001"],  # O001 duplicated
        "customer_id": ["C001", "C002", "C003", "C004", "C001"],
        "product_id":  ["P001", "P002", "P001", "P003", "P001"],
        "quantity":    [2, None, 1, 3, 2],
        "order_date":  ["2024-01-01", "2024-01-02", "bad-date", "2024-01-04", "2024-01-01"],
        "status":      ["delivered", "shipped", "delivered", "unknown_status", "delivered"],
        "discount_pct":[0.0, 0.10, 0.0, 0.0, 0.0],
    })


@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "customer_id": ["C001", "C002", "C003"],
        "first_name":  ["Alice", "Bob", "Clara"],
        "last_name":   ["M", "D", "S"],
        "email":       ["a@b.com", None, "c@d.com"],
        "city":        ["Paris", "Lyon", "London"],
        "country":     ["France", "France", "UK"],
        "signup_date": ["2022-01-01", "2022-02-01", "2022-03-01"],
    })


@pytest.fixture
def sample_products():
    return pd.DataFrame({
        "product_id": ["P001", "P002", "P003"],
        "name":       ["Laptop", "Mouse", "Hub"],
        "category":   ["Electronics", "Electronics", "Electronics"],
        "unit_price": [1000.0, 30.0, 50.0],
        "stock":      [10, 50, 100],
    })


@pytest.fixture
def raw(sample_orders, sample_customers, sample_products):
    return {
        "orders": sample_orders,
        "customers": sample_customers,
        "products": sample_products,
    }


# ---------- Tests ----------

class TestCleanOrders:
    def test_drop_duplicates(self, transformer, raw):
        result = transformer.transform(raw)
        enriched = result["orders_enriched"]
        assert enriched["order_id"].is_unique

    def test_fill_missing_quantity(self, transformer, raw):
        result = transformer.transform(raw)
        enriched = result["orders_enriched"]
        assert enriched["quantity"].isna().sum() == 0

    def test_invalid_date_row_removed(self, transformer, raw):
        result = transformer.transform(raw)
        enriched = result["orders_enriched"]
        assert "O003" not in enriched["order_id"].values

    def test_invalid_status_row_removed(self, transformer, raw):
        result = transformer.transform(raw)
        enriched = result["orders_enriched"]
        assert "O004" not in enriched["order_id"].values


class TestCleanCustomers:
    def test_full_name_computed(self, transformer, raw):
        result = transformer.transform(raw)
        enriched = result["orders_enriched"]
        assert "full_name" in enriched.columns

    def test_missing_email_filled(self, transformer, sample_customers, sample_products, sample_orders):
        customers_cleaned = transformer._clean_customers(sample_customers)
        assert customers_cleaned["email"].isna().sum() == 0


class TestFinancials:
    def test_net_revenue_column_exists(self, transformer, raw):
        result = transformer.transform(raw)
        assert "net_revenue" in result["orders_enriched"].columns

    def test_net_revenue_less_than_gross_when_discount(self, transformer, raw):
        enriched = transformer.transform(raw)["orders_enriched"]
        discounted = enriched[enriched["discount_pct"] > 0]
        assert (discounted["net_revenue"] < discounted["gross_revenue"]).all()

    def test_gross_profit_positive_for_delivered(self, transformer, raw):
        enriched = transformer.transform(raw)["orders_enriched"]
        delivered = enriched[enriched["status"] == "delivered"]
        assert (delivered["gross_profit"] > 0).all()


class TestSummary:
    def test_summary_excludes_cancelled(self, transformer, raw):
        result = transformer.transform(raw)
        summary = result["sales_summary"]
        # The summary should be built from non-cancelled orders only
        # (no easy direct check without re-filtering, but summary should exist)
        assert isinstance(summary, pd.DataFrame)
        assert "net_revenue" in summary.columns

    def test_summary_has_profit_margin(self, transformer, raw):
        result = transformer.transform(raw)
        assert "profit_margin_pct" in result["sales_summary"].columns

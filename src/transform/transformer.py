"""
Transformer module — cleans, validates, joins, and enriches raw DataFrames.
"""

import pandas as pd
from loguru import logger


class Transformer:
    """
    Applies all business transformation rules to the raw datasets and
    produces two analytical tables:
      - orders_enriched : one row per order line, fully joined & cleaned
      - sales_summary   : aggregated KPIs per product category
    """

    def __init__(self, config: dict):
        self.cfg = config["transform"]
        self.revenue_margin = self.cfg["revenue_margin_pct"]
        self.valid_statuses = self.cfg["valid_statuses"]
        self.fill_qty = self.cfg["fill_missing_quantity"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def transform(self, raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        orders = self._clean_orders(raw["orders"])
        customers = self._clean_customers(raw["customers"])
        products = self._clean_products(raw["products"])

        enriched = self._join(orders, customers, products)
        enriched = self._compute_financials(enriched)

        summary = self._build_summary(enriched)

        logger.info(f"Transformation complete — {len(enriched)} enriched orders")
        return {
            "orders_enriched": enriched,
            "sales_summary": summary,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _clean_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning orders …")
        df = df.copy()

        # Drop full duplicates
        if self.cfg["drop_duplicates"]:
            before = len(df)
            df = df.drop_duplicates(subset="order_id")
            logger.info(f"  Dropped {before - len(df)} duplicate order rows")

        # Fill missing quantity
        missing_qty = df["quantity"].isna().sum()
        if missing_qty:
            df["quantity"] = df["quantity"].fillna(self.fill_qty)
            logger.warning(f"  Filled {missing_qty} missing quantity values with {self.fill_qty}")

        df["quantity"] = df["quantity"].astype(int)

        # Parse & validate dates
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        invalid_dates = df["order_date"].isna().sum()
        if invalid_dates:
            logger.warning(f"  Dropping {invalid_dates} rows with invalid order_date")
            df = df.dropna(subset=["order_date"])

        # Validate status
        invalid_status = ~df["status"].isin(self.valid_statuses)
        if invalid_status.any():
            logger.warning(f"  Dropping {invalid_status.sum()} rows with unknown status")
            df = df[~invalid_status]

        df["discount_pct"] = df["discount_pct"].fillna(0.0)

        return df.reset_index(drop=True)

    def _clean_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning customers …")
        df = df.copy()
        if self.cfg["drop_duplicates"]:
            df = df.drop_duplicates(subset="customer_id")
        df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
        df["email"] = df["email"].fillna("unknown@unknown.com")
        df["full_name"] = df["first_name"].str.strip() + " " + df["last_name"].str.strip()
        return df.reset_index(drop=True)

    def _clean_products(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning products …")
        df = df.copy()
        if self.cfg["drop_duplicates"]:
            df = df.drop_duplicates(subset="product_id")
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        df = df.dropna(subset=["unit_price"])
        return df.reset_index(drop=True)

    def _join(
        self,
        orders: pd.DataFrame,
        customers: pd.DataFrame,
        products: pd.DataFrame,
    ) -> pd.DataFrame:
        logger.info("Joining orders → customers → products …")
        df = orders.merge(
            customers[["customer_id", "full_name", "city", "country", "signup_date"]],
            on="customer_id",
            how="left",
            validate="m:1",
        )
        df = df.merge(
            products[["product_id", "name", "category", "unit_price"]],
            on="product_id",
            how="left",
            validate="m:1",
        )
        return df

    def _compute_financials(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Computing financial metrics …")
        df = df.copy()
        df["gross_revenue"] = df["quantity"] * df["unit_price"]
        df["discount_amount"] = df["gross_revenue"] * df["discount_pct"]
        df["net_revenue"] = df["gross_revenue"] - df["discount_amount"]
        df["estimated_cost"] = df["gross_revenue"] * (1 - self.revenue_margin)
        df["gross_profit"] = df["net_revenue"] - df["estimated_cost"]
        df["order_year_month"] = df["order_date"].dt.to_period("M").astype(str)
        return df

    def _build_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Building sales summary …")
        active = df[df["status"] != "cancelled"]
        summary = (
            active.groupby(["order_year_month", "category"])
            .agg(
                total_orders=("order_id", "count"),
                total_quantity=("quantity", "sum"),
                gross_revenue=("gross_revenue", "sum"),
                net_revenue=("net_revenue", "sum"),
                gross_profit=("gross_profit", "sum"),
                avg_discount_pct=("discount_pct", "mean"),
            )
            .reset_index()
        )
        summary["profit_margin_pct"] = (
            summary["gross_profit"] / summary["net_revenue"] * 100
        ).round(2)
        return summary

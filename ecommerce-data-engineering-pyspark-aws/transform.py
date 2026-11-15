"""Transform step: cleaning (bronze -> silver) and business logic (silver -> gold)."""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

DATE_FORMATS = ["yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd"]
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def parse_date(name: str):
    """Try each known date format in order; the first one that parses wins."""
    date = F.trim(F.col(name))
    return F.coalesce(*[F.try_to_timestamp(date, F.lit(f)).cast("date") for f in DATE_FORMATS])


# ---------------------------------------------------------------- bronze -> silver

def clean_orders(orders: DataFrame) -> DataFrame:
    return (
        orders
        .withColumn("status", F.initcap(F.trim(F.col("status"))))
        .withColumn("shipping_city", F.initcap(F.trim(F.col("shipping_city"))))
        .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
        .dropna(subset=["order_id", "customer_id"])
        .fillna({"status": "Unknown", "payment_method": "unknown", "shipping_city": "Unknown"})
        .dropDuplicates(subset=["order_id"])
        .withColumn("order_date_parse", parse_date("order_date"))
        .filter(F.col("order_date_parse").isNotNull())
        .drop("order_date")
        .withColumnRenamed("order_date_parse", "order_date")
    )


def clean_products(products: DataFrame) -> DataFrame:
    return (
        products
        .withColumn("unit_price_p", F.col("unit_price").cast("double")).drop("unit_price")
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("unit_price_p", F.when(F.col("unit_price_p") > 0, F.col("unit_price_p")).otherwise(None))
        .dropDuplicates(subset=["product_id"])
        .dropna(subset=["product_id"])
    )


def clean_order_items(order_items: DataFrame) -> DataFrame:
    return (
        order_items
        .withColumn("discount", F.col("discount_pct").cast("integer"))
        .drop("discount_pct")
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("unit_price", F.when(F.col("unit_price") == 0, None).otherwise(F.col("unit_price")))
        .dropna(subset=["item_id", "order_id", "product_id", "quantity"])
        .dropDuplicates(subset=["item_id"])
        .fillna({"discount": 0})
        .filter(F.col("quantity") > 0)
    )


def join_items_products(order_items_s: DataFrame, products_s: DataFrame) -> DataFrame:
    """Attach product details; use the list price when an item has no price of its own."""
    return (
        order_items_s.join(F.broadcast(products_s), on="product_id", how="left")
        .withColumn("unit_price", F.coalesce(F.col("unit_price"), F.col("unit_price_p")))
        .drop("unit_price_p")
        .dropna(subset=["unit_price"])
        .fillna({"category": "unknown", "brand": "not specified", "product_name": "Unknown Product"})
    )


def clean_customers(customers: DataFrame) -> DataFrame:
    return (
        customers
        .withColumn("name", F.initcap(F.trim(F.col("name"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("email", F.when(F.col("email").rlike(EMAIL_REGEX), F.col("email")).otherwise(F.lit("invalid")))
        .withColumn("annual_income", F.when(F.col("annual_income") > 0, F.col("annual_income")).otherwise(None))
        .fillna({"city": "not_given"})
        .dropna(subset=["customer_id", "name"])
        .dropDuplicates(subset=["customer_id"])
        .withColumn("signup_date_format", parse_date("signup_date"))
        .filter(F.col("signup_date_format").isNotNull())
        .drop("signup_date")
        .withColumnRenamed("signup_date_format", "signup_date")
    )


def build_silver(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Clean every bronze table. Keys are the silver table (folder) names."""
    products_s = clean_products(raw["products"])
    items_s = clean_order_items(raw["order_items"])
    return {
        "orders": clean_orders(raw["orders"]),
        "order_items": join_items_products(items_s, products_s),
        "customers": clean_customers(raw["customers"]),
    }


# ---------------------------------------------------------------- silver -> gold

def build_gold(silver: dict[str, DataFrame]) -> DataFrame:
    """One row per order item, joined to its order and customer, with total_price."""
    return (
        silver["order_items"]
        .join(silver["orders"], on="order_id", how="inner")
        .join(F.broadcast(silver["customers"]), on="customer_id", how="left")
        .withColumn("total_price", F.col("quantity") * F.col("unit_price") * (1 - F.col("discount") / 100))
        .select("order_id", "item_id", "order_date", "customer_id",
                F.col("city").alias("customer_city"), "shipping_city",
                "product_id", "product_name", "category", "brand",
                "quantity", "unit_price", "discount", "total_price",
                "status", "payment_method")
        .withColumn("year", F.year(F.col("order_date")))
        .withColumn("month", F.month(F.col("order_date")))
    )


def monthly_revenue(gold: DataFrame) -> DataFrame:
    return (
        gold.filter(F.col("status") == "Completed")
        .groupBy("year", "month")
        .agg(F.round(F.sum("total_price"), 2).alias("monthly_revenue"),
             F.countDistinct("order_id").alias("total_orders"))
        .orderBy("year", "month")
    )


def top_selling_products(gold: DataFrame) -> DataFrame:
    """Best product by revenue in each category."""
    w = Window.partitionBy("category").orderBy(F.col("revenue_per_product").desc())
    return (
        gold.filter(F.col("status") == "Completed")
        .groupBy("category", "product_id", "product_name")
        .agg(F.round(F.sum("total_price"), 2).alias("revenue_per_product"))
        .withColumn("rank_in_category", F.rank().over(w))
        .filter(F.col("rank_in_category") == 1)
    )


def category_revenue(gold: DataFrame) -> DataFrame:
    return (
        gold.filter(F.col("status") == "Completed")
        .groupBy("category")
        .agg(F.sum("total_price").alias("revenue_by_category"))
        .orderBy(F.col("revenue_by_category").desc())
    )


def customer_revenue(gold: DataFrame) -> DataFrame:
    return (
        gold.filter(F.col("status") == "Completed")
        .groupBy("customer_id")
        .agg(F.countDistinct("order_id").alias("num_of_orders"),
             F.round(F.sum("total_price"), 2).alias("revenue_by_customer"))
        .orderBy(F.col("revenue_by_customer").desc())
    )


def return_rate(gold: DataFrame) -> DataFrame:
    """Returned units as a share of units that shipped (Completed + Returned)."""
    return (
        gold.filter(F.col("status").isin("Returned", "Completed"))
        .groupBy("category")
        .agg(F.sum(F.when(F.col("status") == "Returned", F.col("quantity")).otherwise(0)).alias("returned_units"),
             F.sum(F.col("quantity")).alias("shipped_units"))
        .withColumn("return_rate_pct", F.round(100 * F.col("returned_units") / F.col("shipped_units"), 2))
        .orderBy(F.col("return_rate_pct").desc())
    )


def build_marts(gold: DataFrame) -> dict[str, DataFrame]:
    """Aggregate gold tables. Keys are the S3 folder (Athena table) names."""
    return {
        "monthly_revenue": monthly_revenue(gold),
        "top_selling_products": top_selling_products(gold),
        "category_revenue": category_revenue(gold),
        "customer_revenue": customer_revenue(gold),
        "return_rate": return_rate(gold),
    }

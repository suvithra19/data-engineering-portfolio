"""Extract step: read data from the previous layer into Spark DataFrames."""


from pyspark.sql import DataFrame, SparkSession

BRONZE = "s3a://suvithra-bronze/e-commerce"
SILVER = "s3a://suvithra-silver/e-commerce"


BRONZE_TABLES = ("orders", "products", "order_items", "customers")
SILVER_TABLES = ("orders", "order_items", "customers")


def extract_bronze(spark: SparkSession) -> dict[str, DataFrame]:
    """Read the raw CSV files from the bronze layer."""
    return {
        name: spark.read.csv(f"{BRONZE}/{name}.csv", header=True, inferSchema=True)
        for name in BRONZE_TABLES
    }


def extract_silver(spark: SparkSession) -> dict[str, DataFrame]:
    """Read the cleaned Parquet tables from the silver layer."""
    return {name: spark.read.parquet(f"{SILVER}/{name}/") for name in SILVER_TABLES}

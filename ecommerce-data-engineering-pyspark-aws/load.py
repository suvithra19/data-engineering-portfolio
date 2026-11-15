"""Load step: write DataFrames to the silver and gold layers as Parquet."""


from pyspark.sql import DataFrame

SILVER = "s3a://suvithra-silver/e-commerce"
GOLD = "s3a://suvithra-gold/e-commerce"


def write_silver(tables: dict[str, DataFrame]) -> None:
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{SILVER}/{name}/")


def write_gold(fact: DataFrame, marts: dict[str, DataFrame]) -> None:
    # fact table: one file per year, in year=YYYY folders
    (fact.repartition("year")
         .write.mode("overwrite").partitionBy("year")
         .parquet(f"{GOLD}/gold"))

    # small aggregate tables: one file each
    for name, df in marts.items():
        df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD}/{name}")

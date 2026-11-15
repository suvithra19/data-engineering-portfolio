"""Run the pipeline: bronze -> silver -> gold.

    spark-submit main.py
"""
from pyspark.sql import SparkSession
import extract
import load
import transform


def get_spark(app_name: str = "ReadWriteS3") -> SparkSession:
    spark = (
    SparkSession.builder.appName("ReadWriteS3").config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1").getOrCreate()
)
    # when writing partitioned data, replace only the partitions present in the output
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

def main() -> None:
    spark = get_spark()

    # bronze -> silver
    raw = extract.extract_bronze(spark)
    silver = transform.build_silver(raw)
    load.write_silver(silver)

    # silver -> gold (read back what was just written, as the silver layer is the source of truth)
    silver = extract.extract_silver(spark)
    gold = transform.build_gold(silver).cache()
    marts = transform.build_marts(gold)
    load.write_gold(gold, marts)

    spark.stop()


if __name__ == "__main__":
    main()

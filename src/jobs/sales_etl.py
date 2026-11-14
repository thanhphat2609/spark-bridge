"""
Sales ETL - PySpark Batch Job (reads and writes object storage)
----------------------------------------------------------------
Reads a raw CSV, aggregates revenue per category and day, writes Parquet,
then reads the Parquet back to prove the round-trip.

Input/output paths are defined below (INPUT_PATH / OUTPUT_PATH); edit them
to point at your storage account and container.

Submit via Livy batch endpoint:
    POST http://<livy-host>:8998/batches
    {
        "file":  "<STORAGE_ROOT>/jobs/sales_etl.py"
    }
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

INPUT_PATH = "abfss://data@dataplatform.dfs.core.windows.net/raw/sales.csv"
OUTPUT_PATH = "abfss://data@dataplatform.dfs.core.windows.net/curated/sales_by_category"


def run(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("SalesETL").getOrCreate()

    raw = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    print(f"Read {raw.count()} raw rows from {input_path}")

    curated = (
        raw.withColumn("revenue", col("unit_price") * col("quantity"))
        .groupBy("order_date", "category")
        .agg(
            spark_sum("revenue").alias("total_revenue"),
            spark_sum("quantity").alias("total_units"),
            count("*").alias("order_lines"),
        )
    )

    curated.write.mode("overwrite").partitionBy("order_date").parquet(output_path)
    print(f"Wrote curated Parquet to {output_path}")

    check = spark.read.parquet(output_path)
    print(f"Read back {check.count()} curated rows")
    check.groupBy("category").agg(
        spark_sum("total_revenue").alias("total_revenue")
    ).orderBy("total_revenue", ascending=False).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    run(INPUT_PATH, OUTPUT_PATH)

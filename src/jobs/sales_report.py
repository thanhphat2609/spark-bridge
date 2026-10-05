"""
Sales Report - PySpark Batch Job
----------------------------------
Aggregates in-memory sales data into a category-level report.
In a production scenario the data source would be a data lake (S3, ADLS, etc.),
but this self-contained version demonstrates the Spark computation logic.

Submit via Livy batch endpoint:
    POST http://<livy-host>:8998/batches
    {
        "file": "<STORAGE_ROOT>/jobs/sales_report.py",
        "name": "SalesReport",
        "executorMemory": "1g"
    }
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum


SALES_DATA = [
    ("Electronics", "Laptop",    1200.00,  5),
    ("Electronics", "Phone",      800.00, 10),
    ("Electronics", "Tablet",     500.00,  8),
    ("Electronics", "Headphones", 180.00, 15),
    ("Clothing",    "Jacket",     120.00, 25),
    ("Clothing",    "Shoes",       90.00, 40),
    ("Clothing",    "Hat",         30.00, 60),
    ("Clothing",    "Bag",         75.00, 20),
    ("Food",        "Coffee",      15.00, 100),
    ("Food",        "Tea",         10.00,  80),
    ("Food",        "Chocolate",    5.00, 200),
]

COLUMNS = ["category", "product", "unit_price", "quantity"]


def generate_sales_report() -> None:
    spark = SparkSession.builder.appName("SalesReport").getOrCreate()

    df = spark.createDataFrame(SALES_DATA, COLUMNS)
    df = df.withColumn("revenue", col("unit_price") * col("quantity"))

    report = (
        df.groupBy("category")
        .agg(
            spark_sum("revenue").alias("total_revenue"),
            spark_sum("quantity").alias("total_units_sold"),
            avg("unit_price").alias("avg_unit_price"),
            count("product").alias("product_count"),
        )
        .orderBy("total_revenue", ascending=False)
    )

    print("\nSales Report - Aggregated by Category")
    print("=" * 60)
    report.show(truncate=False)

    top_products = (
        df.orderBy("revenue", ascending=False).limit(5)
    )
    print("Top 5 Products by Revenue")
    print("=" * 60)
    top_products.select("category", "product", "unit_price", "quantity", "revenue").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    generate_sales_report()

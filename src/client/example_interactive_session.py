"""
example_interactive_session.py
-------------------------------
Demonstrates the Livy interactive session workflow.

In this mode, PySpark code is sent as plain text strings over HTTP.
The client does NOT need a local Spark installation -- only 'requests' is
required.  Livy creates a persistent SparkSession on the cluster and forwards
each code snippet to it.

The difference from batch mode:
    - Batch  : submit a .py file that lives on the cluster, fire and forget.
    - Session: send code snippets from the client dynamically, one at a time,
               and receive back the printed output / DataFrame preview.

Run:
    python example_interactive_session.py
"""

import logging
import os
import sys

from livy_client import LivyClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
)
logger = logging.getLogger(__name__)

LIVY_HOST = os.environ.get("LIVY_URL", "http://localhost:8998")


def print_output(label: str, output: dict) -> None:
    """Pretty-print a Livy statement output dict."""
    print(f"\n--- {label} ---")
    if output.get("status") == "error":
        print("ERROR:", output.get("evalue"))
        for line in output.get("traceback", []):
            print(line, end="")
    else:
        data = output.get("data", {})
        text = data.get("text/plain", "")
        print(text if text else "(no output)")
    print()


def run_interactive_demo(client: LivyClient) -> None:
    """
    Open one interactive PySpark session, run several code snippets,
    then close the session to free cluster resources.
    """
    logger.info("Creating interactive PySpark session ...")

    # The context manager guarantees DELETE /sessions/<id> on exit
    with client.create_session(kind="pyspark", name="InteractiveDemo") as session:

        logger.info("Session %d created.  Waiting until ready ...", session.session_id)
        session.wait_until_ready()
        logger.info("Session is ready.")

        # ------------------------------------------------------------------
        # Step 1: Check Spark version (handy sanity check)
        # ------------------------------------------------------------------
        output = session.run("spark.version")
        print_output("Spark version", output)

        # ------------------------------------------------------------------
        # Step 2: Compute Pi inline (no file needed on the cluster)
        # ------------------------------------------------------------------
        pi_code = """
import random

NUM_SAMPLES = 500_000
sc = spark.sparkContext

def inside(_):
    x, y = random.random(), random.random()
    return (x * x + y * y) < 1.0

count = sc.parallelize(range(NUM_SAMPLES)).filter(inside).count()
pi = 4.0 * count / NUM_SAMPLES
print(f"Pi estimate: {pi:.6f}")
"""
        output = session.run(pi_code)
        print_output("Inline Pi estimation", output)

        # ------------------------------------------------------------------
        # Step 3: DataFrame aggregation
        # ------------------------------------------------------------------
        agg_code = """
from pyspark.sql import Row
from pyspark.sql.functions import avg, sum as spark_sum

rows = [
    Row(department="Engineering", salary=95000),
    Row(department="Engineering", salary=110000),
    Row(department="Marketing",   salary=75000),
    Row(department="Marketing",   salary=82000),
    Row(department="HR",          salary=65000),
    Row(department="HR",          salary=70000),
]

df = spark.createDataFrame(rows)
df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    spark_sum("salary").alias("total_salary"),
).orderBy("total_salary", ascending=False).show()
"""
        output = session.run(agg_code)
        print_output("Salary aggregation by department", output)

        # ------------------------------------------------------------------
        # Step 4: Word frequency via RDD
        # ------------------------------------------------------------------
        word_code = """
text = (
    "the quick brown fox jumps over the lazy dog "
    "the fox ran quickly the brown dog chased the fox"
)
sc = spark.sparkContext
freq = (
    sc.parallelize(text.split())
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: -x[1])
    .collect()
)
for word, count in freq:
    print(f"{word:<15} {count}")
"""
        output = session.run(word_code)
        print_output("Word frequency", output)

        logger.info("Session %d will be closed now.", session.session_id)

    logger.info("Session closed.  Cluster resources have been released.")


def main() -> None:
    client = LivyClient(LIVY_HOST)

    if not client.health_check():
        logger.error(
            "Cannot reach Livy at %s.  "
            "Make sure the cluster is running (docker compose up).",
            LIVY_HOST,
        )
        sys.exit(1)

    logger.info("Connected to Livy at %s", LIVY_HOST)
    run_interactive_demo(client)


if __name__ == "__main__":
    main()

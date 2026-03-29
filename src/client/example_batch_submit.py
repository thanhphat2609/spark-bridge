"""
example_batch_submit.py
-----------------------
Demonstrates submitting PySpark scripts as Livy batch jobs.

Requirements on the CLIENT machine:
    pip install requests          # that is all -- no Spark, no Java

The PySpark script files (pi_estimation.py, sales_report.py, word_count.py)
live in src/jobs/ and are mounted into the cluster containers at
/opt/spark-jobs/ via the Docker volume defined in docker-compose.yml.

Run:
    python example_batch_submit.py
"""

import logging
import sys

from livy_client import LivyClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Point this at your Livy server.  When running locally with docker-compose,
# the default is http://localhost:8998.
# ---------------------------------------------------------------------------
LIVY_HOST = "http://localhost:8998"


# ---------------------------------------------------------------------------
# Example 1 - Pi Estimation
# ---------------------------------------------------------------------------

def run_pi_estimation(client: LivyClient) -> None:
    logger.info("--- Submitting Pi Estimation batch job ---")

    job = client.submit_batch(
        file="local:///opt/spark-jobs/pi_estimation.py",
        args=["2000000"],
        name="PiEstimation",
        executor_memory="512m",
        driver_memory="512m",
        conf={"spark.executor.cores": "1"},
    )

    logger.info("Job submitted.  Batch ID: %d", job.batch_id)
    logger.info("Polling for completion ...")

    final_state = job.wait(poll_interval=5)

    if final_state == "success":
        logger.info("Pi Estimation completed successfully.")
    else:
        logger.error("Pi Estimation failed.  Final state: %s", final_state)
        logger.error("Last log lines:")
        for line in job.get_log()[-40:]:
            print(line)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Example 2 - Word Count
# ---------------------------------------------------------------------------

def run_word_count(client: LivyClient) -> None:
    logger.info("--- Submitting Word Count batch job ---")

    job = client.submit_batch(
        file="local:///opt/spark-jobs/word_count.py",
        name="WordCount",
        executor_memory="512m",
    )

    logger.info("Job submitted.  Batch ID: %d", job.batch_id)
    final_state = job.wait(poll_interval=5)
    logger.info("Word Count finished.  State: %s", final_state)

    if final_state != "success":
        for line in job.get_log()[-30:]:
            print(line)


# ---------------------------------------------------------------------------
# Example 3 - Sales Report
# ---------------------------------------------------------------------------

def run_sales_report(client: LivyClient) -> None:
    logger.info("--- Submitting Sales Report batch job ---")

    job = client.submit_batch(
        file="local:///opt/spark-jobs/sales_report.py",
        name="SalesReport",
        executor_memory="1g",
        driver_memory="512m",
    )

    logger.info("Job submitted.  Batch ID: %d", job.batch_id)
    final_state = job.wait(poll_interval=5)
    logger.info("Sales Report finished.  State: %s", final_state)

    if final_state != "success":
        for line in job.get_log()[-30:]:
            print(line)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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

    # Show already-submitted batches
    batches = client.list_batches()
    logger.info("Active/recent batches: %d", len(batches))

    # Run the three example jobs sequentially
    run_pi_estimation(client)
    run_word_count(client)
    run_sales_report(client)

    logger.info("All batch jobs completed.")


if __name__ == "__main__":
    main()

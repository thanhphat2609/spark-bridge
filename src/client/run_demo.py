"""
run_demo.py
-----------
End-to-end demo for the YARN profile. Runs on the "Airflow-lite" machine
(Azure client VM or the Codespaces client container) with only `requests`.

It submits two batch jobs through Livy and fails loudly if either fails:
    1. pi_estimation.py  - compute only, proves YARN cluster mode works
    2. sales_etl.py      - reads CSV from storage, writes Parquet back

Configuration (environment variables):
    LIVY_URL      http://sb-master:8998           (Azure, private network)
                  http://localhost:8998           (Codespaces terminal)
    STORAGE_ROOT  abfss://spark@<acct>.dfs.core.windows.net/spark-bridge
                  file:///data/shared             (Codespaces)
    YARN_QUEUE    etl (default)

Jobs and data must already be uploaded under STORAGE_ROOT:
    Azure      : bash scripts/deploy_jobs_azure.sh
    Codespaces : done by the client container (scripts/upload_local_storage.sh)

Run:
    python src/client/run_demo.py
"""

import logging
import os
import sys

from livy_client import LivyBatchJob, LivyClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
)
logger = logging.getLogger(__name__)

LIVY_URL = os.environ.get("LIVY_URL", "http://localhost:8998")
STORAGE_ROOT = os.environ.get("STORAGE_ROOT", "file:///data/shared").rstrip("/")
YARN_QUEUE = os.environ.get("YARN_QUEUE", "etl")

JOBS = f"{STORAGE_ROOT}/jobs"
DATA = f"{STORAGE_ROOT}/data"


def run_and_report(name: str, job: LivyBatchJob) -> bool:
    logger.info("%s submitted as Livy batch %d, waiting ...", name, job.batch_id)
    state = job.wait(poll_interval=5, timeout=900)

    info = job.get_info()
    app_info = info.get("appInfo") or {}
    logger.info("%s -> state=%s  yarnAppId=%s", name, state, info.get("appId"))
    if app_info.get("sparkUiUrl"):
        logger.info("   Spark/YARN UI : %s", app_info["sparkUiUrl"])

    if state != "success":
        logger.error("%s FAILED. Last Livy log lines:", name)
        for line in job.get_log(60):
            print(line)
        return False
    return True


def main() -> None:
    client = LivyClient(LIVY_URL)
    if not client.health_check():
        logger.error("Cannot reach Livy at %s", LIVY_URL)
        sys.exit(1)

    logger.info("Livy        : %s", LIVY_URL)
    logger.info("Storage root: %s", STORAGE_ROOT)
    logger.info("YARN queue  : %s", YARN_QUEUE)

    results = {}

    results["PiEstimation"] = run_and_report(
        "PiEstimation",
        client.submit_batch(
            file=f"{JOBS}/pi_estimation.py",
            args=["1000000"],
            name="PiEstimation",
            queue=YARN_QUEUE,
        ),
    )

    results["SalesETL"] = run_and_report(
        "SalesETL",
        client.submit_batch(
            file=f"{JOBS}/sales_etl.py",
            args=[f"{DATA}/raw/sales.csv", f"{DATA}/curated/sales_by_category"],
            name="SalesETL",
            queue=YARN_QUEUE,
        ),
    )

    print("\n================ DEMO RESULT ================")
    for name, ok in results.items():
        print(f"  {name:<15} {'PASS' if ok else 'FAIL'}")
    print("=============================================")
    print("Job stdout (println / df.show) is in the YARN driver log:")
    print("  RM UI :8088 -> application -> logs   or   History Server :18080")

    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    main()

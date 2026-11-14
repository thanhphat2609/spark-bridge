"""
Submit one PySpark file to a Livy server as a batch job and wait for it.

    python main.py <livy_url> <storage_account> <container/path/to/job.py> [options]

Only `requests` is needed on this machine.
"""

import argparse
import logging
import os
import sys

from livy_client import LivyClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(levelname)s]  %(message)s")
logger = logging.getLogger("runner")

LOCAL_ROOT = "file:///data/shared"


def build_uri(storage: str, path: str) -> str:
    path = path.strip("/")
    if storage == "local":
        return f"{LOCAL_ROOT}/{path}"
    container, _, blob = path.partition("/")
    if not blob:
        raise SystemExit(f"path must be <container>/<file>, got '{path}'")
    return f"abfss://{container}@{storage}.dfs.core.windows.net/{blob}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a PySpark file on the cluster through Livy.")
    p.add_argument("livy_url", help="e.g. http://sb-master:8998")
    p.add_argument("storage", help="ADLS account name (e.g. dataplatform), or 'local' for the Codespaces volume")
    p.add_argument("path", help="<container>/<path/to/job.py>")
    p.add_argument("--name", help="Spark app name (default: job file name)")
    p.add_argument("--executors", type=int, help="number of executors")
    p.add_argument("--cores", type=int, help="cores per executor")
    p.add_argument("--executor-memory", help="e.g. 2g")
    p.add_argument("--driver-memory", help="e.g. 1g")
    p.add_argument("--timeout", type=int, default=3600, help="max seconds to wait (default: 3600)")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    token = os.environ.get("LIVY_GITHUB_TOKEN")  # only for a private Codespaces forwarded URL
    client = LivyClient(a.livy_url, headers={"X-Github-Token": token} if token else None)
    if not client.health_check():
        logger.error("Cannot reach Livy at %s", a.livy_url)
        sys.exit(1)

    job_uri = build_uri(a.storage, a.path)
    name = a.name or os.path.splitext(os.path.basename(job_uri))[0]
    logger.info("Submitting %s", job_uri)

    job = client.submit_batch(
        file=job_uri,
        name=name,
        num_executors=a.executors,
        executor_cores=a.cores,
        executor_memory=a.executor_memory,
        driver_memory=a.driver_memory,
    )
    state = job.wait(poll_interval=5, timeout=a.timeout)
    info = job.get_info()
    logger.info("%s -> state=%s  yarnAppId=%s", name, state, info.get("appId"))

    if state != "success":
        logger.error("Job failed. Last Livy log lines:")
        for line in job.get_log(60):
            print(line)
        sys.exit(1)


if __name__ == "__main__":
    main()

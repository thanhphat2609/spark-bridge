"""
Pi Estimation - PySpark Batch Job
----------------------------------
Estimates the value of Pi using the Monte Carlo method.
This script runs on the Spark cluster; it is submitted via the Livy REST API
and does not need to be present on the client machine.

Submit via Livy batch endpoint:
    POST http://<livy-host>:8998/batches
    {
        "file": "<STORAGE_ROOT>/jobs/pi_estimation.py",
        "name": "PiEstimation"
    }
"""

import random
import sys
from pyspark.sql import SparkSession


def estimate_pi(num_samples: int) -> float:
    spark = SparkSession.builder.appName("PiEstimation").getOrCreate()
    sc = spark.sparkContext

    def inside(_):
        x = random.random()
        y = random.random()
        return (x * x + y * y) < 1.0

    count = sc.parallelize(range(num_samples)).filter(inside).count()
    pi = 4.0 * count / num_samples

    print(f"Pi is approximately {pi:.6f}  (samples={num_samples:,})")

    spark.stop()
    return pi


if __name__ == "__main__":
    samples = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    estimate_pi(samples)

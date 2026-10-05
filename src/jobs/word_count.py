"""
Word Count - PySpark Batch Job
---------------------------------
Classic word-frequency analysis on a sample corpus.
This script runs on the Spark cluster and is submitted via Livy.

Submit via Livy batch endpoint:
    POST http://<livy-host>:8998/batches
    {
        "file": "<STORAGE_ROOT>/jobs/word_count.py",
        "name": "WordCount"
    }
"""

from pyspark.sql import SparkSession

SAMPLE_TEXT = """
Apache Spark is a unified analytics engine for large scale data processing.
It provides high level APIs in Java Scala Python and R and an optimized engine
that supports general execution graphs. Apache Spark also supports a rich set
of higher level tools including Spark SQL for SQL and structured data processing
MLlib for machine learning GraphX for graph processing and Structured Streaming
for incremental computation and stream processing.

Apache Livy is a service that enables easy interaction with a Spark cluster
over a REST interface. It enables easy submission of Spark jobs or snippets of
Spark code, synchronous or asynchronous result retrieval, as well as Spark
Context management, all via a simple REST interface or an RPC client library.
Apache Livy also simplifies the interaction between Spark and application
servers, thus enabling the use of Spark for interactive web and mobile
applications. Additional features include multiple users sharing the same
Spark Contexts or Sessions and secure authenticated communication.
"""


def word_count(text: str, top_n: int = 20) -> None:
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    sc = spark.sparkContext

    lines = sc.parallelize(text.strip().splitlines())
    words = lines.flatMap(lambda line: line.lower().split())
    frequencies = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    results = sorted(frequencies.collect(), key=lambda x: -x[1])

    print(f"\nTop {top_n} words by frequency:")
    print("-" * 30)
    for word, count in results[:top_n]:
        print(f"  {word:<20} {count}")

    spark.stop()


if __name__ == "__main__":
    word_count(SAMPLE_TEXT)

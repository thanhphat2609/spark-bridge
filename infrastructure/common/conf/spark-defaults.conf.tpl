# Rendered by entrypoint.sh from .env
spark.master                              yarn
spark.submit.deployMode                   cluster

# Spark jars are already in the image on every node -> nothing to upload per job
spark.yarn.jars                           local:${SPARK_HOME}/jars/*
spark.yarn.stagingDir                     ${STORAGE_ROOT}/staging

spark.driver.memory                       ${SPARK_DRIVER_MEMORY}
spark.executor.memory                     ${SPARK_EXECUTOR_MEMORY}
spark.executor.cores                      1

# Dynamic allocation without an external shuffle service (Spark 3+): executors that
# still hold shuffle data are kept until it's no longer needed or the timeout passes.
spark.shuffle.service.enabled             false
spark.dynamicAllocation.enabled           true
spark.dynamicAllocation.shuffleTracking.enabled true
spark.dynamicAllocation.shuffleTracking.timeout 300s
spark.dynamicAllocation.minExecutors      0
spark.dynamicAllocation.maxExecutors      ${SPARK_MAX_EXECUTORS}
spark.dynamicAllocation.executorIdleTimeout 60s

spark.eventLog.enabled                    true
spark.eventLog.dir                        ${STORAGE_ROOT}/spark-history
spark.history.fs.logDirectory             ${STORAGE_ROOT}/spark-history
spark.yarn.historyServer.address          ${MASTER_HOST}:18080

spark.yarn.appMasterEnv.PYSPARK_PYTHON    /usr/bin/python3
spark.executorEnv.PYSPARK_PYTHON          /usr/bin/python3

# Rendered by entrypoint.sh from .env
livy.server.host                     = 0.0.0.0
livy.server.port                     = 8998

# Drivers run in YARN containers on the WORKER, not on the master VM
livy.spark.master                    = yarn
livy.spark.deploy-mode               = cluster
livy.spark.scala-version             = 2.12
livy.server.spark-home               = ${SPARK_HOME}

livy.server.session.timeout          = 1h
livy.server.session.max-creation     = 100

# Livy restart does not kill running batches (state lives on a local volume)
livy.server.recovery.mode            = recovery
livy.server.recovery.state-store     = filesystem
livy.server.recovery.state-store.url = file:///var/livy/recovery

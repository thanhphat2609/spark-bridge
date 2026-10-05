<?xml version="1.0"?>
<!-- Rendered by entrypoint.sh from .env -->
<configuration>
  <!-- ResourceManager (master) -->
  <property><name>yarn.resourcemanager.hostname</name><value>${MASTER_HOST}</value></property>
  <property><name>yarn.resourcemanager.bind-host</name><value>0.0.0.0</value></property>
  <property><name>yarn.resourcemanager.scheduler.class</name>
            <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>${YARN_MIN_ALLOCATION_MB}</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>${NM_MEMORY_MB}</value></property>
  <property><name>yarn.scheduler.maximum-allocation-vcores</name><value>${NM_VCORES}</value></property>

  <!-- NodeManager (worker) -->
  <property><name>yarn.nodemanager.bind-host</name><value>0.0.0.0</value></property>
  <property><name>yarn.nodemanager.address</name><value>0.0.0.0:8041</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>${NM_MEMORY_MB}</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>${NM_VCORES}</value></property>
  <property><name>yarn.nodemanager.local-dirs</name><value>/data/yarn/local</value></property>
  <property><name>yarn.nodemanager.log-dirs</name><value>/data/yarn/logs</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
  <!-- Codespaces / small VM disks are often >90% full; the default would mark the NM unhealthy -->
  <property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>99.0</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name>
            <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,SPARK_HOME,PYSPARK_PYTHON</value></property>

  <!-- Spark external shuffle service (enables dynamic allocation) -->
  <property><name>yarn.nodemanager.aux-services</name><value>spark_shuffle</value></property>
  <property><name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
            <value>org.apache.spark.network.yarn.YarnShuffleService</value></property>

  <!-- Container logs -> object storage after the app finishes -->
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
  <property><name>yarn.nodemanager.remote-app-log-dir</name><value>${STORAGE_ROOT}/yarn-logs</value></property>
  <property><name>yarn.log-aggregation.retain-seconds</name><value>604800</value></property>
</configuration>

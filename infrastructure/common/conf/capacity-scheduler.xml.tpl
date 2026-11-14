<?xml version="1.0"?>
<!-- Single queue (like a Dataproc cluster): every job goes to "default" and gets the whole cluster. -->
<configuration>
  <!-- Small clusters: the Hadoop default (0.1) leaves jobs stuck in ACCEPTED -->
  <property><name>yarn.scheduler.capacity.maximum-am-resource-percent</name><value>${YARN_AM_RESOURCE_PERCENT}</value></property>
  <property><name>yarn.scheduler.capacity.resource-calculator</name>
            <value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value></property>

  <property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property>

  <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>100</value></property>
</configuration>

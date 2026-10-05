<?xml version="1.0"?>
<!-- Queues: default (ad-hoc, no queue given), etl (pipelines), adhoc (analysts / AI agents).
     Each queue can borrow the whole cluster when it is idle (maximum-capacity=100). -->
<configuration>
  <!-- Small clusters: the Hadoop default (0.1) leaves jobs stuck in ACCEPTED -->
  <property><name>yarn.scheduler.capacity.maximum-am-resource-percent</name><value>${YARN_AM_RESOURCE_PERCENT}</value></property>
  <property><name>yarn.scheduler.capacity.resource-calculator</name>
            <value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value></property>

  <property><name>yarn.scheduler.capacity.root.queues</name><value>default,etl,adhoc</value></property>

  <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>40</value></property>
  <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>100</value></property>

  <property><name>yarn.scheduler.capacity.root.etl.capacity</name><value>40</value></property>
  <property><name>yarn.scheduler.capacity.root.etl.maximum-capacity</name><value>100</value></property>

  <property><name>yarn.scheduler.capacity.root.adhoc.capacity</name><value>20</value></property>
  <property><name>yarn.scheduler.capacity.root.adhoc.maximum-capacity</name><value>100</value></property>
</configuration>

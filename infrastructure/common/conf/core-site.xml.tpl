<?xml version="1.0"?>
<!-- Rendered by entrypoint.sh from .env. No HDFS: object storage (or a shared
     volume in Codespaces) is the cluster filesystem, just like gs:// on Dataproc. -->
<configuration>
  <property><name>fs.defaultFS</name><value>${DEFAULT_FS}</value></property>
  <property><name>hadoop.tmp.dir</name><value>/data/hadoop-tmp</value></property>
  ${AZURE_AUTH_PROPERTIES}
</configuration>

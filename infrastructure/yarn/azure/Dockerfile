# =============================================================================
# spark-bridge-yarn : AZURE image  (Dataproc-lite, no HDFS)
#
# Build context: infrastructure/yarn   (see compose.master.yml / compose.worker.yml)
#
# Base : eclipse-temurin:11-jre-jammy
# Adds : Apache Spark 3.5.1 (bin-hadoop3, bundles Hadoop 3.3.4 client)
#        Apache Hadoop 3.3.4  -> YARN daemons only (RM / NM), same version as
#                                Spark's client so ONE hadoop-azure jar fits both
#        Apache Livy 0.8.0 (Scala 2.12 / Spark 3 build)
#        ABFS connector (hadoop-azure) on BOTH Hadoop and Spark classpaths
#        Spark YARN shuffle service jar in the NodeManager classpath
#
# Same image runs every role on the master VM and the worker VM.
# The only difference from codespaces/Dockerfile is the Azure connector block.
# =============================================================================
FROM eclipse-temurin:11-jre-jammy

ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3.3.4
ARG LIVY_VERSION=0.8.0-incubating

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget curl unzip procps python3 python3-pip gettext-base netcat-openbsd && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

# PYSPARK_ARCHIVES_PATH: ship pyspark from the image instead of uploading it to staging on every submit
ENV SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    LIVY_HOME=/opt/livy
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
    YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop \
    SPARK_CONF_DIR=${SPARK_HOME}/conf \
    PATH="${SPARK_HOME}/bin:${HADOOP_HOME}/bin:${LIVY_HOME}/bin:${PATH}" \
    PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip" \
    PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_ARCHIVES_PATH=local:/opt/spark/python/lib/pyspark.zip,local:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# --- Spark --------------------------------------------------------------------
RUN wget -q "${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}" && \
    rm /tmp/spark.tgz

# --- Hadoop (YARN only) ---------------------------------------------------------
# Drop docs, the 280 MB AWS SDK bundle and HDFS webapps we never run.
RUN wget -q "${APACHE_MIRROR}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -O /tmp/hadoop.tgz && \
    tar -xzf /tmp/hadoop.tgz -C /opt/ && \
    mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" && \
    rm /tmp/hadoop.tgz && \
    rm -rf ${HADOOP_HOME}/share/doc \
           ${HADOOP_HOME}/share/hadoop/hdfs/webapps \
           ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar \
           ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aliyun-* \
           ${HADOOP_HOME}/share/hadoop/tools/lib/aliyun-* \
           ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-*

# Spark external shuffle service runs inside the NodeManager
RUN cp ${SPARK_HOME}/yarn/spark-${SPARK_VERSION}-yarn-shuffle.jar ${HADOOP_HOME}/share/hadoop/yarn/lib/

# --- Azure ABFS connector ---------------------------------------------------------
# Hadoop side : HADOOP_OPTIONAL_TOOLS puts tools/lib/hadoop-azure + deps on the
#               RM/NM classpath (NM localizes staging files and uploads logs to abfss://).
# Spark side  : copy the very same jars into $SPARK_HOME/jars (drivers/executors,
#               spark-submit inside Livy, History Server).
ENV HADOOP_OPTIONAL_TOOLS=hadoop-azure
RUN cd ${HADOOP_HOME}/share/hadoop && \
    cp tools/lib/hadoop-azure-${HADOOP_VERSION}.jar \
       tools/lib/azure-storage-*.jar \
       tools/lib/azure-keyvault-core-*.jar \
       tools/lib/wildfly-openssl-*.jar \
       ${SPARK_HOME}/jars/ && \
    cp -n common/lib/hadoop-shaded-guava-*.jar ${SPARK_HOME}/jars/ && \
    ls ${SPARK_HOME}/jars | grep -E 'hadoop-azure|wildfly|shaded-guava'

# --- Livy -----------------------------------------------------------------------------
RUN wget -q "${APACHE_MIRROR}/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /opt/ && \
    mv /opt/apache-livy-${LIVY_VERSION}* "${LIVY_HOME}" && \
    rm /tmp/livy.zip && \
    mkdir -p ${LIVY_HOME}/logs /var/livy/recovery

ENV LIVY_SERVER_JAVA_OPTS="-Xmx1g \
  --add-opens java.base/sun.security.krb5=ALL-UNNAMED \
  --add-opens java.base/sun.security.util=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED"

# Hadoop refuses to start daemons as root unless told who runs them
ENV YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    YARN_RESOURCEMANAGER_OPTS="-Xmx1g" \
    YARN_NODEMANAGER_OPTS="-Xmx512m" \
    SPARK_DAEMON_MEMORY=512m

# --- SparkBridge config + entrypoint ---------------------------------------------------
COPY common/conf/ /opt/sparkbridge/conf/
COPY common/conf/livy-log4j.properties ${LIVY_HOME}/conf/log4j.properties
COPY common/entrypoint.sh /opt/sparkbridge/entrypoint.sh
RUN sed -i 's/\r$//' /opt/sparkbridge/entrypoint.sh && chmod +x /opt/sparkbridge/entrypoint.sh && \
    mkdir -p /data/yarn/local /data/yarn/logs /data/hadoop-tmp

WORKDIR /opt/sparkbridge

# 8088 RM UI | 8030-8033 RM RPC | 8041 NM RPC | 8042 NM UI | 7337 shuffle
# 18080 History | 8998 Livy | 10000-10010 Livy RSC callback
EXPOSE 8088 8030 8031 8032 8033 8041 8042 7337 18080 8998

ENTRYPOINT ["/opt/sparkbridge/entrypoint.sh"]
CMD ["help"]

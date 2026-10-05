# =============================================================================
# spark-bridge-yarn : CODESPACES image  (QA / launch test, single host)
#
# Build context: infrastructure   (see codespaces/docker-compose.yml)
#
# Identical to azure/Dockerfile EXCEPT: no Azure ABFS connector
# (storage = shared Docker volume, file:///data/shared).
#
# Sources (nothing from archive.apache.org):
#   Spark 3.5.1   <- official image apache/spark   (Docker Hub CDN)
#   Hadoop 3.3.6  <- official image apache/hadoop  (Docker Hub CDN), YARN daemons only
#   Livy 0.8.0    <- dlcdn.apache.org              (Apache CDN)
#
# Keep image tags / versions in sync with azure/Dockerfile: QA must test what runs on Azure.
# =============================================================================

ARG SPARK_IMAGE=apache/spark:3.5.1
ARG HADOOP_IMAGE=apache/hadoop:3.3.6

# --- Source stages: take the distributions out of the official images ------------
FROM ${SPARK_IMAGE} AS spark

FROM ${HADOOP_IMAGE} AS hadoop
USER root
# Drop docs, HDFS webapps and optional cloud SDKs (not needed in QA)
RUN rm -rf /opt/hadoop/share/doc \
           /opt/hadoop/share/hadoop/hdfs/webapps \
           /opt/hadoop/share/hadoop/tools/lib/*

# --- Final image ----------------------------------------------------------------------
FROM eclipse-temurin:11-jre-jammy

ARG LIVY_VERSION=0.8.0-incubating
ARG LIVY_URL=https://dlcdn.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl unzip procps python3 python3-pip gettext-base netcat-openbsd && \
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

COPY --from=spark  /opt/spark  /opt/spark
COPY --from=hadoop /opt/hadoop /opt/hadoop
# The official Spark image ships no conf/ dir; entrypoint.sh renders spark-defaults.conf into it
RUN mkdir -p ${SPARK_HOME}/conf && \
    test -x ${SPARK_HOME}/bin/spark-submit && \
    test -f ${SPARK_HOME}/python/lib/pyspark.zip && \
    test -x ${HADOOP_HOME}/bin/yarn

# --- Livy -----------------------------------------------------------------------------
RUN curl -fSL --retry 5 --retry-all-errors -o /tmp/livy.zip "${LIVY_URL}" && \
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

# 8088 RM UI | 8030-8033 RM RPC | 8041 NM RPC | 8042 NM UI
# 18080 History | 8998 Livy | 10000-10010 Livy RSC callback
EXPOSE 8088 8030 8031 8032 8033 8041 8042 18080 8998

ENTRYPOINT ["/opt/sparkbridge/entrypoint.sh"]
CMD ["help"]

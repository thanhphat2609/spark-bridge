# =============================================================================
# spark-bridge-yarn : CODESPACES image  (QA / launch test, single host)
#
# Build context: infrastructure   (see codespaces/docker-compose.yml)
#
# Identical to azure/Dockerfile EXCEPT:
#   - no cloud storage connector (storage = shared Docker volume, file:///data/shared)
#   - all optional Hadoop tools (S3/Azure/Aliyun SDKs) removed -> smaller, faster build
#
# Keep the version ARGs in sync with azure/Dockerfile: QA must test the same
# Spark / Hadoop / Livy versions that run on Azure.
# =============================================================================
FROM eclipse-temurin:11-jre-jammy

# Mirrors tried in order (archive.apache.org throttles big downloads; Huawei Cloud
# mirrors old releases). Every file is verified against the official SHA-512 below,
# so a mirror can't change the content. When bumping a version, update its checksum
# from https://archive.apache.org/dist/<path>.sha512 and keep both Dockerfiles in sync.
ARG APACHE_MIRRORS="https://archive.apache.org/dist https://repo.huaweicloud.com/apache"
ARG SPARK_VERSION=3.5.1
ARG SPARK_SHA512=3d8e3f082c602027d540771e9eba9987f8ea955e978afc29e1349eb6e3f9fe32543e3d3de52dff048ebbd789730454c96447c86ff5b60a98d72620a0f082b355
ARG HADOOP_VERSION=3.3.4
ARG HADOOP_SHA512=ca5e12625679ca95b8fd7bb7babc2a8dcb2605979b901df9ad137178718821097b67555115fafc6dbf6bb32b61864ccb6786dbc555e589694a22bf69147780b4
ARG LIVY_VERSION=0.8.0-incubating
ARG LIVY_SHA512=467c977d4c72c67c2f7fa08134b9d0eeb5123b843cba67b5840620dba521e7f8cd532e7ab00b97b3e73cd737658f8937b0fea6e8179242a5f1a6ce29ba7b1d4f

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget curl unzip procps python3 python3-pip gettext-base netcat-openbsd && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

# Mirror-fallback + checksum-verified downloader
COPY common/fetch.sh /usr/local/bin/fetch-apache
RUN sed -i 's/\r$//' /usr/local/bin/fetch-apache && chmod +x /usr/local/bin/fetch-apache

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
RUN fetch-apache "spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" "${SPARK_SHA512}" /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}" && \
    rm /tmp/spark.tgz

# --- Hadoop (YARN only) ---------------------------------------------------------
# Drop docs, HDFS webapps and every optional cloud SDK (not needed in QA).
RUN fetch-apache "hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" "${HADOOP_SHA512}" /tmp/hadoop.tgz && \
    tar -xzf /tmp/hadoop.tgz -C /opt/ && \
    mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" && \
    rm /tmp/hadoop.tgz && \
    rm -rf ${HADOOP_HOME}/share/doc \
           ${HADOOP_HOME}/share/hadoop/hdfs/webapps \
           ${HADOOP_HOME}/share/hadoop/tools/lib/*

# Spark external shuffle service runs inside the NodeManager
RUN cp ${SPARK_HOME}/yarn/spark-${SPARK_VERSION}-yarn-shuffle.jar ${HADOOP_HOME}/share/hadoop/yarn/lib/

# --- Livy -----------------------------------------------------------------------------
RUN fetch-apache "incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" "${LIVY_SHA512}" /tmp/livy.zip && \
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

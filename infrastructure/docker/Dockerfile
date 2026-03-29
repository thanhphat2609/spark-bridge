# =============================================================================
# spark-bridge image
# Built from scratch -- no dependency on bitnami or any third-party Spark image.
#
# Base : eclipse-temurin:11-jre-jammy  (Eclipse OpenJDK 11 on Ubuntu 22.04)
# Adds : Apache Spark 3.5.1  (downloaded from Apache archives)
#        Apache Livy  0.7.1  (downloaded from Apache archives)
#        Python 3 + PySpark wiring
# =============================================================================
FROM eclipse-temurin:11-jre-jammy

# Install system utilities and Python 3 (required for PySpark)
RUN apt-get update && \
    apt-get install -y wget curl unzip procps python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

# --------------------------------------------------------------------------
# Environment variables
# JAVA_HOME is already set by the eclipse-temurin base image.
# --------------------------------------------------------------------------
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV LIVY_VERSION=0.7.1-incubating
ENV LIVY_HOME=/opt/livy
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${LIVY_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON=python3

# --------------------------------------------------------------------------
# Apache Spark installation
# Using the hadoop3 variant which is the standard distribution.
# --------------------------------------------------------------------------
RUN wget -q \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "${SPARK_HOME}" && \
    rm /tmp/spark.tgz

# --------------------------------------------------------------------------
# Apache Livy installation
# Livy is the REST gateway -- clients submit jobs here, no Spark needed locally.
# --------------------------------------------------------------------------
RUN wget -q \
    "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}-bin.zip" \
    -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /opt/ && \
    mv "/opt/apache-livy-${LIVY_VERSION}-bin" "${LIVY_HOME}" && \
    rm /tmp/livy.zip && \
    mkdir -p "${LIVY_HOME}/logs"

# Copy Livy configuration and log4j settings
COPY livy.conf ${LIVY_HOME}/conf/livy.conf
COPY log4j.properties ${LIVY_HOME}/conf/log4j.properties

# Java 11 opens required by Livy's Hadoop 2.7 dependency (KerberosUtil uses
# internal sun.security APIs via reflection).  These flags suppress the
# illegal-reflective-access warnings that would otherwise flood the logs.
ENV LIVY_SERVER_JAVA_OPTS="--add-opens java.base/sun.security.krb5=ALL-UNNAMED \
  --add-opens java.base/sun.security.util=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED"

# Default working directory is SPARK_HOME so relative commands like
# "bin/spark-class" work without a full path in docker-compose.
WORKDIR ${SPARK_HOME}

# Ports (documented here for reference; exposed per service via docker-compose):
#   8080 - Spark Master Web UI
#   7077 - Spark Master RPC
#   8998 - Livy REST API  <-- the only port clients need
#   4040 - Spark Application Web UI (active jobs)
#   8081 - Spark Worker Web UI
EXPOSE 8080 7077 8998 4040 8081

# No ENTRYPOINT defined here.
# Each service in docker-compose provides its own command, which makes the
# role of each container explicit: spark-master, livy, worker-1, worker-2.

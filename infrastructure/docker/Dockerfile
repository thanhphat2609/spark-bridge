# Base image: Bitnami Spark 3.5.1
FROM bitnami/spark:3.5.1

USER root

# Install system tools required for Livy download and operation
RUN apt-get update && \
    apt-get install -y wget unzip procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------------------------
# Apache Livy installation
# Livy acts as the REST gateway that allows clients to submit Spark jobs
# without having Spark installed locally.
# --------------------------------------------------------------------------
ENV LIVY_VERSION=0.7.1-incubating
ENV LIVY_HOME=/opt/livy
ENV SPARK_HOME=/opt/bitnami/spark
ENV JAVA_HOME=/opt/bitnami/java
ENV PATH="${PATH}:${LIVY_HOME}/bin"

RUN wget -q \
    "https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}-bin.zip" \
    -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /opt/ && \
    mv "/opt/apache-livy-${LIVY_VERSION}-bin" "${LIVY_HOME}" && \
    rm /tmp/livy.zip && \
    mkdir -p "${LIVY_HOME}/logs"

# Copy Livy configuration and startup script
COPY livy.conf ${LIVY_HOME}/conf/livy.conf
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Ports:
#   8080 - Spark Master Web UI
#   7077 - Spark Master RPC
#   8998 - Livy REST API  <-- clients only need this port
#   4040 - Spark Application Web UI (active jobs)
EXPOSE 8080 7077 8998 4040

ENTRYPOINT ["/opt/entrypoint.sh"]

#!/bin/bash
# =============================================================================
# SparkBridge (YARN profile) - container entrypoint
#
# One image, many roles. The role is the first argument:
#
#   resourcemanager   YARN ResourceManager           (master VM)
#   history           Spark History Server           (master VM)
#   livy              Apache Livy REST gateway        (master VM)
#   nodemanager       YARN NodeManager + shuffle svc  (worker VM)
#   smoke-storage     Read/write test against ${STORAGE_ROOT}, then exit
#   <anything else>   exec'd as-is (e.g. "bash")
#
# Every config file is rendered from /opt/sparkbridge/conf/*.tpl using the
# environment (.env), so the same image works in Codespaces and on Azure.
# =============================================================================

set -euo pipefail

ROLE="${1:-help}"
TPL_DIR=/opt/sparkbridge/conf

# -----------------------------------------------------------------------------
# Defaults (override in .env / compose)
# -----------------------------------------------------------------------------
export MASTER_HOST="${MASTER_HOST:-master}"
export STORAGE_BACKEND="${STORAGE_BACKEND:-local}"          # local | azure
export STORAGE_ROOT="${STORAGE_ROOT:-file:///data/shared}"
export NM_MEMORY_MB="${NM_MEMORY_MB:-4096}"
export NM_VCORES="${NM_VCORES:-2}"
export YARN_MIN_ALLOCATION_MB="${YARN_MIN_ALLOCATION_MB:-256}"
export YARN_AM_RESOURCE_PERCENT="${YARN_AM_RESOURCE_PERCENT:-0.5}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-512m}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-512m}"
export SPARK_MAX_EXECUTORS="${SPARK_MAX_EXECUTORS:-2}"
export AZURE_AUTH_MODE="${AZURE_AUTH_MODE:-key}"             # key | msi

# fs.defaultFS = scheme + authority of STORAGE_ROOT (file:/// for local).
if [[ -z "${DEFAULT_FS:-}" ]]; then
  if [[ "${STORAGE_ROOT}" == file:* ]]; then
    DEFAULT_FS="file:///"
  else
    DEFAULT_FS="$(echo "${STORAGE_ROOT}" | sed -E 's#^([a-z0-9]+://[^/]+).*#\1#')"
  fi
fi
export DEFAULT_FS

# -----------------------------------------------------------------------------
# Storage credentials -> core-site.xml snippet
# -----------------------------------------------------------------------------
AZURE_AUTH_PROPERTIES=""
if [[ "${STORAGE_BACKEND}" == "azure" ]]; then
  : "${AZURE_STORAGE_ACCOUNT:?AZURE_STORAGE_ACCOUNT is required when STORAGE_BACKEND=azure}"
  ACCT_HOST="${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
  if [[ "${AZURE_AUTH_MODE}" == "msi" ]]; then
    # VM managed identity (recommended). Grant it "Storage Blob Data Contributor".
    AZURE_AUTH_PROPERTIES="
  <property><name>fs.azure.account.auth.type.${ACCT_HOST}</name><value>OAuth</value></property>
  <property><name>fs.azure.account.oauth.provider.type.${ACCT_HOST}</name><value>org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider</value></property>
  <property><name>fs.azure.account.oauth2.msi.tenant.${ACCT_HOST}</name><value>${AZURE_TENANT_ID:-}</value></property>
  <property><name>fs.azure.account.oauth2.client.id.${ACCT_HOST}</name><value>${AZURE_MSI_CLIENT_ID:-}</value></property>"
  else
    : "${AZURE_STORAGE_KEY:?AZURE_STORAGE_KEY is required when AZURE_AUTH_MODE=key}"
    AZURE_AUTH_PROPERTIES="
  <property><name>fs.azure.account.auth.type.${ACCT_HOST}</name><value>SharedKey</value></property>
  <property><name>fs.azure.account.key.${ACCT_HOST}</name><value>${AZURE_STORAGE_KEY}</value></property>"
  fi
fi
export AZURE_AUTH_PROPERTIES

# -----------------------------------------------------------------------------
# Render templates (only our variables are substituted)
# -----------------------------------------------------------------------------
VARS='${MASTER_HOST} ${STORAGE_ROOT} ${DEFAULT_FS} ${NM_MEMORY_MB} ${NM_VCORES}
${YARN_MIN_ALLOCATION_MB} ${YARN_AM_RESOURCE_PERCENT} ${SPARK_DRIVER_MEMORY}
${SPARK_EXECUTOR_MEMORY} ${SPARK_MAX_EXECUTORS} ${SPARK_HOME} ${AZURE_AUTH_PROPERTIES}'

render() { envsubst "${VARS}" < "$1" > "$2"; }

render "${TPL_DIR}/core-site.xml.tpl"           "${HADOOP_CONF_DIR}/core-site.xml"
render "${TPL_DIR}/yarn-site.xml.tpl"           "${HADOOP_CONF_DIR}/yarn-site.xml"
render "${TPL_DIR}/capacity-scheduler.xml.tpl"  "${HADOOP_CONF_DIR}/capacity-scheduler.xml"
render "${TPL_DIR}/spark-defaults.conf.tpl"     "${SPARK_HOME}/conf/spark-defaults.conf"
render "${TPL_DIR}/livy.conf.tpl"               "${LIVY_HOME}/conf/livy.conf"
render "${TPL_DIR}/livy-client.conf.tpl"        "${LIVY_HOME}/conf/livy-client.conf"

# Codespaces reads job files from a shared volume via file:// -> Livy must whitelist it.
if [[ -n "${LIVY_LOCAL_DIR_WHITELIST:-}" ]]; then
  echo "livy.file.local-dir-whitelist = ${LIVY_LOCAL_DIR_WHITELIST}" >> "${LIVY_HOME}/conf/livy.conf"
fi

# Create the shared folders in object storage / shared volume (idempotent).
ensure_dirs() {
  hadoop fs -mkdir -p \
    "${STORAGE_ROOT}/staging" \
    "${STORAGE_ROOT}/spark-history" \
    "${STORAGE_ROOT}/yarn-logs"
}

banner() {
  echo "----------------------------------------------------------------------"
  echo " SparkBridge role: ${ROLE}"
  echo "   MASTER_HOST  = ${MASTER_HOST}"
  echo "   STORAGE_ROOT = ${STORAGE_ROOT}  (backend: ${STORAGE_BACKEND})"
  echo "----------------------------------------------------------------------"
}

case "${ROLE}" in
  resourcemanager)
    banner
    ensure_dirs
    exec yarn resourcemanager
    ;;

  nodemanager)
    banner
    mkdir -p /data/yarn/local /data/yarn/logs
    exec yarn nodemanager
    ;;

  history)
    banner
    ensure_dirs
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;

  livy)
    banner
    mkdir -p /var/livy/recovery "${LIVY_HOME}/logs"
    # livy-server without "start" runs in the foreground.
    exec "${LIVY_HOME}/bin/livy-server"
    ;;

  smoke-storage)
    banner
    SMOKE="${STORAGE_ROOT}/_smoke"
    echo "[1/2] Hadoop client (what the NodeManager uses) -> ${SMOKE}"
    hadoop fs -mkdir -p "${SMOKE}"
    echo "hello from $(hostname) at $(date -u +%FT%TZ)" > /tmp/smoke.txt
    hadoop fs -put -f /tmp/smoke.txt "${SMOKE}/hadoop.txt"
    hadoop fs -cat "${SMOKE}/hadoop.txt"

    echo "[2/2] Spark client (what drivers/executors use) -> ${SMOKE}/spark"
    cat > /tmp/smoke.py <<EOF
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke-storage").getOrCreate()
path = "${SMOKE}/spark"
spark.range(100).write.mode("overwrite").parquet(path)
print("SMOKE rows read back:", spark.read.parquet(path).count())
spark.stop()
EOF
    spark-submit --master "local[1]" --deploy-mode client \
      --conf spark.eventLog.enabled=false \n      --conf spark.shuffle.service.enabled=false \n      --conf spark.dynamicAllocation.enabled=false \n      /tmp/smoke.py
    echo "Storage smoke test PASSED"
    ;;

  help)
    echo "Usage: entrypoint.sh {resourcemanager|nodemanager|history|livy|smoke-storage|<cmd>}"
    ;;

  *)
    exec "$@"
    ;;
esac

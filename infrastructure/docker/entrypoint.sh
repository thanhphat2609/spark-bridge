#!/bin/bash
# =============================================================================
# Container entrypoint for the Spark Master + Apache Livy node.
#
# Start order:
#   1. Spark Master daemon  (background, writes logs to ${SPARK_HOME}/logs/)
#   2. Apache Livy server   (foreground, keeps the container alive)
# =============================================================================

set -e

SPARK_HOME=${SPARK_HOME:-/opt/bitnami/spark}
LIVY_HOME=${LIVY_HOME:-/opt/livy}

echo "----------------------------------------------------------------------"
echo " Starting Spark Master"
echo "----------------------------------------------------------------------"
"${SPARK_HOME}/sbin/start-master.sh"

echo "----------------------------------------------------------------------"
echo " Waiting 10 s for Spark Master to become ready ..."
echo "----------------------------------------------------------------------"
sleep 10

echo "----------------------------------------------------------------------"
echo " Starting Apache Livy REST Server"
echo ""
echo "   Spark Master Web UI : http://0.0.0.0:8080"
echo "   Livy REST API       : http://0.0.0.0:8998"
echo ""
echo " Clients only need to reach port 8998."
echo " No Spark installation is required on any client machine."
echo "----------------------------------------------------------------------"

# exec replaces the shell with Livy, running in foreground.
# This keeps the container alive and pipes Livy logs directly to stdout.
exec "${LIVY_HOME}/bin/livy-server"

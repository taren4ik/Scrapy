#!/bin/bash

CONTAINER="airflow_carprice-airflow-scheduler"
LOG_DIR="/opt/airflow/logs/scheduler"

docker exec "$CONTAINER" bash -c "
cd $LOG_DIR || exit 1

MAX_DATE=\$(ls -d 20* 2>/dev/null | sort | tail -n 1)

for d in */; do
  d=\${d%/}
  if [[ \"\$d\" != \"latest\" && \"\$d\" != \"\$MAX_DATE\" ]]; then
    rm -rf \"\$d\"
  fi
done
"

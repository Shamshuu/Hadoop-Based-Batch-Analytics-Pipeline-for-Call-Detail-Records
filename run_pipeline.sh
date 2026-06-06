#!/bin/bash
# run_pipeline.sh

if [ -z "$1" ]; then
  echo "Usage: ./run_pipeline.sh <logical_query_name>"
  echo "Supported names: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
  exit 1
fi

QUERY=$1
RUN_ID=$(date +"%Y%m%d_%H%M%S")

case $QUERY in
  top_callers)
    DAG_ID="top_callers_by_spend_dag"
    ;;
  tower_heatmap)
    DAG_ID="tower_utilization_heatmap_dag"
    ;;
  anomalous_calls)
    DAG_ID="anomalous_call_detection_dag"
    ;;
  revenue_recon)
    DAG_ID="revenue_reconciliation_dag"
    ;;
  *)
    echo "Unknown logical query: $QUERY"
    exit 1
    ;;
esac

echo "Triggering DAG $DAG_ID with run_id $RUN_ID..."

# Trigger the DAG using airflow CLI inside the docker container
docker exec airflow airflow dags trigger -r "$RUN_ID" --conf "{\"run_id\":\"$RUN_ID\"}" "$DAG_ID"
if [ $? -ne 0 ]; then
  echo "Failed to trigger DAG $DAG_ID"
  exit 1
fi

echo "Waiting for DAG run to complete..."
while true; do
  STATE=$(docker exec airflow python3 -c "from airflow.models import DagRun; dr = DagRun.find(dag_id='$DAG_ID', run_id='$RUN_ID'); print(dr[0].state if dr else 'queued')" 2>/dev/null)
  STATE=$(echo "$STATE" | tr -d '\r\n[:space:]')
  echo "Current state: $STATE"
  if [ "$STATE" = "success" ]; then
    echo "DAG completed successfully!"
    break
  elif [ "$STATE" = "failed" ]; then
    echo "DAG run failed!"
    exit 1
  elif [ -z "$STATE" ]; then
    # In case the python command fails due to DB lock or other transient issues, just wait and retry
    echo "State check returned empty, retrying..."
  fi
  sleep 5
done

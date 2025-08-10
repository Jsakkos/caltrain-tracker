#!/bin/bash
set -e

echo "Starting Prefect server..."
# Start Prefect server in the background
prefect server start --host 0.0.0.0 &
SERVER_PID=$!

# Wait for Prefect server to be ready
echo "Waiting for Prefect server to be ready..."
until curl --silent --fail http://localhost:4200/api/health &>/dev/null; do
  echo "Waiting for Prefect server to be ready..."
  sleep 5
done

echo "Prefect server is ready. Creating work pool..."
# Create the 'process' work pool
prefect work-pool create "my-pool" --type process --overwrite
echo "Work pool created. Starting worker..."

# Start the worker in the background
prefect worker start --pool "my-pool" &
WORKER_PID=$!

echo "Worker started. Waiting for worker to connect..."
sleep 10

echo "Deploying flows..."
# Deploy flows
cd /app && python src/deployments/deploy_flows.py

echo "Flows deployed. Keeping container running..."
# Monitor processes
while true; do
  if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "Prefect server process died, restarting..."
    prefect server start --host 0.0.0.0 &
    SERVER_PID=$!
  fi
  
  if ! kill -0 $WORKER_PID 2>/dev/null; then
    echo "Worker process died, restarting..."
    prefect worker start --pool "my-pool" &
    WORKER_PID=$!
  fi
  
  sleep 30
done
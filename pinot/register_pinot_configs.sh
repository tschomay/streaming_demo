#!/bin/bash

# Default to "pinot-controller" if not set
CONTROLLER_HOST=${PINOT_CONTROLLER_HOST:-pinot-controller}

echo "Waiting for Pinot Controller at $CONTROLLER_HOST..."

# 1. Wait for Controller API to be responsive
until curl -s http://"$CONTROLLER_HOST":9000/health > /dev/null; do
  echo "Controller not up yet... retrying in 2s"
  sleep 2
done

echo "Controller is up! Now waiting for Brokers and Servers to join..."

# 2. Wait for at least one Broker and one Server to join the cluster
# We grep the /instances response for "Broker" and "Server" keys
until curl -s http://"$CONTROLLER_HOST":9000/instances | grep -q "Broker" && \
      curl -s http://"$CONTROLLER_HOST":9000/instances | grep -q "Server"; do
  echo "Cluster members not ready (waiting for Broker/Server)... retrying in 5s"
  sleep 5
done

echo "Cluster is ready! Registering schemas and tables..."

# 3. Register all schemas
for schema in /opt/pinot/config/schemas/*.json; do
  echo "Registering schema: $schema"
  curl -s -X POST -H "Content-Type: application/json" -d @"$schema" http://"$CONTROLLER_HOST":9000/schemas
  echo "" # Newline for readability
done

# 4. Register all tables
for table in /opt/pinot/config/tables/*.json; do
  echo "Registering table: $table"
  curl -s -X POST -H "Content-Type: application/json" -d @"$table" http://"$CONTROLLER_HOST":9000/tables
  echo "" 
done

echo "Schema and table registration complete."

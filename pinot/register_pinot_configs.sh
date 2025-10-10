#!/bin/bash

# wait until the controller is up
echo "Waiting for Pinot controller to start..."
until curl -s http://localhost:9000/health; do
  sleep 2
done

echo "Pinot controller is up. Registering schemas and tables..."

# register all schemas
for schema in /opt/pinot/config/schemas/*.json; do
  echo "Registering schema: $schema"
  curl -s -X POST -H "Content-Type: application/json" -d @"$schema" http://localhost:9000/schemas
done

# register all tables
for table in /opt/pinot/config/tables/*.json; do
  echo "Registering table: $table"
  curl -s -X POST -H "Content-Type: application/json" -d @"$table" http://localhost:9000/tables
done

echo "Schema and table registration complete."



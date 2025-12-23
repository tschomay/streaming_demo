#!/bin/bash

# Default to "pinot-controller" if not set
CONTROLLER_HOST=${PINOT_CONTROLLER_HOST:-pinot-controller}

echo "Waiting for Pinot Controller at $CONTROLLER_HOST..."

until curl -s http://"$CONTROLLER_HOST":9000/health; do
  sleep 2
done

echo "Pinot controller is up. Registering schemas and tables..."

# Register all schemas
for schema in /opt/pinot/config/schemas/*.json; do
  echo "Registering schema: $schema"
  # Use -f to fail silently on server errors, or remove it to see errors
  curl -s -X POST -H "Content-Type: application/json" -d @"$schema" http://"$CONTROLLER_HOST":9000/schemas
done

# Register all tables
for table in /opt/pinot/config/tables/*.json; do
  echo "Registering table: $table"
  curl -s -X POST -H "Content-Type: application/json" -d @"$table" http://"$CONTROLLER_HOST":9000/tables
done

echo "Schema and table registration complete."

#!/bin/bash
# Start Flink cluster
start-cluster.sh

# Wait for JobManager to be ready
sleep 10

# Submit the Python job
flink run -py /opt/flink/user_jobs/taxi_ride_enrich.py

# Keep container running
tail -f /dev/null


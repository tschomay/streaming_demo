# Flink + Kafka + Pinot Demo

## Setup

1. Start environment:
   ```bash
   docker compose up -d
   ```

2. Produce some Kafka events:
   ```bash
   docker exec -i kafka kafka-console-producer      --topic taxi_rides      --bootstrap-server kafka:9092 <<EOF
   {"ride_id":1,"pickup":"Manhattan","fare":12.5}
   {"ride_id":2,"pickup":"Brooklyn","fare":8.0}
   {"ride_id":3,"pickup":"Queens","fare":15.0}
   EOF
   ```

3. Submit Flink job:
   ```bash
   docker exec -it jobmanager ./bin/flink run -py /opt/flink-job/flink_job.py
   ```

4. Add Pinot schema & table:
   ```bash
   docker exec -it pinot-controller bin/pinot-admin.sh AddSchema      -schemaFile /opt/pinot/fare_totals.schema.json -exec

   docker exec -it pinot-controller bin/pinot-admin.sh AddTable      -tableConfigFile /opt/pinot/fare_totals.table.json -exec
   ```

5. Query Pinot (http://localhost:9000/query):
   ```sql
   SELECT pickup, SUM(total) AS grand_total
   FROM fare_totals
   GROUP BY pickup
   ORDER BY grand_total DESC;
   ```

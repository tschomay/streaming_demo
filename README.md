# Flink + Kafka + Pinot Demo

## Setup

1. Start environment:
   ```bash
   docker-compose up -d
   ```

2.  Create new topic
  ```bash
  docker-compose exec kafka kafka-topics \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic taxi_rides
  ```

3. Produce some Kafka events:
   ```bash
   docker-compose exec -i kafka kafka-console-producer      --topic taxi_rides      --bootstrap-server kafka:9092 <<EOF
   {"ride_id":1,"pickup":"Manhattan","fare":12.5}
   {"ride_id":2,"pickup":"Brooklyn","fare":8.0}
   {"ride_id":3,"pickup":"Queens","fare":15.0}
   EOF
   ```

4. Consume messages
   ```bash
   docker-compose exec kafka kafka-console-consumer --topic taxi_rides --bootstrap-server kafka:9092 --from-beginning --max-messages 3
   ```

5. Create new topic for Flink Analytics
   ```bash
   docker-compose exec kafka kafka-topics \
   --bootstrap-server kafka:9092 \
   --create \
   --topic taxi_rides_enriched \
   --partitions 1 \
   --replication-factor 1
   ```

6. Submit Flink job:
   ```bash
   docker-compose exec -d jobmanager flink run -py /opt/flink/user_jobs/taxi_ride_enrich.py
   ```

7. Check if data is being produced
   ```bash
   docker-compose exec kafka kafka-console-consumer \
   --topic taxi_rides_enriched \
   --bootstrap-server kafka:9092 \
   --from-beginning \
   --max-messages 3
   ```

8. Add and Register Pinot schema & table:
   ```bash
   docker-compose exec pinot-controller /opt/pinot/register_pinot_configs.sh
   ```

   -- OR --

   ```bash
   docker exec -it pinot-controller bin/pinot-admin.sh AddSchema      -schemaFile /opt/pinot/fare_totals.schema.json -exec

   docker exec -it pinot-controller bin/pinot-admin.sh AddTable      -tableConfigFile /opt/pinot/fare_totals.table.json -exec
   ```

9. Query Pinot (http://localhost:9000/query):
   ```sql
   SELECT pickup, SUM(total) AS grand_total
   FROM fare_totals
   GROUP BY pickup
   ORDER BY grand_total DESC;
   ```

  -- OR --
  ```bash
  curl -X POST \
  http://localhost:8099/query/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM taxi_rides LIMIT 5"}'
  ```


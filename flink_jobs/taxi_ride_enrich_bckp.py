from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource, KafkaSink
from pyflink.common.serialization import SimpleStringSchema
import json

env = StreamExecutionEnvironment.get_execution_environment()

# --- Source: read from taxi_rides ---
source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("taxi_rides") \
    .set_group_id("demo-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, watermark_strategy=None, source_name="kafka_source")

# --- Transformation: add fare_category ---
def enrich_ride(msg):
    ride = json.loads(msg)
    fare = ride.get("fare", 0)
    if fare < 10:
        ride["fare_category"] = "cheap"
    elif fare < 15:
        ride["fare_category"] = "medium"
    else:
        ride["fare_category"] = "expensive"
    return json.dumps(ride)

enriched_stream = stream.map(enrich_ride)

# --- Sink: write to taxi_rides_enriched ---
sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(lambda v: (None, v.encode("utf-8"))) \
    .set_topic("taxi_rides_enriched") \
    .build()

enriched_stream.sink_to(sink)

env.execute("Taxi Rides Enrichment Job")


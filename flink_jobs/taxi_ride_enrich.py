from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

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

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # --- Source: read from taxi_rides ---
    kafka_consumer = FlinkKafkaConsumer(
        topics='taxi_rides',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'demo-group'
        }
    )

    # --- Sink: write to taxi_rides_enriched ---
    kafka_producer = FlinkKafkaProducer(
        topic='taxi_rides_enriched',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    # --- Stream processing ---
    stream = env.add_source(kafka_consumer)
    enriched_stream = stream.map(enrich_ride, output_type=Types.STRING())
    enriched_stream.add_sink(kafka_producer)

    env.execute("Taxi Rides Enrichment Job")

if __name__ == '__main__':
    main()


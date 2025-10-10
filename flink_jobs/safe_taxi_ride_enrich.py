import json
import traceback
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema

def safe_enrich(msg):
    """
    Defensive wrapper for parsing/enrichment.
    Returns a JSON string on success, or None on failure.
    """
    try:
        if msg is None:
            print("DEBUG: Received None message")
            return None

        # if bytes, decode to str
        if isinstance(msg, (bytes, bytearray)):
            try:
                msg = msg.decode("utf-8")
            except Exception as e:
                print("DEBUG: Failed to decode bytes message:", e)
                print("MSG BYTES (len={}): {}".format(len(msg), msg))
                return None

        # ensure it's a str
        if not isinstance(msg, str):
            print("DEBUG: Received message of unexpected type:", type(msg), "value:", msg)
            return None

        # parse JSON
        ride = json.loads(msg)

        fare = ride.get("fare", 0) or 0

        if fare < 10:
            ride["fare_category"] = "cheap"
        elif fare < 15:
            ride["fare_category"] = "medium"
        else:
            ride["fare_category"] = "expensive"

        # ALWAYS return a string
        return json.dumps(ride)

    except Exception:
        print("ERROR: Exception while processing message:")
        print("ORIGINAL MESSAGE:", repr(msg))
        traceback.print_exc()
        return None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_consumer = FlinkKafkaConsumer(
        topics='taxi_rides',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'demo-group',
            'auto.offset.reset': 'latest'
        }
    )

    kafka_producer = FlinkKafkaProducer(
        topic='taxi_rides_enriched',
        serialization_schema=SimpleStringSchema(),  # expects Python str
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    stream = env.add_source(kafka_consumer)

    # apply safe map and filter out None
    enriched = stream.map(safe_enrich).filter(lambda x: x is not None)

    enriched.add_sink(kafka_producer)

    env.execute("Taxi Rides Enrichment Job (safe)")

if __name__ == '__main__':
    main()

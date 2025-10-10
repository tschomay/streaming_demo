from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Kafka source
t_env.execute_sql("""
CREATE TABLE taxi_rides (
    ride_id INT,
    pickup STRING,
    fare DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'taxi_rides',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
)
""")

# Kafka sink
t_env.execute_sql("""
CREATE TABLE fare_totals (
    pickup STRING,
    total DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'fare_totals',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
)
""")

# Transformation
t_env.execute_sql("""
INSERT INTO fare_totals
SELECT pickup, SUM(fare) AS total
FROM taxi_rides
GROUP BY pickup
""")

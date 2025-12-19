import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # 1. Setup the Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Keep it simple for the demo
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Add Kafka Connector (Jar is already in the image at this path)
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", 
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    )

    print("Creating Source Tables...")

    # SOURCE 1: REQUESTS (The Context)
    t_env.execute_sql("""
        CREATE TABLE dsp_requests (
            request_id STRING,
            timestamp_ts BIGINT,
            user_id STRING,
            ip_address STRING,
            os STRING,
            site_domain STRING,
            site_category STRING,
            publisher_id STRING,
            -- Define Event Time for Flink Watermarks
            ts AS TO_TIMESTAMP_LTZ(timestamp_ts, 3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_requests',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_dsp_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # SOURCE 2: WINS (The Action)
    t_env.execute_sql("""
        CREATE TABLE dsp_wins (
            request_id STRING,
            auction_id STRING,
            timestamp_ts BIGINT,
            campaign_id STRING,
            creative_id STRING,
            bid_price DOUBLE,
            win_price DOUBLE,
            ts AS TO_TIMESTAMP_LTZ(timestamp_ts, 3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_wins',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_dsp_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # SOURCE 3: CLICKS (The Label)
    t_env.execute_sql("""
        CREATE TABLE dsp_clicks (
            request_id STRING,
            timestamp_ts BIGINT,
            user_action STRING,
            ts AS TO_TIMESTAMP_LTZ(timestamp_ts, 3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_clicks',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_dsp_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # SINK: ENRICHED DATA (The ML Dataset)
    t_env.execute_sql("""
        CREATE TABLE dsp_enriched_impressions (
            request_id STRING,
            user_id STRING,
            os STRING,
            site_category STRING,
            campaign_id STRING,
            win_price DOUBLE,
            is_clicked INT,
            event_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_enriched_impressions',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json'
        )
    """)

    print("Executing Join Logic...")

    # THE LOGIC:
    # 1. Join Wins to Requests (Inner Join - we only care if we won)
    #    Constraint: Win must happen within 1 minute of Request
    # 2. Left Join Clicks (We want the row even if they didn't click)
    #    Constraint: Click must happen within 2 minutes of Win
    
    t_env.execute_sql("""
        INSERT INTO dsp_enriched_impressions
        SELECT 
            w.request_id,
            r.user_id,
            r.os,
            r.site_category,
            w.campaign_id,
            w.win_price,
            CASE WHEN c.user_action IS NOT NULL THEN 1 ELSE 0 END as is_clicked,
            w.ts as event_time
        FROM dsp_wins w
        JOIN dsp_requests r 
            ON w.request_id = r.request_id 
            AND w.ts BETWEEN r.ts AND r.ts + INTERVAL '1' MINUTE
        LEFT JOIN dsp_clicks c 
            ON w.request_id = c.request_id 
            AND c.ts BETWEEN w.ts AND w.ts + INTERVAL '2' MINUTE
    """)

if __name__ == '__main__':
    main()

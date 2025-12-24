from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    
    # 1. Read Enriched Impressions (History)
    t_env.execute_sql("""
        CREATE TABLE dsp_enriched_impressions (
            user_id STRING,
            site_category STRING,
            is_clicked INT,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_enriched_impressions',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'profile_builder',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 2. Define Output: Upsert Kafka (Compacted Topic)
    # This topic will hold the "latest" state for every user
    t_env.execute_sql("""
        CREATE TABLE user_profiles (
            user_id STRING,
            total_views BIGINT,
            tech_views BIGINT,
            tech_clicks BIGINT,
            lifestyle_views BIGINT,
            lifestyle_clicks BIGINT,
            PRIMARY KEY (user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'user_profiles',
            'properties.bootstrap.servers' = 'kafka:9092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # 3. Calculate Aggregates
    print("Building User Profiles...")
    t_env.execute_sql("""
        INSERT INTO user_profiles
        SELECT 
            user_id,
            COUNT(*) as total_views,
            SUM(CASE WHEN site_category = 'Tech' THEN 1 ELSE 0 END) as tech_views,
            SUM(CASE WHEN site_category = 'Tech' THEN is_clicked ELSE 0 END) as tech_clicks,
            SUM(CASE WHEN site_category = 'Lifestyle' THEN 1 ELSE 0 END) as lifestyle_views,
            SUM(CASE WHEN site_category = 'Lifestyle' THEN is_clicked ELSE 0 END) as lifestyle_clicks
        FROM dsp_enriched_impressions
        GROUP BY user_id
    """)

if __name__ == '__main__':
    main()

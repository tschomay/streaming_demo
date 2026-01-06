import pickle
import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf

class PredictCTR(ScalarFunction):
    def open(self, function_context):
        with open('/opt/flink/user_jobs/ctr_model_v2.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open('/opt/flink/user_jobs/model_columns_v2.pkl', 'rb') as f:
            self.model_columns = pickle.load(f)

    def eval(self, os, site_category, total_views, tech_views, lifestyle_views):
        try:
            if total_views is None or total_views == 0:
                frac_tech = 0.0
                frac_lifestyle = 0.0
            else:
                frac_tech = float(tech_views) / float(total_views)
                frac_lifestyle = float(lifestyle_views) / float(total_views)

            features = {col: 0 for col in self.model_columns}
            
            os_col = f"os_{os}"
            cat_col = f"site_category_{site_category}"
            if os_col in features: features[os_col] = 1
            if cat_col in features: features[cat_col] = 1
            
            if 'frac_tech' in features: features['frac_tech'] = frac_tech
            if 'frac_lifestyle' in features: features['frac_lifestyle'] = frac_lifestyle

            df_features = pd.DataFrame([features], columns=self.model_columns)
            prob = self.model.predict_proba(df_features)[0][1]
            return float(prob)
            
        except Exception as e:
            return 0.0

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    predict_ctr = udf(PredictCTR(), result_type=DataTypes.DOUBLE())
    t_env.create_temporary_system_function("predict_ctr", predict_ctr)

    # 1. SOURCE: Requests (Now defined with EVENT TIME 'ts')
    t_env.execute_sql("""
        CREATE TABLE dsp_requests (
            request_id STRING,
            timestamp_ts BIGINT,
            user_id STRING,
            os STRING,
            site_category STRING,
            ts AS CAST(TO_TIMESTAMP_LTZ(timestamp_ts, 3) AS TIMESTAMP(3)),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_requests',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'inference_group_v3',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 2. LOOKUP SOURCE: User Profiles (Now defined as VERSIONED TABLE via Watermark)
    t_env.execute_sql("""
        CREATE TABLE user_profiles (
            user_id STRING,
            total_views BIGINT,
            tech_views BIGINT,
            lifestyle_views BIGINT,
            update_time TIMESTAMP(3),
            PRIMARY KEY (user_id) NOT ENFORCED,
            WATERMARK FOR update_time AS update_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'user_profiles',
            'properties.bootstrap.servers' = 'kafka:9092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    # 3. SINK: Scored Requests
    t_env.execute_sql("""
        CREATE TABLE dsp_scored_requests (
            request_id STRING,
            ctr_score DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_scored_requests',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json'
        )
    """)

    # 4. EXECUTE: Temporal Join using EVENT TIME (r.ts)
    t_env.execute_sql("""
        INSERT INTO dsp_scored_requests
        SELECT 
            r.request_id,
            predict_ctr(
                r.os, 
                r.site_category, 
                u.total_views, 
                u.tech_views, 
                u.lifestyle_views
            ) as ctr_score
        FROM dsp_requests AS r
        LEFT JOIN user_profiles FOR SYSTEM_TIME AS OF r.ts AS u
        ON r.user_id = u.user_id
    """)

if __name__ == '__main__':
    main()

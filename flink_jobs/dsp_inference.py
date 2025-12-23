import pickle
import json
import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf

# --- 1. DEFINE THE INFERENCE LOGIC (UDF) ---
class PredictCTR(ScalarFunction):
    def open(self, function_context):
        # Load model & columns ONCE when the worker starts
        print("Loading Model...")
        with open('/opt/flink/user_jobs/ctr_model.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open('/opt/flink/user_jobs/model_columns.pkl', 'rb') as f:
            self.model_columns = pickle.load(f)
        print("Model Loaded!")

    def eval(self, os, site_category):
        try:
            # 1. Create a dictionary for the current row
            # We match the raw input string to the features expected by the model.
            # Example: If input site_category is "Tech", we want "site_category_Tech": 1
            
            # Initialize all expected columns to 0
            features = {col: 0 for col in self.model_columns}
            
            # Set the matching categorical flags to 1
            # Note: Pandas get_dummies format is usually "ColumnName_Value"
            os_col = f"os_{os}"
            cat_col = f"site_category_{site_category}"
            
            if os_col in features:
                features[os_col] = 1
            if cat_col in features:
                features[cat_col] = 1
                
            # 2. Convert to DataFrame (1 row) ensuring correct column order
            df_features = pd.DataFrame([features], columns=self.model_columns)
            
            # 3. Predict Probability (Class 1 = Click)
            prob = self.model.predict_proba(df_features)[0][1]
            return float(prob)
            
        except Exception as e:
            print(f"Inference Error: {e}")
            return 0.0

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Add Kafka Connector
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", 
        "file:///opt/flink/user_lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    )

    # Register the UDF
    predict_ctr = udf(PredictCTR(), result_type=DataTypes.DOUBLE())
    t_env.create_temporary_system_function("predict_ctr", predict_ctr)

    # SOURCE: Raw Requests
    t_env.execute_sql("""
        CREATE TABLE dsp_requests (
            request_id STRING,
            os STRING,
            site_category STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_requests',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'inference_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # SINK: Scored Requests
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

    print("Starting Inference Stream...")
    
    # EXECUTE: Read -> Score -> Write
    t_env.execute_sql("""
        INSERT INTO dsp_scored_requests
        SELECT 
            request_id,
            predict_ctr(os, site_category) as ctr_score
        FROM dsp_requests
    """)

if __name__ == '__main__':
    main()

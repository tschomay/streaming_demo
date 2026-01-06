import os
import pickle
import time
import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import ScalarFunction, udf
from sklearn.linear_model import SGDClassifier

# We use a global model variable to persist state in the UDF
# In a real production cluster, this would be managed via Flink State or a Feature Store
learner_model = None
learner_columns = None

class OnlineTrainer(ScalarFunction):
    def open(self, function_context):
        global learner_model, learner_columns
        model_path = '/opt/flink/user_jobs/ctr_model_online.pkl'
        cols_path = '/opt/flink/user_jobs/model_columns_v2.pkl'
        
        # 1. Load Feature Columns (from the V2 Batch training)
        if os.path.exists(cols_path):
            with open(cols_path, 'rb') as f:
                learner_columns = pickle.load(f)
        else:
            print("ERROR: Feature columns not found! Train V2 model first.")

        # 2. Load or Initialize Model
        if os.path.exists(model_path):
            try:
                with open(model_path, 'rb') as f:
                    learner_model = pickle.load(f)
                print("Loaded existing online model.")
            except:
                print("Model corrupted, starting fresh.")
                learner_model = None

        if learner_model is None:
            # SGDClassifier with 'log_loss' is essentially Online Logistic Regression
            print("Initializing NEW Online Model...")
            learner_model = SGDClassifier(loss='log_loss', penalty='l2', alpha=0.0001, random_state=42)

    def eval(self, os_val, site_category, total_views, tech_views, lifestyle_views, is_clicked):
        global learner_model, learner_columns
        
        try:
            # --- Feature Engineering (Same as Inference) ---
            if total_views is None or total_views == 0:
                frac_tech = 0.0
                frac_lifestyle = 0.0
            else:
                frac_tech = float(tech_views) / float(total_views)
                frac_lifestyle = float(lifestyle_views) / float(total_views)

            features = {col: 0 for col in learner_columns}
            
            os_col = f"os_{os_val}"
            cat_col = f"site_category_{site_category}"
            if os_col in features: features[os_col] = 1
            if cat_col in features: features[cat_col] = 1
            
            if 'frac_tech' in features: features['frac_tech'] = frac_tech
            if 'frac_lifestyle' in features: features['frac_lifestyle'] = frac_lifestyle

            # Prepare Data
            df_features = pd.DataFrame([features], columns=learner_columns)
            y = [int(is_clicked)]

            # --- THE ONLINE LEARNING STEP ---
            # classes=[0, 1] is required for the first partial_fit call so it knows the labels
            learner_model.partial_fit(df_features, y, classes=[0, 1])
            
            # --- Save Updated Model ---
            # We save to a temp file and move it to ensure atomic writes
            # (In production, you'd save every N events, not every single one)
            temp_path = '/opt/flink/user_jobs/ctr_model_online.pkl.tmp'
            final_path = '/opt/flink/user_jobs/ctr_model_online.pkl'
            
            with open(temp_path, 'wb') as f:
                pickle.dump(learner_model, f)
            os.replace(temp_path, final_path)
            
            return "Updated"
            
        except Exception as e:
            print(f"Training Error: {e}")
            return "Error"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Register UDF
    train_step = udf(OnlineTrainer(), result_type=DataTypes.STRING())
    t_env.create_temporary_system_function("train_step", train_step)

    # 1. READ: Enriched Impressions (The Truth)
    t_env.execute_sql("""
        CREATE TABLE dsp_enriched_impressions (
            user_id STRING,
            os STRING,
            site_category STRING,
            is_clicked INT,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dsp_enriched_impressions',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'online_learner_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)
    
    # 2. READ: User Profiles (For Features)
    # We need to join profile features again because the impression stream 
    # might not have the latest profile stats attached directly.
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

    # 3. SINK: Dummy Sink (We just need to trigger the UDF side-effect)
    t_env.execute_sql("""
        CREATE TABLE model_updates_log (
            status STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 4. EXECUTE: Join & Train
    t_env.execute_sql("""
        INSERT INTO model_updates_log
        SELECT 
            train_step(
                e.os, 
                e.site_category, 
                u.total_views, 
                u.tech_views, 
                u.lifestyle_views,
                e.is_clicked
            )
        FROM dsp_enriched_impressions AS e
        LEFT JOIN user_profiles FOR SYSTEM_TIME AS OF e.event_time AS u
        ON e.user_id = u.user_id
    """)

if __name__ == '__main__':
    main()

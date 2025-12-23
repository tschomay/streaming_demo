import pandas as pd
from pinotdb import connect
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
import pickle

# 1. Connect to Pinot (Broker handles queries)
# Note: internal docker hostname for broker is 'pinot-broker'
conn = connect(host='pinot-broker', port=8099, path='/query/sql', scheme='http')

print("Fetching data from Pinot...")
# We fetch enough rows to get a signal. 
# In production, you'd paginate or use the Spark connector.
query = """
    SELECT 
        os, 
        site_category, 
        win_price, 
        is_clicked 
    FROM dsp_impressions 
    LIMIT 10000
"""
df = pd.read_sql(query, conn)

print(f"Data fetched! Shape: {df.shape}")
print(f"Click Rate in dataset: {df['is_clicked'].mean():.2%}")

if df.empty or df['is_clicked'].sum() == 0:
    print("Not enough clicks to train! Run the simulator longer.")
    exit()

# 2. Feature Engineering (One-Hot Encoding)
# We convert categorical strings (OS, Category) into numbers (0/1)
X = pd.get_dummies(df[['os', 'site_category']], drop_first=True)
y = df['is_clicked']

# 3. Train/Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# 4. Train Model (Logistic Regression)
# Class_weight='balanced' helps because clicks are rare (imbalanced classes)
model = LogisticRegression(class_weight='balanced', max_iter=1000)
model.fit(X_train, y_train)

# 5. Evaluate
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

print("\n--- Model Performance ---")
print(f"ROC AUC Score: {roc_auc_score(y_test, y_prob):.3f}")
print(classification_report(y_test, y_pred))

# 6. The "Aha!" Moment: Interpretability
# Let's see what the model learned. Did it find the hidden patterns?
print("\n--- What drives a click? (Feature Importance) ---")
coefs = pd.DataFrame({
    'Feature': X.columns,
    'Coefficient': model.coef_[0]
}).sort_values(by='Coefficient', ascending=False)
print(coefs)

# 7. Save the model for Phase 2 (Real-time Inference)
with open('/opt/flink/user_jobs/ctr_model.pkl', 'wb') as f:
    pickle.dump(model, f)
    print("\nModel saved to ctr_model.pkl")
    
# Save the columns too, so we know how to encode live data
with open('/opt/flink/user_jobs/model_columns.pkl', 'wb') as f:
    pickle.dump(X.columns.tolist(), f)

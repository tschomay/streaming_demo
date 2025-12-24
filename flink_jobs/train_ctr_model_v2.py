import pandas as pd
from pinotdb import connect
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report
import pickle

# 1. Connect to Pinot
conn = connect(host='pinot-broker', port=8099, path='/query/sql', scheme='http')

print("Fetching training data...")
# We fetch raw impressions to reconstruct history
query = """
    SELECT user_id, os, site_category, is_clicked, event_time
    FROM dsp_impressions
    LIMIT 50000
"""
df = pd.read_sql(query, conn)

if df.empty:
    print("No data! Run simulator longer.")
    exit()

# 2. Simulate "User History" Features
# In production, Flink does this in real-time. Here, we calculate it using pandas.
print("Engineering User History features...")

# Sort by time so we don't leak future info (simplified for demo)
df = df.sort_values('event_time')

# Calculate running totals per user
df['user_total_views'] = df.groupby('user_id').cumcount()
df['user_tech_views'] = df.groupby('user_id')['site_category'].transform(lambda x: (x == 'Tech').shift().cumsum()).fillna(0)
df['user_lifestyle_views'] = df.groupby('user_id')['site_category'].transform(lambda x: (x == 'Lifestyle').shift().cumsum()).fillna(0)

# 3. Create Fractional Features (The "Behavior" signals)
# Avoid division by zero
df['frac_tech'] = df['user_tech_views'] / df['user_total_views'].replace(0, 1)
df['frac_lifestyle'] = df['user_lifestyle_views'] / df['user_total_views'].replace(0, 1)

# 4. Prepare Training Data
features = ['os', 'site_category', 'frac_tech', 'frac_lifestyle']
X = pd.get_dummies(df[features], columns=['os', 'site_category'], drop_first=True)
y = df['is_clicked']

# 5. Train
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
model = LogisticRegression(class_weight='balanced', max_iter=1000)
model.fit(X_train, y_train)

# 6. Evaluate
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

print("\n--- Model Performance ---")
print(f"ROC AUC Score: {roc_auc_score(y_test, y_prob):.3f}")
print(classification_report(y_test, y_pred))

# 7. The "Aha!" Moment: Interpretability
# Let's see what the model learned. Did it find the hidden patterns?
print("\n--- What drives a click? (Feature Importance) ---")
coefs = pd.DataFrame({
    'Feature': X.columns,
    'Coefficient': model.coef_[0]
}).sort_values(by='Coefficient', ascending=False)
print(coefs)

# 8. Save
with open('/opt/flink/user_jobs/ctr_model_v2.pkl', 'wb') as f:
    pickle.dump(model, f)
with open('/opt/flink/user_jobs/model_columns_v2.pkl', 'wb') as f:
    pickle.dump(X.columns.tolist(), f)
    
print("Model V2 Saved!")

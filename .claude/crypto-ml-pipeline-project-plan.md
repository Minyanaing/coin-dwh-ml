# Snowpark ML — Model Registry + dbt Python Scoring

**Stack:** Snowpark ML · XGBoost · Snowflake Model Registry · dbt Python models · GitHub Actions

---

## How it fits into the existing pipeline

```
Existing pipeline              New ML layer
──────────────────────         ────────────────────────────────────
ingest_coingecko.py            1_train_and_register.py
  └── BRONZE.COINGECKO_RAW       └── trains on GOLD.MART_DAILY_RETURNS
        └── dbt SQL models              └── registers to ML_MODELS registry
              └── GOLD.MART_*                 └── mart_price_predictions.py (dbt Python)
                                                    └── GOLD.MART_PRICE_PREDICTIONS
```

---

## Files added

```
crypto-pipeline/
├── ml/
│   ├── 1_train_and_register.py       # run once to bootstrap the model
│   └── retrain_task.sql              # Snowflake Task: weekly auto-retrain
├── dbt/
│   └── models/
│       └── gold/
│           ├── mart_price_predictions.py    # dbt Python model (scoring)
│           └── mart_price_predictions.yml   # schema + tests
└── .github/
    └── workflows/
        └── deploy.yml                # updated: adds score job after transform
```

---

## Step 1 — Snowflake setup for ML

Run this once before the training script.

```sql
-- Create registry schema
CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.ML_MODELS;

-- Grant the pipeline role access
GRANT ALL ON SCHEMA CRYPTO_DB.ML_MODELS TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN SCHEMA CRYPTO_DB.ML_MODELS TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA CRYPTO_DB.ML_MODELS TO ROLE CRYPTO_PIPELINE_ROLE;

-- Allow stored procedures to be created
GRANT CREATE PROCEDURE ON SCHEMA CRYPTO_DB.ML_MODELS TO ROLE CRYPTO_PIPELINE_ROLE;
```

---

## Step 2 — Install dependencies locally

```bash
pip install snowflake-ml-python==1.5.0 dbt-snowflake==1.7.0
```

Also add to `dbt/packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]

# Tell dbt this Python model needs Snowpark ML available in Snowflake
# (no local install needed for dbt Python models — runs on Snowflake)
```

And add to `dbt_project.yml` under the gold model config:

```yaml
models:
  crypto_pipeline:
    gold:
      +schema: GOLD
      +materialized: table
      mart_price_predictions:
        +materialized: incremental
        +packages:
          - snowflake-ml-python
```

---

## Step 3 — Train and register (run once)

```bash
# Set env vars first
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=pipeline_svc
export SNOWFLAKE_PASSWORD=yourpassword

python ml/1_train_and_register.py
```

Expected output:
```
Session created: CRYPTO_DB
Train rows: 3640 | Test rows: 910
Model trained.
Evaluation metrics: {'accuracy': 0.5831, 'precision': 0.5714, 'recall': 0.5923}
Model registered: CRYPTO_DIRECTION_CLASSIFIER / V1
Done. You can now call this model from the dbt Python model.
```

> Note on accuracy: ~58% is realistic and actually useful for directional trading
> signals. The goal is consistent edge, not perfection.

---

## Step 4 — Run the dbt Python scoring model

```bash
cd dbt

# Score predictions for all dates not yet in MART_PRICE_PREDICTIONS
dbt run --select mart_price_predictions

# Test the output
dbt test --select mart_price_predictions
```

The model:
1. Reads `GOLD.MART_DAILY_RETURNS` (only new rows on incremental runs)
2. Engineers the same features used at training time
3. Loads `CRYPTO_DIRECTION_CLASSIFIER/V1` from the registry
4. Calls `model_version.run()` to score
5. Writes `COIN_ID`, `PRICE_DATE`, `PREDICTED_DIRECTION`, `CONFIDENCE_SCORE`,
   `SCORED_AT`, `MODEL_VERSION` back to `GOLD.MART_PRICE_PREDICTIONS`

---

## Step 5 — Set up weekly retraining via Snowflake Task

```bash
# Run this SQL in Snowflake once to set up the stored proc + task
snowsql -f snowflake/retrain_task.sql
```

Or paste it directly into a Snowflake worksheet. After that:
- Every Sunday at 02:00 UTC, `TASK_RETRAIN_MODEL` calls the stored procedure
- The proc trains a fresh model on all available Gold data
- It registers it as `V_YYYYMMDD` (e.g. `V_20250713`)
- It sets the `production` alias on the new version
- The dbt model can be updated to use `.version("production")` instead
  of a hardcoded `"V1"` to always pick up the latest

---

## Step 6 — Query predictions

```sql
-- Latest predictions for all coins
SELECT
    coin_id,
    symbol,
    price_date,
    close_price,
    predicted_direction_label,
    ROUND(confidence_score * 100, 1) AS confidence_pct,
    model_version
FROM CRYPTO_DB.GOLD.MART_PRICE_PREDICTIONS
WHERE price_date = (SELECT MAX(price_date) FROM CRYPTO_DB.GOLD.MART_PRICE_PREDICTIONS)
ORDER BY confidence_score DESC;

-- High-confidence UP signals only
SELECT *
FROM CRYPTO_DB.GOLD.MART_PRICE_PREDICTIONS
WHERE predicted_direction_label = 'UP'
  AND confidence_score > 0.65
ORDER BY price_date DESC, confidence_score DESC;

-- Model accuracy audit: predicted vs actual
SELECT
    p.coin_id,
    p.price_date,
    p.predicted_direction_label,
    CASE WHEN a.daily_return_pct > 0 THEN 'UP' ELSE 'DOWN' END AS actual_direction,
    CASE WHEN p.predicted_direction = (CASE WHEN a.daily_return_pct > 0 THEN 1 ELSE 0 END)
         THEN 'CORRECT' ELSE 'WRONG' END AS result
FROM CRYPTO_DB.GOLD.MART_PRICE_PREDICTIONS p
JOIN CRYPTO_DB.GOLD.MART_DAILY_RETURNS a
  ON p.coin_id = p.coin_id AND p.price_date = a.price_date
ORDER BY p.price_date DESC;
```

---

## Full execution order (daily)

```
06:00 UTC  ingest job         → 50 rows → BRONZE.COINGECKO_RAW
           (needs: ingest)
           transform job      → dbt SQL builds Silver + Gold SQL marts
           (needs: transform)
           score job          → dbt Python model scores predictions
                             → GOLD.MART_PRICE_PREDICTIONS updated

02:00 UTC  (every Sunday)
           TASK_RETRAIN_MODEL → new model version registered in ML_MODELS
```

---

## Model Registry — useful commands

```python
from snowflake.ml.registry import Registry

registry = Registry(session=session, database_name="CRYPTO_DB", schema_name="ML_MODELS")

# List all versions
model = registry.get_model("CRYPTO_DIRECTION_CLASSIFIER")
for v in model.versions():
    print(v.version_name, v.get_metrics())

# Compare versions
v1_metrics = model.version("V1").get_metrics()
v2_metrics = model.version("V_20250713").get_metrics()
print("V1 accuracy:", v1_metrics["accuracy"])
print("Latest accuracy:", v2_metrics["accuracy"])

# Promote a version to production alias
model.version("V_20250713").set_alias("production")

# Delete old versions
model.version("V1").drop()
```

---

## Key design decisions

**dbt Python model over a standalone script.** Putting scoring inside a dbt model means it inherits dbt's dependency graph — it automatically runs after `mart_daily_returns` is ready, shows up in `dbt docs`, is covered by `dbt test`, and fits the same CI/CD workflow.

**Incremental scoring.** The `is_incremental` guard means each daily run only scores new rows rather than re-scoring all history. This keeps the Gold table cheap and fast to query.

**Separate train and score jobs in CI.** Training is expensive and slow — it runs weekly via Snowflake Task, not on every deploy. The daily GitHub Actions workflow only runs scoring (`dbt run --select mart_price_predictions`), which is fast.

**Model alias for zero-downtime upgrades.** The `production` alias on the registry means you can retrain, validate offline, then `set_alias("production")` to cut over — the dbt model picks it up on the next run without any code changes.

**Feature parity between train and score.** The same feature engineering block (lag windows, volume ratio) appears in both `1_train_and_register.py` and `mart_price_predictions.py`. In production, extract this into a shared dbt macro or a Snowpark utility function to avoid drift.

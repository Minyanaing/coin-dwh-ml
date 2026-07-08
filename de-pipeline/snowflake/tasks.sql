-- snowflake/tasks.sql

-- Fires when the stream has unconsumed rows (polls hourly).
-- This is a lightweight signal only — actual Silver/Gold builds
-- run via dbt in GitHub Actions (de-deploy.yml), scoped per
-- environment (dev_db / qa_db / prod_db) by --target.
CREATE OR REPLACE TASK CRYPTO_DB.STG.TASK_NOTIFY_NEW_DATA
  WAREHOUSE = CRYPTO_WH
  SCHEDULE  = '60 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('CRYPTO_DB.STG.STREAM_COINGECKO_RAW')
AS
  SELECT CURRENT_TIMESTAMP();

ALTER TASK CRYPTO_DB.STG.TASK_NOTIFY_NEW_DATA RESUME;

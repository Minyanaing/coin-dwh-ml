-- snowflake/streams.sql
-- IF NOT EXISTS (not OR REPLACE) — this script re-runs on every infra
-- deploy, and CREATE OR REPLACE STREAM would reset the tracked offset,
-- silently re-surfacing already-consumed rows as "new" every deploy.

-- Captures every new INSERT on the raw landing table
CREATE STREAM IF NOT EXISTS CRYPTO_DB.STG.STREAM_COINGECKO_RAW
  ON TABLE CRYPTO_DB.STG.COINGECKO_RAW
  APPEND_ONLY = TRUE;

-- snowflake/setup.sql
-- Run once with a role that has both SYSADMIN and USERADMIN granted
-- (see README.md "Create a developer service account" for setup).
-- The script switches roles itself: SYSADMIN owns warehouses/databases/
-- schemas/tables, USERADMIN owns role creation — SYSADMIN alone cannot
-- run CREATE ROLE.

USE ROLE SYSADMIN;

-- Warehouse (shared across all environments)
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- ============================================================
-- CRYPTO_DB — landing zone, shared across all environments
-- ============================================================
CREATE DATABASE IF NOT EXISTS CRYPTO_DB;
CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.STG;   -- raw ingestion (append-only)

CREATE TABLE IF NOT EXISTS CRYPTO_DB.STG.COINGECKO_RAW (
  id                       VARCHAR,
  symbol                   VARCHAR,
  name                     VARCHAR,
  current_price            FLOAT,
  market_cap               FLOAT,
  total_volume             FLOAT,
  price_change_24h         FLOAT,
  price_change_pct_24h     FLOAT,
  high_24h                 FLOAT,
  low_24h                  FLOAT,
  circulating_supply       FLOAT,
  ath                      FLOAT,
  fetched_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Historical daily backfill (append-only). Populated on-demand by the
-- manual backfill_coingecko_history.py script / de-ingest-history.yml
-- workflow — one row per coin per day from HISTORY_START_DATE to run date.
CREATE TABLE IF NOT EXISTS CRYPTO_DB.STG.COINGECKO_HISTORY_RAW (
  id                       VARCHAR,
  symbol                   VARCHAR,
  name                     VARCHAR,
  price_date               DATE,
  price                    FLOAT,
  market_cap               FLOAT,
  total_volume             FLOAT,
  fetched_at               TIMESTAMP_NTZ,
  _loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DEV_DB — dbt development target
-- ============================================================
CREATE DATABASE IF NOT EXISTS DEV_DB;
CREATE SCHEMA IF NOT EXISTS DEV_DB.SILVER_CRYPTO;
CREATE SCHEMA IF NOT EXISTS DEV_DB.GOLD_CRYPTO;

-- ============================================================
-- QA_DB — dbt QA target, validated before prod promotion
-- ============================================================
CREATE DATABASE IF NOT EXISTS QA_DB;
CREATE SCHEMA IF NOT EXISTS QA_DB.SILVER_CRYPTO;
CREATE SCHEMA IF NOT EXISTS QA_DB.GOLD_CRYPTO;

-- ============================================================
-- PROD_DB — dbt production target
-- ============================================================
CREATE DATABASE IF NOT EXISTS PROD_DB;
CREATE SCHEMA IF NOT EXISTS PROD_DB.SILVER_CRYPTO;
CREATE SCHEMA IF NOT EXISTS PROD_DB.GOLD_CRYPTO;

-- ============================================================
-- Service account role for ingestion + dbt — shared across all databases
-- ============================================================
USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS CRYPTO_PIPELINE_ROLE;

USE ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;

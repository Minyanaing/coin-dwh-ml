-- snowflake/dbt_project_task.sql
-- DISABLED — "dbt Projects on Snowflake" (EXECUTE DBT PROJECT) run by a Task.
-- This is Step 5a: the target-state orchestration where Snowflake fetches this
-- repo's de-pipeline/dbt, registers it as a DBT PROJECT, and runs it on a Task —
-- no external runner, no credentials leaving Snowflake.
--
-- NOT applied by infra-deploy.yml, and skipped by infra-ci's sqlfluff check
-- (via .sqlfluffignore) — because it needs:
--   * a BILLED (non-trial) account with the dbt-Projects feature available, and
--   * an EXTERNAL ACCESS INTEGRATION (+ account-level CREATE INTEGRATION,
--     ACCOUNTADMIN only) so Snowflake can fetch the Git repo and `dbt deps` can
--     reach the package hub.
-- On a trial account, orchestration runs on GitHub Actions instead (Step 5b,
-- .github/workflows/dbt-run.yml). This file is reference SQL only — it runs
-- nowhere automatically.
--
-- The CREATE DBT PROJECT / EXECUTE DBT PROJECT DDL is a newer Snowflake feature;
-- confirm the exact syntax against your account's release before enabling.
--
-- To enable on a billed account:
--   1. Fill in the <owner> / <github_username> / <PAT> placeholders below.
--   2. Run the ACCOUNTADMIN block once AS ACCOUNTADMIN (secret, git API +
--      external access integrations, grants); the SYSADMIN block creates the
--      git repo, the refresh procedure SP_RUN_DBT_PROJECT (fetch + recreate the
--      DBT PROJECT — called from CI on merge), and the Task (which executes the
--      project on a schedule). Split on purpose: recreate on code change, run on
--      schedule — see (f)/(g).
--   3. Wire it into deploy (e.g. add the SYSADMIN block to run_sql.py's file
--      list in infra-deploy.yml) and drop it from .sqlfluffignore once your
--      Snowflake / sqlfluff version parses the DDL.
--   4. Decide whether to retire the Step 5b workflow (dbt-run.yml) so dbt isn't
--      run from two places.

-- ============================================================================
-- One-time, as ACCOUNTADMIN
-- ============================================================================

-- (a) GitHub credential for the Git repository integration (private repo).
--     For a PUBLIC repo you can skip the secret and GIT_CREDENTIALS.
CREATE OR REPLACE SECRET CRYPTO_DB.STG.GITHUB_PAT
  TYPE = PASSWORD
  USERNAME = '<github_username>'
  PASSWORD = '<github_fine_grained_PAT_with_repo_contents_read>';

-- (b) API integration that allows Git over HTTPS to this GitHub org/user.
CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<owner>')
  ALLOWED_AUTHENTICATION_SECRETS = (CRYPTO_DB.STG.GITHUB_PAT)
  ENABLED = TRUE;

-- (c) External Access Integration so `dbt deps` can fetch packages
--     (dbt_utils) from the dbt hub / GitHub codeload.
CREATE OR REPLACE NETWORK RULE CRYPTO_DB.STG.DBT_DEPS_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com', 'github.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBT_DEPS_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (CRYPTO_DB.STG.DBT_DEPS_NETWORK_RULE)
  ENABLED = TRUE;

-- (d) Let SYSADMIN use the integrations + secret it will reference below.
GRANT USAGE ON INTEGRATION GITHUB_API_INTEGRATION        TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION DBT_DEPS_ACCESS_INTEGRATION   TO ROLE SYSADMIN;
GRANT USAGE ON SECRET CRYPTO_DB.STG.GITHUB_PAT           TO ROLE SYSADMIN;

-- ============================================================================
-- As SYSADMIN — git repo, the run procedure, and the scheduling task
-- ============================================================================

USE ROLE SYSADMIN;

-- (e) Snowflake Git repository object pointing at this GitHub repo (one-time).
CREATE OR REPLACE GIT REPOSITORY CRYPTO_DB.STG.CRYPTO_REPO
  API_INTEGRATION = GITHUB_API_INTEGRATION
  GIT_CREDENTIALS = CRYPTO_DB.STG.GITHUB_PAT
  ORIGIN = 'https://github.com/<owner>/coin-dwh-ml.git';

-- (f) SP_RUN_DBT_PROJECT — RECREATE only, no execute. It FETCHes the latest
--     commits and re-creates the DBT PROJECT object so it mirrors `main`. This
--     is called from CI (GitHub Actions) when repo code changes — the project
--     object is only refreshed when there's actually new code, not on every
--     scheduled build. EXECUTE AS CALLER: runs with the caller's role (SYSADMIN,
--     which owns DEV_DB / CRYPTO_DB.STG).
--     NOTE: call this once after first setup so CRYPTO_PIPELINE exists before the
--     Task below runs.
CREATE OR REPLACE PROCEDURE CRYPTO_DB.STG.SP_RUN_DBT_PROJECT()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
BEGIN
  -- 1. Pull the latest commits from GitHub into the git repository stage.
  ALTER GIT REPOSITORY CRYPTO_DB.STG.CRYPTO_REPO FETCH;

  -- 2. Recreate the dbt project from the just-fetched files so it mirrors main.
  CREATE OR REPLACE DBT PROJECT CRYPTO_DB.STG.CRYPTO_PIPELINE
    FROM '@CRYPTO_DB.STG.CRYPTO_REPO/branches/main/de-pipeline/dbt'
    EXTERNAL_ACCESS_INTEGRATIONS = (DBT_DEPS_ACCESS_INTEGRATION);

  RETURN 'dbt project refreshed from main';
END;
$$;

-- (g) Task: EXECUTE the dbt project on a schedule (deps + build into DEV_DB).
--     It does NOT recreate the project — SP_RUN_DBT_PROJECT() (called from CI on
--     merge) keeps CRYPTO_PIPELINE in sync with the repo; the Task just runs it.
CREATE OR REPLACE TASK CRYPTO_DB.STG.TASK_DBT_BUILD
  WAREHOUSE = CRYPTO_WH
  SCHEDULE  = 'USING CRON 30 1 * * * UTC'   -- 08:30 Asia/Bangkok
AS
BEGIN
  EXECUTE DBT PROJECT CRYPTO_DB.STG.CRYPTO_PIPELINE ARGS = 'deps';
  EXECUTE DBT PROJECT CRYPTO_DB.STG.CRYPTO_PIPELINE ARGS = 'build --target dev';
END;

ALTER TASK CRYPTO_DB.STG.TASK_DBT_BUILD RESUME;

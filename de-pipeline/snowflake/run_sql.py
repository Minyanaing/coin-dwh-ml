import argparse
import logging
import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger(__name__)


def load_private_key_der(pem: str, passphrase: str | None) -> bytes:
    key = serialization.load_pem_private_key(
        pem.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_DEV_SVC_USER"],
        private_key=load_private_key_der(
            os.environ["SNOWFLAKE_DEV_SVC_PRIVATE_KEY"],
            os.environ.get("SNOWFLAKE_DEV_SVC_PRIVATE_KEY_PASSPHRASE"),
        ),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH"),
    )


def run_file(cur, path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    if "${STREAMLIT_ENV}" in sql:
        env = os.environ.get("STREAMLIT_ENV")
        if not env:
            raise SystemExit(f"{path} needs STREAMLIT_ENV set (DEV/QA/PROD)")
        sql = sql.replace("${STREAMLIT_ENV}", env)

    # Drop line comments first so a ';' inside a comment can't split a statement.
    sql = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())

    for statement in sql.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        logger.info("  -> %s", statement.splitlines()[0][:80])
        cur.execute(statement)


def run_put(cur, spec: str) -> None:
    # Upload a local file to a stage: "<localpath>=@DB.SCHEMA.STAGE".
    local, sep, stage = spec.partition("=@")
    if not sep:
        raise SystemExit(f"Bad put spec '{spec}'. Expected put:<localpath>=@DB.SCHEMA.STAGE")
    stage = stage.strip()
    local_abs = Path(local.strip()).resolve()
    if not local_abs.is_file():
        raise SystemExit(f"put: local file not found: {local_abs}")
    if stage.count(".") != 2:
        raise SystemExit(f"put: stage must be DB.SCHEMA.STAGE (got '{stage}')")
    db, schema, _ = stage.split(".")

    for stmt in (
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}",
        f"CREATE STAGE IF NOT EXISTS {stage}",
    ):
        logger.info("  -> %s", stmt)
        cur.execute(stmt)
    logger.info("  -> PUT %s @%s", local_abs.name, stage)
    cur.execute(f"PUT 'file://{local_abs}' @{stage} OVERWRITE = TRUE AUTO_COMPRESS = FALSE")


def main():
    parser = argparse.ArgumentParser(
        description="Apply Snowflake steps in order. Each step is a SQL file to run, "
        "or a 'put:<localpath>=@DB.SCHEMA.STAGE' upload (used to ship the Streamlit "
        "app.py before streamlit-setup.sql's CREATE STREAMLIT)."
    )
    parser.add_argument("steps", nargs="+", help="SQL files and/or put: specs, in order")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for step in args.steps:
                if step.startswith("put:"):
                    logger.info("=== PUT %s ===", step[4:])
                    run_put(cur, step[4:])
                else:
                    logger.info("=== Running %s ===", step)
                    run_file(cur, step)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

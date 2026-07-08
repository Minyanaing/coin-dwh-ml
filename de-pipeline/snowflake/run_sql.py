import argparse
import os
import sys

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


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
        account            = os.environ["SNOWFLAKE_ACCOUNT"],
        user               = os.environ["SNOWFLAKE_DEV_SVC_USER"],
        private_key        = load_private_key_der(
            os.environ["SNOWFLAKE_DEV_SVC_PRIVATE_KEY"],
            os.environ.get("SNOWFLAKE_DEV_SVC_PRIVATE_KEY_PASSPHRASE"),
        ),
        warehouse          = os.environ.get("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH"),
    )


def run_file(cur, path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";")]
    for statement in statements:
        if not statement:
            continue
        code_lines = [
            line for line in statement.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        preview = code_lines[0][:80] if code_lines else statement.splitlines()[0][:80]
        print(f"  -> {preview}")
        cur.execute(statement)


def main():
    parser = argparse.ArgumentParser(description="Apply Snowflake infra SQL files in order")
    parser.add_argument("files", nargs="+", help="SQL files to run, in order")
    args = parser.parse_args()

    conn = get_connection()
    try:
        cur = conn.cursor()
        for path in args.files:
            print(f"=== Running {path} ===")
            run_file(cur, path)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

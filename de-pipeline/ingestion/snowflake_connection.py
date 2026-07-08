"""Shared Snowflake connection helper.

Supports key-pair auth (CI reuses the DEVELOPER_SVC key pair) or password auth,
selected automatically by what's configured. Used by both ingestion scripts.
"""

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import config


def _load_private_key_der():
    """Return the private key as DER bytes for key-pair auth, or None if no
    key is configured (then password auth is used). Accepts an inline PEM
    (SNOWFLAKE_PRIVATE_KEY) or a path to a .p8 file (SNOWFLAKE_PRIVATE_KEY_PATH)."""
    pem = config.SNOWFLAKE_PRIVATE_KEY
    if not pem and config.SNOWFLAKE_PRIVATE_KEY_PATH:
        with open(config.SNOWFLAKE_PRIVATE_KEY_PATH, "r", encoding="utf-8") as f:
            pem = f.read()
    if not pem:
        return None

    passphrase = config.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
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


def get_snowflake_conn():
    kwargs = dict(
        account   = config.SNOWFLAKE_ACCOUNT,
        user      = config.SNOWFLAKE_USER,
        role      = config.SNOWFLAKE_ROLE,
        warehouse = config.SNOWFLAKE_WAREHOUSE,
        database  = config.SNOWFLAKE_DATABASE,
        schema    = config.SNOWFLAKE_SCHEMA,
    )
    private_key_der = _load_private_key_der()
    if private_key_der is not None:
        kwargs["private_key"] = private_key_der   # key-pair auth (e.g. DEVELOPER_SVC)
    else:
        kwargs["password"] = config.SNOWFLAKE_PASSWORD   # password auth
    return snowflake.connector.connect(**kwargs)

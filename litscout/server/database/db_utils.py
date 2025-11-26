# server/database/db_utils.py

import os
import sys

import psycopg2
from psycopg2 import sql, OperationalError
from getpass import getpass
from colorama import Fore

from server.globals import ENV_DB_NAME, ENV_DB_USER, ENV_DB_PASSWORD, ENV_DB_HOST, ENV_DB_PORT
from server.logger import ColorLogger

log = ColorLogger("DB", tag_color=Fore.BLUE, include_timestamps=False, include_threading_id=False)


def _connect_with_optional_prompt(dbname: str, user: str, password: str, host: str, port: str):
    """
    Try to connect with given password.
    If password is empty or invalid, prompt once and retry.
    Returns (connection, final_password).
    """
    attempted_prompt = False
    current_password = password

    while True:
        try:
            conn = psycopg2.connect(dbname=dbname, user=user, password=current_password, host=host, port=port)
            return conn, current_password

        except OperationalError as e:
            msg = str(e)
            needs_prompt = (
                not attempted_prompt
                and (not current_password or "password authentication failed" in msg.lower())
            )

            if needs_prompt:
                log.warn(f"LITSCOUT_DB_PASSWORD is missing or invalid.")
                current_password = getpass(f"Enter password for PostgreSQL user '{user}': ")
                attempted_prompt = True
                continue

            log.error(f"Could not connect to Postgres.")
            log.error(e)
            raise e

def get_conn():
    """
    Get a connection to the target database using env vars.
    Prompts for password if needed.
    """
    conn, _ = _connect_with_optional_prompt(
        dbname=ENV_DB_NAME,
        user=ENV_DB_USER,
        password=ENV_DB_PASSWORD,
        host=ENV_DB_HOST,
        port=ENV_DB_PORT,
    )
    return conn

def schema_exists(conn) -> bool:
    """
    Returns True if there is at least one table in the public schema.
    Used to decide whether to apply schema.sql or not.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """
    )
    count = cur.fetchone()[0]
    cur.close()
    return count > 0


def ensure_database_exists(admin_conn, db_name: str, force: bool = False) -> None:
    """
    Ensure the target database exists.
    If force=True, drop it first (killing connections) and recreate.
    """
    admin_conn.autocommit = True
    cur = admin_conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    exists = cur.fetchone() is not None

    if exists and force:
        log.warn(f"Dropping existing database '{db_name}' (force)...")

        # Terminate any active connections to this DB
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid();
            """,
            (db_name,),
        )

        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
        exists = False

    if not exists:
        log.info(f"Creating database '{db_name}'...")
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    cur.close()
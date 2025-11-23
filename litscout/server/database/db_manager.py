# server/database/db_manager.py

import os
import sys
import subprocess
from pathlib import Path

import psycopg2
from colorama import Fore

from server.database.db_utils import (
    log,
    ENV_DB_NAME,
    ENV_DB_USER,
    ENV_DB_PASSWORD,
    ENV_DB_HOST,
    ENV_DB_PORT,
    _connect_with_optional_prompt,
    schema_exists,
    ensure_database_exists,
)

BASE_DIR = Path(__file__).parent
DEFAULT_PGDATA = BASE_DIR / "pgdata"
SCHEMA_PATH = BASE_DIR / "schema.sql"


#  Shell command runner
def run_cmd(command: list[str]) -> None:
    """Run a shell command with logging."""
    log.cmd(" ".join(command))
    try:
        subprocess.run(command, check=True)
    except FileNotFoundError:
        log.error(f"Command not found: {command[0]}")
        log.warn("Ensure PostgreSQL binaries (initdb, pg_ctl) are installed and in your PATH.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        log.error(f"Command failed with exit code {e.returncode}")
        sys.exit(e.returncode)


#  Postgres control functions
def start_postgres() -> None:
    """
    Start a local Postgres instance using pg_ctl.
    Uses PGDATA from LITSCOUT_PGDATA or database/pgdata by default.
    Also ensures a superuser role 'admin' with password 'admin' exists.
    """
    pgdata = Path(os.getenv("LITSCOUT_PGDATA", DEFAULT_PGDATA))
    pgdata.mkdir(parents=True, exist_ok=True)

    # Initialize a new cluster if needed
    if not (pgdata / "PG_VERSION").exists():
        log.info(f"Initializing new Postgres cluster in {pgdata} (UTF8)...")
        run_cmd([
            "initdb",
            "-D", str(pgdata),
            "-E", "UTF8",
            "--locale=C",
        ])

    port = os.getenv("LITSCOUT_DB_PORT", ENV_DB_PORT)
    log_file = BASE_DIR / "postgres.log"

    log.info(f"Starting Postgres on port {port} (PGDATA={pgdata})...")
    run_cmd(
        [
            "pg_ctl",
            "-D",
            str(pgdata),
            "-l",
            str(log_file),
            "-o",
            f"-p {port}",
            "start",
        ]
    )
    log.success("Postgres started.")

    # Ensure 'admin' role exists
    log.info("Ensuring admin role exists...")

    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=os.getlogin(),  # OS user, cluster superuser
            password="",
            host=ENV_DB_HOST,
            port=ENV_DB_PORT,
        )
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'admin';")
        exists = cur.fetchone() is not None

        if not exists:
            log.warn("Role 'admin' does not exist. Creating it now...")
            cur.execute("CREATE ROLE admin WITH LOGIN SUPERUSER PASSWORD 'admin';")
            log.success("Role 'admin' created with SUPERUSER privileges.")
        else:
            log.info("Role 'admin' already exists. Nothing to do.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        log.error("Failed to ensure admin role exists.")
        print(e)


def stop_postgres() -> None:
    """
    Stop the local Postgres instance (if it exists).
    """
    pgdata = Path(os.getenv("LITSCOUT_PGDATA", DEFAULT_PGDATA))

    if not (pgdata / "PG_VERSION").exists():
        log.warn(f"No Postgres cluster found at {pgdata}. Nothing to stop.")
        return

    log.info(f"Stopping Postgres (PGDATA={pgdata})...")
    run_cmd(["pg_ctl", "-D", str(pgdata), "stop", "-m", "fast"])
    log.success("Postgres stopped.")


# Schema application & DB creation
def apply_schema(
    db_name: str,
    user: str,
    password: str,
    host: str,
    port: str,
) -> None:
    """Read schema.sql and apply it to the given database, only if schema is empty."""
    if not SCHEMA_PATH.exists():
        log.error(f"schema.sql not found at {SCHEMA_PATH}")
        sys.exit(1)

    conn, _ = _connect_with_optional_prompt(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port,
        purpose=f"applying schema to '{db_name}'",
    )

    if schema_exists(conn):
        log.warn("Schema already exists. To force override, run with -F.")
        conn.close()
        return

    log.info("Dropping existing schema (if any)...")

    cur = conn.cursor()
    cur.execute(f"DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")

    log.info(f"Applying schema from {SCHEMA_PATH} to database '{db_name}'...")

    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    cur.execute(sql_text)

    conn.commit()
    cur.close()
    conn.close()

    log.success("Schema applied successfully.")


def init_database(
    force: bool = False,
    db_name: str | None = None,
    db_user: str | None = None,
    db_host: str | None = None,
    db_port: str | None = None,
    db_password: str | None = None,
) -> None:
    """
    Initialize the LitScout database:

    - Resolve config from args/env/defaults.
    - Connect to admin DB 'postgres' (with password prompt if needed).
    - Create/drop target DB as required.
    - Apply schema.sql to the target DB (if schema not already present).

    Priority:
        function args > environment variables > hardcoded defaults
    """
    name = db_name or ENV_DB_NAME
    user = db_user or ENV_DB_USER
    host = db_host or ENV_DB_HOST
    port = db_port or ENV_DB_PORT

    initial_password = db_password if db_password is not None else ENV_DB_PASSWORD

    log.info(
        f"Initializing database '{name}' "
        f"(user='{user}', host='{host}', port='{port}', force={force})"
    )

    # 1) Connect to admin DB 'postgres'
    log.info("Connecting to admin database 'postgres'...")
    admin_conn, final_password = _connect_with_optional_prompt(
        dbname="postgres",
        user=user,
        password=initial_password,
        host=host,
        port=port,
        purpose="admin connection to 'postgres'",
    )

    # 2) Ensure target DB exists (or drop & recreate if force=True)
    ensure_database_exists(admin_conn, name, force=force)
    admin_conn.close()

    # 3) Apply schema to target DB using same confirmed password
    apply_schema(db_name=name, user=user, password=final_password, host=host, port=port)
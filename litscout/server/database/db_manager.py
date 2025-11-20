#  litscout/server/database/db_manager.py

import os
import sys
import subprocess
from pathlib import Path
from getpass import getpass

import psycopg2
from psycopg2 import sql, OperationalError
from colorama import Fore

from litscout.server.logger import ColorLogger

log = ColorLogger("DB", tag_color=Fore.BLUE, include_timestamps=False)

BASE_DIR = Path(__file__).parent

DEFAULT_PGDATA = BASE_DIR / "pgdata"
SCHEMA_PATH = BASE_DIR / "schema.sql"

ENV_DB_NAME = os.getenv("LITSCOUT_DB_NAME", "litscout")
ENV_DB_USER = os.getenv("LITSCOUT_DB_USER", "postgres")
ENV_DB_PASSWORD = os.getenv("LITSCOUT_DB_PASSWORD", "")
ENV_DB_HOST = os.getenv("LITSCOUT_DB_HOST", "localhost")
ENV_DB_PORT = os.getenv("LITSCOUT_DB_PORT", "5432")


#   Shell command runner
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

#   Postgres control functions
def start_postgres() -> None:
    """
    Start a local Postgres instance using pg_ctl.
    Uses PGDATA from LITSCOUT_PGDATA or database/pgdata by default.
    """
    pgdata = Path(os.getenv("LITSCOUT_PGDATA", DEFAULT_PGDATA))
    pgdata.mkdir(parents=True, exist_ok=True)

    #  Initialize a new cluster if needed
    if not (pgdata / "PG_VERSION").exists():
        log.info(f"Initializing new Postgres cluster in {pgdata}...")
        run_cmd(["initdb", "-D", str(pgdata)])

    port = os.getenv("LITSCOUT_DB_PORT", ENV_DB_PORT)
    log_file = BASE_DIR / "postgres.log"

    log.info(f"Starting Postgres on port {port} (PGDATA={pgdata})...")
    run_cmd([
        "pg_ctl",
        "-D", str(pgdata),
        "-l", str(log_file),
        "-o", f"-p {port}",
        "start",
    ])
    log.success("Postgres started.")


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


#   DB Connection
def _connect_with_optional_prompt(
    dbname: str,
    user: str,
    password: str,
    host: str,
    port: str,
    purpose: str,
):
    """
    Try to connect with given password.
    If password is empty or invalid, prompt once and retry.
    Returns the connection object.
    """
    attempted_prompt = False
    current_password = password

    while True:
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=current_password,
                host=host,
                port=port,
            )
            return conn
        except OperationalError as e:
            msg = str(e)
            needs_prompt = (
                not attempted_prompt and
                (not current_password or "password authentication failed" in msg.lower())
            )

            if needs_prompt:
                log.warn(f"{purpose} password is missing or invalid.")
                current_password = getpass(f"Enter password for PostgreSQL user '{user}': ")
                attempted_prompt = True
                continue

            log.error(f"Could not connect to Postgres for {purpose}.")
            log.error(e)
            sys.exit(1)


#  Schema application & DB creation
def apply_schema(
    db_name: str,
    user: str,
    password: str,
    host: str,
    port: str,
) -> None:
    """Read schema.sql and apply it to the given database."""
    if not SCHEMA_PATH.exists():
        log.error(f"schema.sql not found at {SCHEMA_PATH}")
        sys.exit(1)

    log.info(f"Applying schema from {SCHEMA_PATH} to database '{db_name}'...")

    conn = _connect_with_optional_prompt(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port,
        purpose=f"applying schema to '{db_name}'",
    )
    conn.autocommit = True
    cur = conn.cursor()

    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    cur.execute(sql_text)

    cur.close()
    conn.close()

    log.success("Schema applied successfully.")


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

        #  Terminate any active connections to this DB
        cur.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid();
        """, (db_name,))

        cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
        exists = False

    if not exists:
        log.info(f"Creating database '{db_name}'...")
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    cur.close()


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
    - Apply schema.sql to the target DB.

    Priority:
        function args > environment variables > hardcoded defaults
    """
    #  Resolve configuration with priority: args > env > defaults
    name = db_name or ENV_DB_NAME
    user = db_user or ENV_DB_USER
    host = db_host or ENV_DB_HOST
    port = db_port or ENV_DB_PORT

    #  Password: args > env; may be empty, and we handle prompt later
    initial_password = db_password if db_password is not None else ENV_DB_PASSWORD

    log.info(
        f"Initializing database '{name}' "
        f"(user='{user}', host='{host}', port='{port}', force={force})"
    )

    #  1) Connect to admin DB 'postgres' using the user/password, with prompt if needed
    log.info("Connecting to admin database 'postgres'...")
    admin_conn, final_password = _connect_with_optional_prompt(
        dbname="postgres",
        user=user,
        password=initial_password,
        host=host,
        port=port,
        purpose="admin connection to 'postgres'",
    )

    #  2) Ensure target DB exists (or drop & recreate if force=True)
    ensure_database_exists(admin_conn, name, force=force)
    admin_conn.close()

    #  3) Apply schema to target DB using the same confirmed password
    apply_schema(db_name=name, user=user, password=final_password, host=host, port=port)
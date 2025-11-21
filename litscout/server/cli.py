# server/cli.py

import argparse
from colorama import Fore

from server.database.db_manager import (
    start_postgres,
    stop_postgres,
    init_database,
)
from server.logger import ColorLogger

cli_log = ColorLogger("", tag_color=Fore.CYAN, include_timestamps=False)


def add_db_options(parser: argparse.ArgumentParser) -> None:
    """
    Add common DB override options to a subcommand parser.
    These override environment variables if provided.
    """
    parser.add_argument(
        "--db-name",
        help="Database name (overrides LITSCOUT_DB_NAME if provided).",
    )
    parser.add_argument(
        "--db-user",
        help="Database user (overrides LITSCOUT_DB_USER if provided).",
    )
    parser.add_argument(
        "--db-host",
        help="Database host (overrides LITSCOUT_DB_HOST if provided).",
    )
    parser.add_argument(
        "--db-port",
        help="Database port (overrides LITSCOUT_DB_PORT if provided).",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="LitScout Server CLI (db, and other components in future)."
    )

    # Top-level components: db, (future: api, worker, etc.)
    subparsers = parser.add_subparsers(dest="component", required=True)

    # Database component: supports both "db" and "database"
    db_parser = subparsers.add_parser(
        "db",
        help="Database management commands.",
        aliases=["database"],
    )

    db_subparsers = db_parser.add_subparsers(
        dest="db_command",
        required=True,
    )

    # db init
    init_parser = db_subparsers.add_parser(
        "init",
        help="Initialize or reinstall the LitScout database schema from schema.sql.",
    )
    init_parser.add_argument(
        "-F",
        "--force",
        action="store_true",
        help="Force: drop and recreate the database before applying schema.",
    )
    add_db_options(init_parser)

    # db start
    db_subparsers.add_parser(
        "start",
        help="Start local PostgreSQL instance (PGDATA under server/database/pgdata by default).",
    )

    # db stop
    db_subparsers.add_parser(
        "stop",
        help="Stop local PostgreSQL instance.",
    )

    return parser


def main():
    cli_log.banner(title="LitScout", subtitle="Server CLI")

    parser = build_parser()
    args = parser.parse_args()

    # Handle DB-related commands ("db" or "database")
    if args.component in ("db", "database"):
        if args.db_command == "init":
            cli_log.info("Initializing database schema...")
            init_database(
                force=args.force,
                db_name=args.db_name,
                db_user=args.db_user,
                db_host=args.db_host,
                db_port=args.db_port,
            )
        elif args.db_command == "start":
            cli_log.info("Starting PostgreSQL...")
            start_postgres()
        elif args.db_command == "stop":
            cli_log.info("Stopping PostgreSQL...")
            stop_postgres()
        else:
            cli_log.error(f"Unknown db command: {args.db_command}")
    else:
        cli_log.error(f"Unknown component: {args.component}")


if __name__ == "__main__":
    main()
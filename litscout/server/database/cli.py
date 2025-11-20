#  litscout/server/database/cli.py

import argparse
from colorama import Fore

from .db_manager import start_postgres, stop_postgres, init_database
from litscout.server.logger import ColorLogger

cli_log = ColorLogger("CLI", tag_color=Fore.CYAN, include_timestamps=False)


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


def main():
    cli_log.banner(title="LitScout", subtitle="Database Manager")

    parser = argparse.ArgumentParser(
        description="LitScout Database Manager (start | stop | init)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
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

    subparsers.add_parser(
        "start",
        help="Start local PostgreSQL instance (PGDATA under database/pgdata by default).",
    )

    subparsers.add_parser(
        "stop",
        help="Stop local PostgreSQL instance.",
    )

    args = parser.parse_args()

    if args.command == "init":
        cli_log.info("Initializing database schema...")
        init_database(
            force=args.force,
            db_name=args.db_name,
            db_user=args.db_user,
            db_host=args.db_host,
            db_port=args.db_port,
        )
    elif args.command == "start":
        cli_log.info("Starting PostgreSQL...")
        start_postgres()
    elif args.command == "stop":
        cli_log.info("Stopping PostgreSQL...")
        stop_postgres()


if __name__ == "__main__":
    main()
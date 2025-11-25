# server/cli.py

import os
import sys
import argparse
from colorama import Fore

from server.logger import ColorLogger
from server.database.db_manager import (
    start_postgres,
    stop_postgres,
    init_database,
)
from server.ingestion.openalex.ingest import ingest_openalex_concept
from server.ingestion.openalex.fetch_concepts import ingest_openalex_from_fields
from server.ingestion.openalex.enrich import enrich_openalex
from server.embeddings.papers import embed_missing_papers


cli_log = ColorLogger("CLI", include_timestamps=False, include_threading_id=False)


def add_db_options(parser: argparse.ArgumentParser):
    """Common DB override flags used by db init/start/stop."""
    parser.add_argument("--db-name", help="Override database name.")
    parser.add_argument("--db-user", help="Override database user.")
    parser.add_argument("--db-host", help="Override database host.")
    parser.add_argument("--db-port", help="Override database port.")


def build_parser():
    """Build the full CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="LitScout Command Line Interface"
    )
    subparsers = parser.add_subparsers(dest="category", required=True)

    # DB COMMANDS
    for db_alias in ("db", "database"):
        db_parser = subparsers.add_parser(db_alias, help="Database operations")
        db_subp = db_parser.add_subparsers(dest="db_cmd", required=True)

        db_subp.add_parser("start", help="Start PostgreSQL server.")

        db_subp.add_parser("stop", help="Stop PostgreSQL server.")

        init_p = db_subp.add_parser("init", help="Initialize or reinitialize DB schema.")
        init_p.add_argument("-F", "--force", action="store_true", help="Force: drop and recreate database before applying schema.")
        add_db_options(init_p)

    # INGEST COMMANDS
    ingest_parser = subparsers.add_parser("ingest", help="Data ingestion commands.")
    ingest_subp = ingest_parser.add_subparsers(dest="ingest_cmd", required=True)

    # Single concept
    oa_parser = ingest_subp.add_parser("openalex", help="Ingest a single OpenAlex concept.")
    oa_parser.add_argument("--concept_id", required=True, help="OpenAlex concept ID, e.g., C41008148.")
    oa_parser.add_argument("--pages", type=int, default=1, help="How many pages of results to fetch (~200 papers per page).")
    oa_parser.add_argument("--verify", action="store_true", help="Re-enrich existing papers for this concept.")

    # Multi: by fields
    oa_multi_parser = ingest_subp.add_parser(
        "openalex-multi",
        help="Fetch concepts by field(s) and ingest them in parallel.",
    )
    oa_multi_parser.add_argument("--fields", nargs="+", required=True,
        help="One or more field names to search concepts for, e.g.: --fields 'computer science' 'economics'."
    )
    oa_multi_parser.add_argument("--pages", type=int, default=1, help="How many pages per concept to fetch (~200 papers per page).")
    oa_multi_parser.add_argument("--max-workers", type=int, default=None,
        help="Maximum number of worker threads. Defaults to min(#concepts, cpu_cores*2)."
    )
    oa_multi_parser.add_argument("--skip-existing", action="store_true",
        help="Skip concepts already recorded in openalex_ingested_concepts."
    )
    oa_multi_parser.add_argument("--per-field-limit", type=int, default=500,
        help="Maximum number of concepts to fetch per field (default: 500)."
    )
    oa_multi_parser.add_argument("--verify", action="store_true", help="Run verify/enrich pass instead of fresh ingest.")

    # Enrich authors/papers/concepts
    oa_enrich = subparsers.add_parser("enrich", help="Enrich existing papers/authors with missing data from OpenAlex.")
    oa_enrich.add_argument("--authors", action="store_true", help="Enrich authors.")
    oa_enrich.add_argument("--papers", action="store_true", help="Enrich papers.")
    oa_enrich.add_argument("--concepts", action="store_true", help="Enrich concepts.")
    oa_enrich.add_argument("--concept-ids", nargs="*", help="List of concept IDs to limit paper enrichment to (CXXXX format).")
    oa_enrich.add_argument("--max-workers", type=int, default=None, help="Maximum number of worker threads. ")

    # Embedding papers
    embed_parser = subparsers.add_parser("embed", help="Embedding utilities (papers, authors, etc.).")
    embed_sub = embed_parser.add_subparsers(dest="embed_command", required=True)

    embed_papers_parser = embed_sub.add_parser("papers", help="Embed papers into vectors using an embedding model.")
    embed_papers_parser.add_argument("--model", default="bge-small-en-v1.5-local", 
        help="Embedding model name (default: bge-small-en-v1.5-local)."
    )
    embed_papers_parser.add_argument("--batch-size", type=int, default=64, help="Embedding batch size per API call.")
    embed_papers_parser.add_argument("--limit", type=int, default=None, help="Optional max number of papers to embed (for testing).")

    return parser


def main():
    cli_log.banner("LitScout", subtitle="Universal CLI")

    parser = build_parser()
    args = parser.parse_args()

    # DB COMMANDS
    if args.category in ("db", "database"):
        if args.db_cmd == "start":
            cli_log.info("Starting PostgreSQL...")
            start_postgres()

        elif args.db_cmd == "stop":
            cli_log.info("Stopping PostgreSQL...")
            stop_postgres()

        elif args.db_cmd == "init":
            cli_log.info("Initializing database schema...")
            init_database(
                # force=args.force,
                db_name=args.db_name,
                db_user=args.db_user,
                db_host=args.db_host,
                db_port=args.db_port,
            )

    # INGEST COMMANDS
    elif args.category == "ingest":
        if args.ingest_cmd == "openalex":
            cli_log.info(f"Starting OpenAlex ingestion for concept {args.concept_id} ({args.pages} pages)...")
            ingest_openalex_concept(concept_id=args.concept_id, pages=args.pages)

        elif args.ingest_cmd == "openalex-multi":
            fields = [f.strip() for f in (args.fields or []) if f.strip()]
            if not fields:
                cli_log.error("No valid fields provided. Use --fields 'computer science' 'economics' ...")
                return

            max_workers = min(os.cpu_count() or 4, args.max_workers or 4)
            cli_log.info(
                "Starting OpenAlex multi-field ingestion for fields={fields}, pages={args.pages}, max_workers={max_workers}, "
                f"skip_existing={args.skip_existing}, per_field_limit={args.per_field_limit}..."
            )

            ingest_openalex_from_fields(
                fields=fields, max_workers=max_workers, pages=args.pages, 
                skip_existing=args.skip_existing, per_field_limit=args.per_field_limit,
            )

    # ENRICH COMMAND
    elif args.category == "enrich":
        if not args.authors and not args.papers and not args.concepts:
            cli_log.error("No enrichment target specified. Use at least one of --authors, --papers and --concepts.")
            return

        msg = []
        if args.authors: msg.append("authors")
        if args.papers:
            papers_msg = "papers"
            if args.concept_ids:
                papers_msg += f" with concepts: {' '.join(args.concept_ids)}"
            msg.append(papers_msg)
        if args.concepts: msg.append("concepts")
        
        max_workers = min(os.cpu_count() or 4, args.max_workers or 4)
        cli_log.info(f"Starting OpenAlex enrichment for {', '.join(msg)} with {max_workers} workers...")
        
        enrich_openalex(
            enrich_authors=args.authors, enrich_papers=args.papers, enrich_concepts=args.concepts,
            concept_ids=[c.strip() for c in (args.concept_ids or []) if c.strip()], max_workers=max_workers
        )

    # EMBEDDING COMMANDS
    elif args.category == "embed":
        if args.embed_command == "papers":
            cli_log.info(f"Embedding papers using model '{args.model}'...")
            embed_missing_papers(model_name=args.model, batch_size=args.batch_size, limit=args.limit)

    else:
        cli_log.error("Unknown command.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        from colorama import Fore
        from server.logger import ColorLogger  # or however you import it

        # Create a minimal logger here if cli_log isn't in scope, or just print:
        try:
            cli_log.warn("Interrupted by user (Ctrl+C). Exiting immediately.")
        except NameError:
            print(Fore.YELLOW + "[CLI - WARN] Interrupted by user (Ctrl+C). Exiting immediately.")

        import os
        os._exit(130)  # hard exit, kills all worker threads
# server/cli.py

import os
import sys
import shlex
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
from server.semantic.embeddings import embed_missing_papers, embed_missing_concepts
from server.semantic.search import search_papers, search_papers_hybrid, search_papers_via_concepts

cli_log = ColorLogger("CLI", include_timestamps=False, include_threading_id=False)


def add_db_options(parser: argparse.ArgumentParser):
    """Common DB override flags used by db init/start/stop."""
    parser.add_argument("--db-name", help="Override database name.")
    parser.add_argument("--db-user", help="Override database user.")
    parser.add_argument("--db-host", help="Override database host.")
    parser.add_argument("--db-port", help="Override database port.")


def build_parser() -> argparse.ArgumentParser:
    """Build the full CLI argument parser (reused in REPL)."""
    parser = argparse.ArgumentParser(
        prog="litscout",
        description="LitScout Command Line Interface",
        add_help=True,
    )
    subparsers = parser.add_subparsers(dest="category", required=True)

    # =========================
    # DB COMMANDS
    # =========================
    for db_alias in ("db", "database"):
        db_parser = subparsers.add_parser(db_alias, help="Database operations")
        db_subp = db_parser.add_subparsers(dest="db_cmd", required=True)

        db_subp.add_parser("start", help="Start PostgreSQL server.")
        db_subp.add_parser("stop", help="Stop PostgreSQL server.")

        init_p = db_subp.add_parser(
            "init",
            help="Initialize or reinitialize DB schema.",
        )
        init_p.add_argument(
            "-F",
            "--force",
            action="store_true",
            help="Force: drop and recreate database before applying schema.",
        )
        add_db_options(init_p)

    # =========================
    # INGEST COMMANDS
    # =========================
    ingest_parser = subparsers.add_parser("ingest", help="Data ingestion commands.")
    ingest_subp = ingest_parser.add_subparsers(dest="ingest_cmd", required=True)

    # Single concept
    oa_parser = ingest_subp.add_parser(
        "openalex", help="Ingest a single OpenAlex concept."
    )
    oa_parser.add_argument(
        "--concept_id",
        required=True,
        help="OpenAlex concept ID, e.g., C41008148.",
    )
    oa_parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="How many pages of results to fetch (~200 papers per page).",
    )
    oa_parser.add_argument(
        "--verify",
        action="store_true",
        help="Re-enrich existing papers for this concept.",
    )

    # Multi: by fields
    oa_multi_parser = ingest_subp.add_parser(
        "openalex-multi",
        help="Fetch concepts by field(s) and ingest them in parallel.",
    )
    oa_multi_parser.add_argument(
        "--fields",
        nargs="+",
        required=True,
        help=(
            "One or more field names to search concepts for, e.g.: "
            "--fields 'computer science' 'economics'."
        ),
    )
    oa_multi_parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="How many pages per concept to fetch (~200 papers per page).",
    )
    oa_multi_parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help=(
            "Maximum number of worker threads. Defaults to min(#concepts, cpu_cores*2)."
        ),
    )
    oa_multi_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip concepts already recorded in openalex_ingested_concepts.",
    )
    oa_multi_parser.add_argument(
        "--per-field-limit",
        type=int,
        default=500,
        help="Maximum number of concepts to fetch per field (default: 500).",
    )
    oa_multi_parser.add_argument(
        "--verify",
        action="store_true",
        help="Run verify/enrich pass instead of fresh ingest.",
    )

    # =========================
    # ENRICH COMMANDS
    # =========================
    oa_enrich = subparsers.add_parser(
        "enrich",
        help="Enrich existing papers/authors/concepts with missing data from OpenAlex.",
    )
    oa_enrich.add_argument(
        "--authors",
        action="store_true",
        help="Enrich authors.",
    )
    oa_enrich.add_argument(
        "--papers",
        action="store_true",
        help="Enrich papers.",
    )
    oa_enrich.add_argument(
        "--concepts",
        action="store_true",
        help="Enrich concepts.",
    )
    oa_enrich.add_argument(
        "--concept-ids",
        nargs="*",
        help="List of concept IDs to limit paper enrichment to (CXXXX format).",
    )
    oa_enrich.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of worker threads.",
    )

    # =========================
    # SEMANTIC
    # =========================
    semantic_parser = subparsers.add_parser(
        "semantic",
        help="Semantic utilities (embedding and search)",
    )
    semantic_subp = semantic_parser.add_subparsers(dest="semantic_cmd", required=True)

    # Embeddings
    embed_parser = semantic_subp.add_parser(
        "embed",
        help="Embed missing paper texts."
    )
    embed_parser.add_argument(
        "embed_command",
        choices=["papers", "concepts"],
        help="What to embed.",
    )
    embed_parser.add_argument(
        "--model",
        type=str,
        help="Model name/label to use for embedding papers.",
    )
    embed_parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Number of texts to embed per batch.",
    )
    embed_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of papers to embed.",
    )

    # Semantic Search
    sem_search_parser = semantic_subp.add_parser(
        "search",
        help="Semantic search over paper embeddings.",
    )
    sem_search_parser.add_argument(
        "search_command",
        choices=["papers", "concepts", "hybrid"],
        help="What to embed.",
    )
    sem_search_parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Search query string.",
    )
    sem_search_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of top results to return.",
    )
    sem_search_parser.add_argument(
        "--concepts-limit",
        type=int,
        default=10,
        help="Number of top concepts to consider in concept-based search.",
    )
    sem_search_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of top results to skip.",
    )
    sem_search_parser.add_argument(
        "--paper-weight",
        type=float,
        default=0.6,
        help="Weight for paper similarity in hybrid search.",
    )
    sem_search_parser.add_argument(
        "--concept-weight",
        type=float,
        default=0.4,
        help="Weight for concept-based similarity in hybrid search.",
    )

    return parser


def run_command(args: argparse.Namespace) -> None:
    """
    Dispatch parsed args to the appropriate handler.
    This is called both from one-shot mode and REPL mode.
    """

    # =========================
    # DB
    # =========================
    if args.category in ("db", "database"):
        if args.db_cmd == "start":
            start_postgres()

        elif args.db_cmd == "stop":
            stop_postgres()

        elif args.db_cmd == "init":
            cli_log.info("Initializing database schema...")
            init_database(
                force=getattr(args, "force", False),
                db_name=args.db_name,
                db_user=args.db_user,
                db_host=args.db_host,
                db_port=args.db_port,
            )
        return

    # =========================
    # INGEST
    # =========================
    if args.category == "ingest":
        if args.ingest_cmd == "openalex":
            cli_log.info(
                f"Starting OpenAlex ingestion for concept {args.concept_id} "
                f"({args.pages} pages)..."
            )
            ingest_openalex_concept(
                concept_id=args.concept_id,
                pages=args.pages,
                verify=args.verify,
            )

        elif args.ingest_cmd == "openalex-multi":
            fields = [f.strip() for f in (args.fields or []) if f.strip()]
            if not fields:
                cli_log.error(
                    "No valid fields provided. Use --fields 'computer science' 'economics' ..."
                )
                return

            max_workers = args.max_workers or (os.cpu_count() or 4)
            cli_log.info(
                f"Starting OpenAlex multi-field ingestion for "
                f"fields={fields}, pages={args.pages}, max_workers={max_workers}, "
                f"skip_existing={args.skip_existing}, per_field_limit={args.per_field_limit}, "
                f"verify={args.verify}..."
            )

            ingest_openalex_from_fields(
                fields=fields,
                max_workers=max_workers,
                pages=args.pages,
                skip_existing=args.skip_existing,
                per_field_limit=args.per_field_limit,
                verify=args.verify,
            )
        return

    # =========================
    # ENRICH
    # =========================
    if args.category == "enrich":
        if not args.authors and not args.papers and not args.concepts:
            cli_log.error(
                "No enrichment target specified. Use at least one of "
                "--authors, --papers, --concepts."
            )
            return

        parts = []
        if args.authors:
            parts.append("authors")
        if args.papers:
            papers_msg = "papers"
            if args.concept_ids:
                papers_msg += f" (only concepts: {' '.join(args.concept_ids)})"
            parts.append(papers_msg)
        if args.concepts:
            parts.append("concepts")

        max_workers = args.max_workers or (os.cpu_count() or 4)
        cli_log.info(
            f"Starting OpenAlex enrichment for {', '.join(parts)} "
            f"with {max_workers} workers..."
        )

        enrich_openalex(
            enrich_authors=args.authors,
            enrich_papers=args.papers,
            enrich_concepts=args.concepts,
            concept_ids=[
                c.strip() for c in (args.concept_ids or []) if c.strip()
            ],
            max_workers=max_workers,
        )
        return

    # =========================
    # SEMANTIC
    # =========================
    if args.category == "semantic":
        if args.semantic_cmd == "embed":
            if args.embed_command == "papers":
                embed_missing_papers(
                    batch_size=args.batch_size,
                    limit=args.limit,
                )
            elif args.embed_command == "concepts":
                embed_missing_concepts(
                    batch_size=args.batch_size,
                    limit=args.limit,
                )
            return
        elif args.semantic_cmd == "search":
            if args.search_command == "papers":
                results = search_papers(query=args.query, limit=args.limit, offset=args.offset)
                cli_log.info(f"Top {len(results)} results:")
                for r in results:
                    print(f"{r['similarity']:.3f}  |  {r['external_ids']['openalex']}  |  {r['title']}")
                return
            elif args.search_command == "concepts":
                result = search_papers_via_concepts(
                    query=args.query, top_k_concepts=args.concepts_limit,
                    top_k_papers_per_concept=args.limit, limit=args.limit, offset=args.offset,
                )
                cli_log.info(f"Top {result['offset']} - {result['offset'] + result['limit']} results:")

                print(f"Found via {len(result['concepts'])} concepts:")
                for c in result['concepts']:
                    print(f"  {c['similarity']:.3f}  |  {c['concept_id']} | {c['name']}")
                print("Papers:")
                for r in result['papers']:
                    print(f"  {r['total_score']:.3f}  |  {r['external_ids']['openalex']}  |  {r['title']}")
                return
            elif args.search_command == "hybrid":
                if args.paper_weight + args.concept_weight != 1.0:
                    if args.paper_weight == 0.4:
                        args.paper_weight = 1.0 - args.concept_weight
                    else:
                        args.concept_weight = 1.0 - args.paper_weight
                    cli_log.warn(
                        f"paper_weight and concept_weight must sum to 1.0; "
                        f"adjusted to paper_weight={args.paper_weight}, concept_weight={args.concept_weight}."
                    )

                result = search_papers_hybrid(
                    query=args.query, limit=args.limit, offset=args.offset,
                    paper_weight=args.paper_weight, concept_weight=args.concept_weight,
                    top_k_concepts=args.concepts_limit, top_k_papers_per_concept=args.limit
                )
                cli_log.info(f"Top {result['offset']} - {result['offset'] + result['limit']} results:")
                for r in result['papers']:
                    print(f"{r['combined_score']:.3f}  |  {r['external_ids']['openalex']}  |  {r['title']}")
                return

    cli_log.error("Unknown command.")


def repl(parser: argparse.ArgumentParser) -> None:
    """
    Interactive REPL:
        litscout> db start
        litscout> ingest openalex --concept_id C41008148 --pages 2
        litscout> embed papers --limit 100
        litscout> search --query "graph neural networks" --limit 5
        litscout> exit
    """
    cli_log.info(
        "Interactive mode. Type 'help' for global help, "
        "'exit' or 'quit' to leave."
    )

    while True:
        try:
            line = input("litscout> ")
        except (EOFError, KeyboardInterrupt):
            print()
            cli_log.warn("Exiting interactive mode.")
            break

        line = line.strip()
        if not line:
            continue

        if line in ("exit", "quit", "q"):
            cli_log.info("Goodbye.")
            break

        if line in ("help", "?"):
            parser.print_help()
            continue

        # Allow inline help like: "db --help" or "ingest openalex --help"
        tokens = shlex.split(line)
        try:
            args = parser.parse_args(tokens)
        except SystemExit:
            # argparse tried to call sys.exit (e.g. on error); ignore and continue REPL
            continue

        try:
            run_command(args)
        except KeyboardInterrupt:
            cli_log.warn("Command interrupted by user (Ctrl+C).")
        except Exception as e:
            cli_log.error(f"Command failed: {e}")


def main():
    cli_log.banner("LitScout", subtitle="Universal CLI")

    parser = build_parser()

    # One-shot mode: called like `py -m server.cli ingest openalex ...`
    if len(sys.argv) > 1:
        args = parser.parse_args()
        run_command(args)
        return

    repl(parser)


if __name__ == "__main__":
    main()
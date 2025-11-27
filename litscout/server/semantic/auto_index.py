from typing import Tuple
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor

from server.logger import ColorLogger
from colorama import Fore

log = ColorLogger("INDEX", tag_color=Fore.MAGENTA, include_timestamps=False)

PAPER_INDEX_NAME = "idx_paper_embeddings_vec_ivfflat"
PAPER_TABLE_NAME = "paper_embeddings"
PAPER_VECTOR_COLUMN = "embedding_vec"
CONCEPT_INDEX_NAME = "idx_concept_embeddings_vec_ivfflat"
CONCEPT_TABLE_NAME = "concept_embeddings"
CONCEPT_VECTOR_COLUMN = "embedding_vec"


def choose_ivfflat_lists(num_rows: int) -> int:
    if num_rows < 1000: return 50
    if num_rows < 10000: return 100
    if num_rows < 100000: return 200
    if num_rows < 1000000: return 1000
    return 2000


def choose_probes(lists: int) -> int:
    if lists <= 0: return 1
    if lists <= 50: return 5
    if lists <= 100: return 10
    if lists <= 200: return 20
    if lists <= 1000: return 50
    return 100


def _get_row_count(cur: RealDictCursor, table_name: str, vector_column: str) -> int:
    """
    Count non-NULL vectors in the given table/column.
    (Matches the semantics you used for paper_embeddings.)
    """
    cur.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {table_name}
        WHERE {vector_column} IS NOT NULL;
        """
    )
    row = cur.fetchone()
    return int(row["cnt"] or 0)


def _get_current_lists(cur: RealDictCursor, index_name: str) -> int | None:
    """
    Introspect pg_class/pg_index to discover current IVFFLAT 'lists' setting
    for the given index name, or None if no such index.
    """
    cur.execute(
        """
        SELECT
            CASE
                WHEN reloptions IS NULL THEN NULL
                ELSE
                    (
                        regexp_match(
                            reloptions::text,
                            '.*lists=([0-9]+).*'
                        )
                    )[1]::int
            END AS lists
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = %s;
        """,
        (index_name,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row["lists"]


def _ensure_embedding_index(
    conn: PGConnection, *, table_name: str, index_name: str,
    vector_column: str, label: str, dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Shared implementation for IVFFLAT index tuning on any <table, index, column>.
    Returns (lists, probes).
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1) How many rows do we have?
    num_rows = _get_row_count(cur, table_name, vector_column)
    if num_rows == 0:
        log.warn(
            f"No rows with non-NULL {vector_column} found in {table_name}. "
            "Skipping index tuning."
        )
        cur.close()
        return (50, 1)

    desired_lists = choose_ivfflat_lists(num_rows)
    current_lists = _get_current_lists(cur, index_name)

    if current_lists is None:
        log.info(
            f"No IVFFLAT index '{index_name}' found on {table_name} (or not detectable). "
            f"Will ensure one exists with lists={desired_lists} for {num_rows} rows."
        )
        if not dry_run:
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table_name}
                USING ivfflat ({vector_column} vector_l2_ops)
                WITH (lists = %s);
                """,
                (desired_lists,),
            )
            conn.commit()
            log.success(f"Ensured IVFFLAT index '{index_name}' exists with lists={desired_lists}.")
    else:
        if current_lists == 0:
            ratio = 999.0
        else:
            ratio = max(desired_lists, current_lists) / min(desired_lists, current_lists)

        if ratio > 1.5:
            log.warn(f"Existing IVFFLAT index '{index_name}' uses lists={current_lists}, "
                f"but heuristic suggests lists={desired_lists} for {num_rows} rows "
                f"on {label}."
            )
            if not dry_run:
                log.info(f"Dropping and recreating index '{index_name}' with lists={desired_lists}...")
                cur.execute(f"DROP INDEX IF EXISTS {index_name};")
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {index_name}
                    ON {table_name}
                    USING ivfflat ({vector_column} vector_l2_ops)
                    WITH (lists = %s);
                    """,
                    (desired_lists,),
                )
                conn.commit()
                log.success(f"Recreated IVFFLAT index '{index_name}' with lists={desired_lists}.")
        else:
            desired_lists = current_lists  # keep existing
            log.info(
                f"IVFFLAT index '{index_name}' already exists with lists={current_lists}, "
                f"which is close enough for {num_rows} rows on {label}."
            )

    cur.close()

    probes = choose_probes(desired_lists)
    return desired_lists, probes


def ensure_paper_embedding_index(conn: PGConnection, dry_run: bool = False) -> Tuple[int, int]:
    return _ensure_embedding_index(
        conn, table_name=PAPER_TABLE_NAME, index_name=PAPER_INDEX_NAME,
        vector_column=PAPER_VECTOR_COLUMN, label="paper_embeddings", dry_run=dry_run,
    )


def ensure_concept_embedding_index(conn: PGConnection, dry_run: bool = False) -> Tuple[int, int]:
    return _ensure_embedding_index(
        conn, table_name=CONCEPT_TABLE_NAME, index_name=CONCEPT_INDEX_NAME,
        vector_column=CONCEPT_VECTOR_COLUMN, label="concept_embeddings", dry_run=dry_run,
    )
# server/semantic/auto_index.py

import re
from typing import Optional, Tuple

from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor

from server.logger import ColorLogger
from colorama import Fore

log = ColorLogger("INDEX", tag_color=Fore.MAGENTA, include_timestamps=False)


INDEX_NAME = "idx_paper_embeddings_vec_ivfflat"
TABLE_NAME = "paper_embeddings"
VECTOR_COLUMN = "embedding_vec"


def choose_ivfflat_lists(num_rows: int) -> int:
    """
    Heuristic to choose number of lists based on row count.
    Tuned for 'paper_embeddings' scale.
    """
    if num_rows <= 0:
        return 50
    if num_rows < 1_000:
        return 50
    if num_rows < 10_000:
        return 100
    if num_rows < 100_000:
        return 200
    if num_rows < 1_000_000:
        return 1000
    return 2000


def choose_probes(lists: int) -> int:
    """
    Heuristic: how many lists to probe per query.
    Higher = better recall, slower.
    """
    if lists <= 0:
        return 1
    # 10% of lists, capped between 1 and 50
    probes = max(1, min(50, lists // 10))
    return probes


def _get_row_count(cur) -> int:
    cur.execute(f"SELECT COUNT(*) AS n FROM {TABLE_NAME} WHERE {VECTOR_COLUMN} IS NOT NULL;")
    row = cur.fetchone()
    return int(row["n"]) if row else 0


def _get_current_lists(cur) -> Optional[int]:
    """
    Parse current index definition to see which 'lists = N' it has, if any.
    """
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = %s AND indexname = %s;
        """,
        (TABLE_NAME, INDEX_NAME),
    )
    row = cur.fetchone()
    if not row:
        return None

    indexdef = row["indexdef"]
    m = re.search(r"lists\s*=\s*(\d+)", indexdef)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def ensure_paper_embedding_index(conn: PGConnection, dry_run: bool = False) -> Tuple[int, int]:
    """
    Ensure an IVFFLAT index on paper_embeddings(embedding_vec) exists
    and is tuned based on row count.

    Returns:
        (lists, probes) being used / recommended.

    If dry_run=True, it will only log what it *would* do, without changing the DB.
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1) How many rows do we have?
    num_rows = _get_row_count(cur)
    if num_rows == 0:
        log.warn("No rows with non-NULL embedding_vec found in paper_embeddings. Skipping index tuning.")
        cur.close()
        return (50, 1)

    desired_lists = choose_ivfflat_lists(num_rows)
    current_lists = _get_current_lists(cur)

    if current_lists is None:
        log.info(
            f"No IVFFLAT index '{INDEX_NAME}' found on {TABLE_NAME} (or not detectable). "
            f"Will ensure one exists with lists={desired_lists} for {num_rows} rows."
        )
        if not dry_run:
            # Use IF NOT EXISTS to avoid 'relation already exists' errors if the index
            # was created earlier by migrations or another process.
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {INDEX_NAME}
                ON {TABLE_NAME}
                USING ivfflat ({VECTOR_COLUMN} vector_l2_ops)
                WITH (lists = %s);
                """,
                (desired_lists,),
            )
            conn.commit()
            log.success(f"Ensured IVFFLAT index '{INDEX_NAME}' exists with lists={desired_lists}.")
    else:
        # Compare current vs desired
        if current_lists == 0:
            ratio = 999.0
        else:
            ratio = max(desired_lists, current_lists) / min(desired_lists, current_lists)

        if ratio > 1.5:
            log.warn(
                f"Existing IVFFLAT index '{INDEX_NAME}' uses lists={current_lists}, "
                f"but heuristic suggests lists={desired_lists} for {num_rows} rows."
            )
            if not dry_run:
                log.info(f"Dropping and recreating index '{INDEX_NAME}' with lists={desired_lists}...")
                cur.execute(f"DROP INDEX IF EXISTS {INDEX_NAME};")
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {INDEX_NAME}
                    ON {TABLE_NAME}
                    USING ivfflat ({VECTOR_COLUMN} vector_l2_ops)
                    WITH (lists = %s);
                    """,
                    (desired_lists,),
                )
                conn.commit()
                log.success(f"Recreated IVFFLAT index '{INDEX_NAME}' with lists={desired_lists}.")
        else:
            desired_lists = current_lists  # keep existing
            log.info(
                f"IVFFLAT index '{INDEX_NAME}' already exists with lists={current_lists}, "
                f"which is close enough for {num_rows} rows."
            )

    cur.close()

    probes = choose_probes(desired_lists)
    return desired_lists, probes
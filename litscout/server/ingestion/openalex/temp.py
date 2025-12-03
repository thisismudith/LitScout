import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from server.database.db_utils import get_conn
from server.utils.progress import ProgressBar
from server.logger import ColorLogger
from colorama import Fore

BASE = "https://api.openalex.org/works"
HEADERS = {"User-Agent": "LitScout Backfill/1.0"}
log = ColorLogger("openalex_backfill", tag_color=Fore.BLUE, include_timestamps=False)


def _strip_id(val):
    if not val:
        return None
    return val.replace("https://openalex.org/", "")


def _fetch_works_by_ids(openalex_ids):
    """
    Fetch up to 50 works by their OpenAlex IDs (short form, e.g. 'W2741809807').

    Uses:
        GET /works?filter=openalex:W1|W2|W3...
    """
    if not openalex_ids:
        return {}

    params = {
        "filter": "openalex:" + "|".join(openalex_ids),
        "per-page": len(openalex_ids),
    }
    resp = requests.get(BASE, params=params, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    data = resp.json()

    out = {}
    for w in data.get("results", []):
        key = _strip_id(w.get("id"))
        if key:
            out[key] = w
    return out


def backfill_paper_sources_via_api_threaded(
    batch_size: int = 50,
    max_workers: int = 6,
) -> None:
    # 1) Fetch all candidate papers
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, external_ids ->> 'openalex' AS openalex_id
        FROM papers
        WHERE source_id IS NULL
          AND external_ids ->> 'openalex' IS NOT NULL
        ORDER BY id;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        log.info("No papers require backfilling of source_id/publisher_id.")
        return

    total_papers = len(rows)
    log.info(
        f"Backfilling source_id/publisher_id for {total_papers} papers "
        f"using OpenAlex API (batch_size={batch_size}, max_workers={max_workers})."
    )

    # 2) Chunk into batches (OpenAlex allows up to 50 IDs per request)
    if batch_size > 50:
        batch_size = 50

    chunks = [rows[i : i + batch_size] for i in range(0, total_papers, batch_size)]

    def _row_id_and_openalex(row):
        """Helper to support both tuple rows and dict rows."""
        if isinstance(row, dict):
            return row["id"], row["openalex_id"]
        else:
            # tuple: (id, openalex_id)
            return row[0], row[1]

    # 3) Worker function for a chunk
    def _process_chunk(chunk) -> int:
        """
        Process one chunk of papers:
          - fetch works from OpenAlex
          - compute source_id/publisher_id
          - update DB
        Returns number of papers in this chunk.
        """
        # Extract OpenAlex IDs safely (dict or tuple)
        openalex_ids = []
        for r in chunk:
            _, oa_id = _row_id_and_openalex(r)
            if oa_id:
                openalex_ids.append(oa_id)

        if not openalex_ids:
            return len(chunk)

        works_map = _fetch_works_by_ids(openalex_ids)

        updates = []  # (source_id, publisher_id, paper_id)
        for row in chunk:
            paper_id, openalex_id = _row_id_and_openalex(row)
            w = works_map.get(openalex_id)
            if not w:
                continue

            loc = (w or {}).get("primary_location") or {}
            src = loc.get("source") or {}

            source_id = _strip_id(src.get("id"))
            publisher_id = _strip_id(src.get("host_organization"))

            if source_id:
                updates.append((source_id, publisher_id, paper_id))

        if updates:
            conn_local = get_conn()
            cur_local = conn_local.cursor()
            cur_local.executemany(
                """
                UPDATE papers
                SET source_id = %s,
                    publisher_id = %s
                WHERE id = %s;
                """,
                updates,
            )
            conn_local.commit()
            cur_local.close()
            conn_local.close()

        return len(chunk)

    # 4) Thread pool + progress bar
    desc = f"Backfilling papers via OpenAlex (threads={max_workers})"
    processed = 0

    with ProgressBar(total=total_papers, desc=desc, unit="paper") as bar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_size = {
                executor.submit(_process_chunk, chunk): len(chunk)
                for chunk in chunks
            }

            for future in as_completed(future_to_size):
                chunk_size = future_to_size[future]
                try:
                    processed_in_chunk = future.result()
                except Exception as e:
                    log.error(f"Error in backfill worker: {e}")
                    # still advance bar by chunk_size so progress roughly reflects coverage
                    bar.update(chunk_size)
                    processed += chunk_size
                    continue

                bar.update(processed_in_chunk)
                processed += processed_in_chunk

    log.success(
        f"Backfill completed: {processed}/{total_papers} papers updated "
        f"(batch_size={batch_size}, max_workers={max_workers})."
    )
    return

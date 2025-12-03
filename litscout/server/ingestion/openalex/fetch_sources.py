from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import Fore

from server.utils.progress import ProgressBar
from server.database.db_utils import get_conn
from server.logger import ColorLogger
from server.ingestion.openalex.ingest import ingest_source


log = ColorLogger("INGEST OA", Fore.GREEN, include_timestamps=True)

def ingest_sources_from_papers(batch_size: int = 100, max_workers: int = 6) -> None:
    """
    Iterate over all papers, collect distinct source_ids, and ensure
    there is a matching row in the 'sources' table for each.
    """
    conn = get_conn()
    cur = conn.cursor()

    # 1) Collect all distinct source_ids from papers
    cur.execute(
        """
        SELECT DISTINCT source_id
        FROM papers
        WHERE source_id IS NOT NULL;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    all_source_ids = [r[0] if not isinstance(r, dict) else r["id"] for r in rows]
    all_source_ids = [sid for sid in all_source_ids if sid]  # drop None / empty

    if not all_source_ids:
        log.info("No non-null id found in papers; nothing to ingest.")
        return

    # 2) Find which of those are already present in sources.id
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM sources
        WHERE id = ANY(%s);
        """,
        (all_source_ids,),
    )
    existing = {r[0] for r in cur.fetchall()}
    cur.close()
    conn.close()

    missing_ids = [sid for sid in all_source_ids if sid not in existing]

    if not missing_ids:
        log.info("All ids from papers already exist in sources table.")
        return

    log.info(
        f"Found {len(missing_ids)} missing sources out of {len(all_source_ids)} distinct "
        f"ids in papers. Ingesting with {max_workers} workers..."
    )

    # 3) Chunk missing_ids for nicer logging (not strictly necessary)
    chunks = [missing_ids[i : i + batch_size] for i in range(0, len(missing_ids), batch_size)]

    def _worker_ingest_source_ids(ids_chunk):
        for sid in ids_chunk:
            ingest_source(sid)
        return len(ids_chunk)

    processed = 0
    desc = f"Ingesting sources from papers (threads={max_workers})"

    with ProgressBar(total=len(missing_ids), desc=desc, unit="source") as bar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_size = {
                executor.submit(_worker_ingest_source_ids, chunk): len(chunk)
                for chunk in chunks
            }

            for future in as_completed(future_to_size):
                chunk_size = future_to_size[future]
                try:
                    processed_in_chunk = future.result()
                except Exception as e:
                    log.error(f"Error in sources ingest worker: {e}")
                    # still advance bar
                    bar.update(chunk_size)
                    processed += chunk_size
                    continue

                bar.update(processed_in_chunk)
                processed += processed_in_chunk

    log.success(
        f"Sources ingest completed: {processed}/{len(missing_ids)} missing sources processed."
    )
# server/ingestion/openalex/ingest.py

import os
import time
import traceback
from typing import List, Set, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from server.ingestion.db_writer import (
    get_conn,
    upsert_venue,
    get_or_create_venue_instance,
    upsert_author,
    upsert_paper,
    insert_paper_authors,
)
from server.ingestion.openalex.client import iter_works_for_concept
from server.ingestion.openalex.normalizer import normalize_openalex_work
from server.database.db_utils import log
from server.utils.progress import create_progress_bar


OPENALEX_CONCEPTS_URL = "https://api.openalex.org/concepts"


# -------------------------------------------------------------------
# Tracking table for "which concepts have already been ingested"
# -------------------------------------------------------------------
def ensure_openalex_tracking_table_global() -> None:
    """
    Ensure the tracking table exists once, before threaded ingestion.

    This avoids concurrent CREATE TABLE races from multiple threads.
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        ensure_openalex_tracking_table(cur)
        conn.commit()
    finally:
        cur.close()
        conn.close()

def ensure_openalex_tracking_table(cur) -> None:
    """
    Ensure the tracking table for ingested OpenAlex concepts exists.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS openalex_ingested_concepts (
            concept_id       TEXT PRIMARY KEY,
            pages_ingested   INTEGER,
            last_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def mark_openalex_concept_ingested(cur, concept_id: str, pages: int) -> None:
    """
    Mark a concept as ingested in the tracking table.
    """
    ensure_openalex_tracking_table(cur)
    cur.execute(
        """
        INSERT INTO openalex_ingested_concepts (concept_id, pages_ingested, last_ingested_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (concept_id) DO UPDATE
          SET pages_ingested   = EXCLUDED.pages_ingested,
              last_ingested_at = NOW();
        """,
        (concept_id, pages),
    )


def get_existing_openalex_concepts(concept_ids: List[str]) -> Set[str]:
    """
    Return the subset of concept_ids that are already in openalex_ingested_concepts.
    """
    if not concept_ids:
        return set()

    conn = get_conn()
    cur = conn.cursor()
    try:
        ensure_openalex_tracking_table(cur)
        cur.execute(
            """
            SELECT concept_id
            FROM openalex_ingested_concepts
            WHERE concept_id = ANY(%s);
            """,
            (concept_ids,),
        )
        rows = cur.fetchall()
        return {row[0] for row in rows}
    finally:
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# Single-concept ingestion
# -------------------------------------------------------------------

def ingest_openalex_concept(
    concept_id: str,
    pages: int = 1,
    show_progress: bool = True,
    log_output: bool = True,
) -> None:
    """
    Ingest OpenAlex works for a single concept into the litscout database.

    If show_progress=True, displays a per-paper ASCII progress bar.
    Safe to call from multiple threads because it uses its own DB connection.
    """
    conn = get_conn()
    cur = conn.cursor()

    progress = None
    try:
        conn.commit()

        # Rough estimate: ~200 works per page from OpenAlex
        per_page_estimate = 200
        total_estimated = pages * per_page_estimate

        log.info(
            f"[INGEST-OA] Fetching works for concept {concept_id} "
            f"(pages={pages}, ~{total_estimated} works estimated)..."
        )

        if show_progress:
            progress = create_progress_bar(
                total=total_estimated,
                desc=f"OpenAlex {concept_id}",
                unit="paper",
            )

        count = 0

        for work in iter_works_for_concept(concept_id, pages=pages):
            # Normalize JSON → NormalizedPaper
            p = normalize_openalex_work(work)

            # Venue & instance
            venue_id = None
            venue_instance_id = None
            if p.venue:
                venue_id = upsert_venue(cur, p.venue)
                year = p.venue_instance.year if p.venue_instance else p.year
                venue_instance_id = get_or_create_venue_instance(
                    cur, venue_id, year
                )

            # Authors
            author_ids = [upsert_author(cur, a) for a in p.authors]

            # Paper
            paper_id = upsert_paper(cur, p, venue_id, venue_instance_id)

            # Paper-author mapping
            insert_paper_authors(cur, paper_id, p, author_ids)

            conn.commit()
            count += 1

            if progress is not None:
                progress.update(1)

        # Mark concept as ingested
        mark_openalex_concept_ingested(cur, concept_id, pages)
        conn.commit()

        if progress is not None:
            progress.close()
        if log_output:
            log.success(
                f"[INGEST-OA] Concept {concept_id}: ingestion completed. "
                f"Processed {count} works."
            )
    except Exception as e:
        conn.rollback()
        if progress is not None:
            progress.close()
        log.error(
            f"[INGEST-OA] Concept {concept_id}: ingestion failed; "
            f"transaction rolled back."
        )
        log.error(str(e))
        traceback.print_exc()
        raise

    finally:
        if progress is not None:
            progress.close()
        cur.close()
        conn.close()


# -------------------------------------------------------------------
# Multi-concept, threaded ingestion (concept IDs already known)
# -------------------------------------------------------------------

def ingest_openalex_concepts(
    concept_ids: List[str],
    pages: int = 1,
    max_workers: int | None = None,
    skip_existing: bool = False,
) -> None:
    """
    Ingest multiple OpenAlex concepts in parallel using threads.

    Each concept is ingested with its own DB connection via ingest_openalex_concept.
    """
    if not concept_ids:
        log.warn("[INGEST-OA] No concept IDs provided; nothing to ingest.")
        return

    # Deduplicate while preserving order
    concept_ids = list(dict.fromkeys(concept_ids))

    # Optionally filter out already ingested concepts
    if skip_existing:
        existing = get_existing_openalex_concepts(concept_ids)
        if existing:
            log.info(
                f"[INGEST-OA] Skipping {len(existing)} concepts that are already ingested."
            )
        concept_ids = [cid for cid in concept_ids if cid not in existing]

        if not concept_ids:
            log.info("[INGEST-OA] All provided concepts are already ingested; nothing to do.")
            return

    # Cap concurrency to avoid OpenAlex 429s
    MAX_OPENALEX_WORKERS = 8
    CONCEPT_COUNT = len(concept_ids)

    if max_workers is None or max_workers <= 0:
        cpu = os.cpu_count() or 4
        max_workers = min(len(concept_ids), cpu, MAX_OPENALEX_WORKERS)
    else:
        if max_workers > MAX_OPENALEX_WORKERS:
            log.warn(
                f"[INGEST-OA] Requested {max_workers} workers, "
                f"capping to {MAX_OPENALEX_WORKERS} to respect OpenAlex rate limits."
            )
            max_workers = MAX_OPENALEX_WORKERS

    log.info(
        f"[INGEST-OA] Starting parallel ingestion for {CONCEPT_COUNT} concepts "
        f"using up to {max_workers} threads (pages={pages} each)..."
    )

    results: List[tuple[str, bool, str | None]] = []

    def worker(cid: str) -> tuple[str, bool, str | None]:
        try:
            # Disable per-paper progress in multi mode to avoid messy output
            ingest_openalex_concept(cid, pages=pages, show_progress=False, log_output=False)
            return cid, True, None
        except Exception as e:
            return cid, False, str(e)

    # Progress bars
    concept_bar = create_progress_bar(
        total=len(concept_ids),
        desc="Concepts",
        unit="concept",
    )

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(worker, cid): cid for cid in concept_ids}

            from concurrent.futures import as_completed

            for future in as_completed(future_map):
                cid = future_map[future]

                try:
                    concept_id, ok, err = future.result()
                    if ok:
                        log.success(f"[INGEST-OA] Concept {concept_id} finished successfully.")
                    else:
                        log.error(f"[INGEST-OA] Concept {concept_id} failed: {err}")
                    results.append((concept_id, ok, err))
                except Exception as e:
                    log.error(f"[INGEST-OA] Concept {cid} raised an unexpected error: {e}")
                    traceback.print_exc()
                    results.append((cid, False, str(e)))
                finally:
                    # update bar for each finished concept
                    concept_bar.update(1)
    finally:
        concept_bar.close()

    # Summary
    success_count = sum(1 for _, ok, _ in results if ok)
    failure_count = len(results) - success_count

    log.info(
        f"[INGEST-OA] Parallel ingestion completed: "
        f"{success_count} succeeded, {failure_count} failed."
    )


# -------------------------------------------------------------------
# Fetch concepts from OpenAlex by field name
# -------------------------------------------------------------------
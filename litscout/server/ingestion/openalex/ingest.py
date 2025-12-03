# server/ingestion/openalex/ingest.py

import traceback
from typing import Any, Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor

from colorama import Fore
from psycopg2.extras import Json
import requests

from server.ingestion.models import NormalizedSource
from server.logger import ColorLogger
from server.database.db_utils import get_conn
from server.ingestion.db_writer import upsert_concept, upsert_sources_batch, upsert_author, upsert_paper, insert_paper_authors
from server.ingestion.openalex.client import iter_works_for_concept
from server.ingestion.openalex.normalizer import normalize_openalex_source, normalize_openalex_work
from server.utils.progress import create_progress_bar

log = ColorLogger("INGEST OA", Fore.GREEN, include_timestamps=True, include_threading_id=False)

OPENALEX_CONCEPTS_URL = "https://api.openalex.org/concepts"
OPENALEX_SOURCES_URL = "https://api.openalex.org/sources"


# Tracking table for "which concepts have already been ingested"
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


# Single-concept ingestion
def ingest_openalex_concept(concept_id: str, pages: int = 1, show_progress: bool = True, log_output: bool = True) -> None:
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
            f"Fetching works for concept {concept_id} "
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
            # Concepts
            for cid, info in p.concepts.items():
                upsert_concept(cur, cid, info["name"], info["level"])

            # Authors
            author_ids = [upsert_author(cur, a) for a in p.authors]

            # Paper
            paper_id = upsert_paper(cur, p)

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
            log.success(f"Concept {concept_id}: ingestion completed. Processed {count} works.")
    except Exception as e:
        conn.rollback()
        if progress is not None:
            progress.close()
        log.error(f"Concept {concept_id}: ingestion failed; transaction rolled back.")
        log.error(str(e))
        traceback.print_exc()
        raise

    finally:
        if progress is not None:
            progress.close()
        cur.close()
        conn.close()
    
    log.success(f"Concept {concept_id}: ingestion completed. Processed {count} works.")


# Multi-concept, threaded ingestion (concept IDs already known)
def ingest_openalex_concepts(concept_ids: List[str], max_workers: int, pages: int = 1, skip_existing: bool = False) -> None:
    """
    Ingest multiple OpenAlex concepts in parallel using threads.

    Each concept is ingested with its own DB connection via ingest_openalex_concept.
    """
    if not concept_ids:
        log.warn("No concept IDs provided; nothing to ingest.")
        return

    # Deduplicate while preserving order
    concept_ids = list(dict.fromkeys(concept_ids))

    # Optionally filter out already ingested concepts
    if skip_existing:
        existing = get_existing_openalex_concepts(concept_ids)
        if existing:
            log.info(
                f"Skipping {len(existing)} concepts that are already ingested."
            )
        concept_ids = [cid for cid in concept_ids if cid not in existing]

        if not concept_ids:
            log.info("All provided concepts are already ingested; nothing to do.")
            return

    CONCEPT_COUNT = len(concept_ids)
    max_workers = min(CONCEPT_COUNT, max_workers)

    log.info(
        f"Starting parallel ingestion for {CONCEPT_COUNT} concepts "
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
                        log.success(f"Concept {concept_id} finished successfully.")
                    else:
                        log.error(f"Concept {concept_id} failed: {err}")
                    results.append((concept_id, ok, err))
                except Exception as e:
                    log.error(f"Concept {cid} raised an unexpected error: {e}")
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

    log.success(f"Parallel ingestion completed: {success_count} succeeded, {failure_count} failed.")


# Source ingestions
def _fetch_source_by_id(source_id: str) -> Dict[str, Any]:
    """
    Fetch a single source from OpenAlex by its short_id (S...) or full URL.
    """
    if source_id.startswith("http"):
        url = source_id
    else:
        url = f"{OPENALEX_SOURCES_URL}/{source_id}"

    resp = requests.get(url, timeout=25)
    resp.raise_for_status()
    return resp.json()


def _upsert_sources_batch(records: list[NormalizedSource]) -> None:
    """
    Upsert a batch of sources into the 'sources' table.
    Expects records from normalize_openalex_source (dicts).
    """
    if not records:
        return

    conn = get_conn()
    cur = conn.cursor()

    values = [
        (
            r.id,
            r.name,
            r.source_type,
            r.host_organization_id,
            r.host_organization_name,
            r.country_code,
            r.issn_l,
            r.issn,
            r.is_oa,
            r.is_in_doaj,
            r.works_count,
            r.cited_by_count,
            Json(r.summary_stats),
            Json(r.topics),
            Json(r.counts_by_year),
            r.homepage_url,
            r.created_date,
            r.updated_date,
        )
        for r in records
        if r.id
    ]

    if not values:
        cur.close()
        conn.close()
        return

    cur.executemany(
        """
        INSERT INTO sources (
            id,
            name,
            source_type,
            host_organization_id,
            host_organization_name,
            country_code,
            issn_l,
            issn,
            is_oa,
            is_in_doaj,
            works_count,
            cited_by_count,
            summary_stats,
            topics,
            counts_by_year,
            homepage_url,
            created_date,
            updated_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            name                   = EXCLUDED.name,
            source_type            = EXCLUDED.source_type,
            host_organization_id   = EXCLUDED.host_organization_id,
            host_organization_name = EXCLUDED.host_organization_name,
            country_code           = EXCLUDED.country_code,
            issn_l                 = EXCLUDED.issn_l,
            issn                   = EXCLUDED.issn,
            is_oa                  = EXCLUDED.is_oa,
            is_in_doaj             = EXCLUDED.is_in_doaj,
            works_count            = EXCLUDED.works_count,
            cited_by_count         = EXCLUDED.cited_by_count,
            summary_stats          = EXCLUDED.summary_stats,
            topics                 = EXCLUDED.topics,
            counts_by_year         = EXCLUDED.counts_by_year,
            homepage_url           = EXCLUDED.homepage_url,
            created_date           = EXCLUDED.created_date,
            updated_date           = EXCLUDED.updated_date;
        """,
        values,
    )

    conn.commit()
    cur.close()
    conn.close()


def ingest_source(source_id: str) -> None:
    try:
        raw = _fetch_source_by_id(source_id)
    except Exception as e:
        log.error(f"Failed to fetch source {source_id} from OpenAlex: {e}")
        return
    norm = normalize_openalex_source(raw)
    _upsert_sources_batch([norm])
    log.info(f"Ingested/updated source {source_id} ({norm.name!r}).")
# server/ingestion/openalex/verify.py

from typing import Optional
from server.ingestion.db_writer import get_conn, upsert_paper, upsert_author, insert_paper_authors, upsert_venue_and_instance
from server.ingestion.openalex.client import _get
from server.ingestion.openalex.normalizer import normalize_openalex_work
from server.database.db_utils import log


def _fetch_work_by_openalex_url(openalex_url: str) -> dict:
    # openalex_url like "https://openalex.org/Wxxxx"
    return _get(openalex_url, params=None)


def verify_and_enrich_papers(limit: Optional[int] = None) -> None:
    """
    Iterate over existing papers and fill missing values:
    - concepts
    - abstract/title/venue if missing (from OpenAlex)
    - author affiliations + stats + specialties

    Relies on papers.external_ids->>'openalex' to be present.
    """
    conn = get_conn(role="VERIFY")
    cur = conn.cursor()

    # Find candidate papers to fix
    cur.execute(
        """
        SELECT id, external_ids->>'openalex' AS openalex_url
        FROM papers
        WHERE (concepts IS NULL OR abstract IS NULL OR venue_id IS NULL)
          AND external_ids->>'openalex' IS NOT NULL
        ORDER BY id
        """
        + ("LIMIT %s" if limit is not None else ""),
        ((limit,) if limit is not None else ()),
    )
    rows = cur.fetchall()
    total = len(rows)
    log.info(f"[VERIFY] Found {total} papers needing verification/enrichment.")

    for idx, (paper_id, openalex_url) in enumerate(rows, start=1):
        log.info(f"[VERIFY] ({idx}/{total}) Fixing paper id={paper_id} ({openalex_url})...")
        try:
            work_json = _fetch_work_by_openalex_url(openalex_url)
            np = normalize_openalex_work(work_json)

            # upsert venue/instance and paper (will update missing fields)
            venue_id, venue_instance_id = upsert_venue_and_instance(cur, np)
            new_paper_id = upsert_paper(cur, np, venue_id, venue_instance_id)

            # now update authors with enriched info
            author_ids = [upsert_author(cur, a, fetch_details=True) for a in np.authors]
            insert_paper_authors(cur, new_paper_id, np, author_ids)

            conn.commit()
        except Exception as e:
            conn.rollback()
            log.error(f"[VERIFY] Failed to verify paper id={paper_id}: {e}")

    cur.close()
    conn.close()
    log.success("[VERIFY] Verification/enrichment pass completed.")
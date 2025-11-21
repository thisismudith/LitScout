# server/ingestion/openalex/ingest.py

from server.ingestion.db_writer import (
    get_conn,
    ensure_source,
    upsert_venue,
    get_or_create_venue_instance,
    upsert_author,
    upsert_paper,
    insert_paper_authors,
    insert_paper_source,
)
from server.ingestion.openalex.client import iter_works_for_concept
from server.ingestion.openalex.normalizer import normalize_openalex_work
from server.logger import ColorLogger
from colorama import Fore

log = ColorLogger("INGEST-OA", tag_color=Fore.MAGENTA, include_timestamps=True)


def ingest_openalex_concept(concept_id: str, pages: int = 1):
    conn = get_conn()
    cur = conn.cursor()
    try:
        source_id = ensure_source(cur, "openalex")
        conn.commit()

        for work in iter_works_for_concept(concept_id, pages=pages):
            p = normalize_openalex_work(work)

            venue_id = None
            venue_instance_id = None
            if p.venue:
                venue_id = upsert_venue(cur, p.venue)
                venue_instance_id = get_or_create_venue_instance(cur, venue_id, p.year)

            author_ids = []
            for a in p.authors:
                aid = upsert_author(cur, a)
                author_ids.append(aid)

            paper_id = upsert_paper(cur, p, venue_id, venue_instance_id)

            insert_paper_authors(cur, paper_id, p, author_ids)

            oa_id = p.external_ids.get("openalex")
            url = f"https://openalex.org/{oa_id.split('/')[-1]}" if oa_id else None
            if oa_id:
                insert_paper_source(cur, paper_id, source_id, oa_id, url)

            conn.commit()

        log.success("OpenAlex ingestion completed.")
    except Exception as e:
        conn.rollback()
        log.error("OpenAlex ingestion failed; transaction rolled back.")
        print(e)
    finally:
        cur.close()
        conn.close()
# server/ingestion/openalex/enrich.py

import time
from typing import Any, Dict

from colorama import Fore
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import Json

from server.database.db_utils import get_conn
from server.ingestion.openalex.client import _get
from server.logger import ColorLogger
from server.utils.progress import create_progress_bar

log = ColorLogger("ENRICH", Fore.YELLOW, include_timestamps=True)


# Fetch helpers
def fetcher(openalex_id: str):
    url = f"https://api.openalex.org/{openalex_id}"
    return _get(url)


def enrich_single_concept(cur, concept_id: str):
    """Update a single concept entry in the DB."""
    data = fetcher(concept_id)

    cur.execute(
        """
        UPDATE concepts
        SET
            description = %s,
            works_count = %s,
            cited_by_count = %s,
            related_concepts = %s
        WHERE id = %s;
        """,
        (
            data.get("description"),
            data.get("works_count"),
            data.get("cited_by_count"),
            Json(data.get("related_concepts")),
            concept_id,
        ),
    )

    return True


def enrich_concepts_chunked(max_workers: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT id FROM concepts ORDER BY id;")
    concepts = [row["id"] for row in cur.fetchall()]

    log.info(f"Enriching {len(concepts)} concepts")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(enrich_single_concept, cur, concept): concept for concept in concepts
        }

        progress = create_progress_bar(total=len(futures), desc="Concepts", unit="concepts")

        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                concept_id = futures[f]
                log.error(f"Concept enrichment failed for concept {concept_id}: {e}")
            finally:
                progress.update(1)

        progress.close()

    conn.commit()
    cur.close()
    conn.close()


# Author Enrichment
def enrich_single_author(cur, author: Dict[str, Any]):
    """Update a single author entry in the DB."""
    author_id = author["external_ids"]["openalex"]
    data = fetcher(author_id)

    # Parse details
    full_name = data.get("display_name")
    works = int(data.get("works_count"))
    cited = int(data.get("cited_by_count"))
    affiliations = []
    last_known_institutions = []
    topic_shares = data.get("topic_shares") or []
    topics = data.get("topics") or []
    orcid = data.get("orcid")
    ids = data.get("ids") or {}

    for aff in data.get("affiliations", []):
        inst = aff.get("institution") or {}
        affiliations.append(
            {
                "name": inst.get("display_name"),
                "id": inst.get("id"),
                "country_code": inst.get("country_code"),
                "type": inst.get("type"),
                "years": aff.get("years"),
            }
        )

    for laff in data.get("last_known_institutions", []):
        last_known_institutions.append(
            {
                "name": laff.get("display_name"),
                "id": laff.get("id"),
                "country_code": laff.get("country_code"),
                "type": laff.get("type"),
            }
        )

    cur.execute(
        """
        UPDATE authors
        SET
            full_name = %s,
            works_counted = %s,
            cited_by_count = %s,
            affiliations = %s,
            last_known_institutions = %s,
            topics = %s,
            topic_shares = %s,
            orcid = %s,
            external_ids = %s
        WHERE id = %s;
        """,
        (
            full_name,
            works,
            cited,
            Json(affiliations),
            Json(last_known_institutions),
            Json(topics),
            Json(topic_shares),
            orcid,
            Json(ids),
            author["id"],
        ),
    )

    return True, None


def enrich_authors_chunked(max_workers: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM authors ORDER BY id;")
    authors = cur.fetchall()

    log.info(f"Enriching {len(authors)} authors…")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(enrich_single_author, cur, author): author["id"] for author in authors
        }

        progress = create_progress_bar(total=len(futures), desc="Authors", unit="authors")

        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                author_id = futures[f]
                log.error(f"Author enrichment failed for author {author_id}: {e}")
            finally:
                progress.update(1)

        progress.close()

    conn.commit()
    cur.close()
    conn.close()


# Paper Enrichment
def enrich_single_paper(cur, paper: Dict[str, Any]):
    paper_id = paper["external_ids"]["openalex"]
    data = fetcher(paper_id)

    # paper update fields
    abstract = data.get("abstract_inverted_index")
    title = data.get("title")

    # Concepts
    concepts_map: Dict[str, float] = {}
    for c in data.get("concepts", []):
        cid = (c.get("id") or "").split("/")[-1]
        score = c.get("score")
        if not cid or score is None or score <= 0.0:
            continue
        prev = concepts_map.get(cid, {"score": 0.0})["score"]
        if score > prev:
            concepts_map[cid] = {
                "name": c.get("display_name"),
                "level": c.get("level"),
                "score": float(score),
            }

    cur.execute(
        """
        UPDATE papers
        SET
            title = %s,
            abstract = %s,
            concepts = %s
        WHERE id = %s;
        """,
        (
            title,
            abstract,
            Json(concepts_map),
            paper["id"],
        ),
    )

    return True


def enrich_papers_chunked(max_workers: int, concept_ids: list = None):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if concept_ids:
        cur.execute(
            """
            SELECT * FROM papers
            WHERE concepts ?| %s::text[]
            ORDER BY id;
            """,
            (concept_ids,),
        )
    else:
        cur.execute("SELECT * FROM papers ORDER BY id;")

    papers = cur.fetchall()

    log.info(f"Enriching {len(papers)} papers…")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(enrich_single_paper, cur, p): p["id"] for p in papers
        }

        progress = create_progress_bar(total=len(futures), desc="Papers", unit="papers")

        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                paper_id = futures[f]
                log.error(f"Paper enrichment failed for paper {paper_id}: {e}")
            finally:
                progress.update(1)

        progress.close()

    conn.commit()
    cur.close()
    conn.close()


def enrich_openalex(
    enrich_authors: bool,
    enrich_papers: bool,
    enrich_concepts: bool,
    concept_ids: list,
    max_workers: int,
):
    if enrich_authors:
        enrich_authors_chunked(max_workers=max_workers)
        log.success("Author enrichment completed.")

    if enrich_papers:
        enrich_papers_chunked(max_workers=max_workers, concept_ids=concept_ids)
        log.success("Paper enrichment completed.")

    if enrich_concepts:
        enrich_concepts_chunked(max_workers=max_workers)
        log.success("Concept enrichment completed.")

    log.success("OpenAlex enrichment process finished.")
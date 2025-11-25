# server/ingestion/openalex/enrich.py

import os
from typing import Dict
from colorama import Fore
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

from server.ingestion.db_writer import get_conn
from server.ingestion.openalex.client import _get
from server.logger import ColorLogger

log = ColorLogger("ENRICH", Fore.YELLOW, include_timestamps=True)


# Fetch helpers
def fetcher(openalex_id: str):
    time.sleep(0.3)
    url = f"https://api.openalex.org/{openalex_id}"
    return _get(url)

# Concept Enrichment
def enrich_single_concept(cur, concept_id: str):
    """Update a single concept entry in the DB."""
    data = fetcher(concept_id)

    # Update concept entry
    cur.execute("""
        UPDATE openalex_concepts
        SET
            description = %s,
            works_count = %s,
            cited_by_count = %s,
            related_concepts = %s
        WHERE concept_id = %s;
    """, (
        data.get("description"),
        data.get("works_count"),
        data.get("cited_by_count"),
        data.get("related_concepts"),
        concept_id
    ))

    return True


def enrich_concepts_chunked(max_workers: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM concepts ORDER BY id;")
    concepts = cur.fetchall()

    log.info(f"Enriching {len(concepts)} concepts")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(enrich_single_concept, cur, concept): concept["id"]
            for concept in concepts
        }

        for f in tqdm(as_completed(futures), total=len(futures), desc="Concepts"):
            _ok, _err = f.result()

    conn.commit()
    cur.close()
    conn.close()


# Author Enrichment
def enrich_single_author(cur, author_id: str):
    """Update a single author entry in the DB."""
    data = fetcher(author_id)

    # Parse details
    full_name = data.get("display_name")
    works = data.get("works_count")
    cited = data.get("cited_by_count")
    affiliations = []
    last_known_institutions = []
    topic_shares = data.get("topic_shares") or []
    topics = data.get("topics") or []
    orcid = data.get("orcid")
    ids = data.get("ids") or {}

    for aff in data.get("affiliations", []):
        inst = aff.get("institution") or {}
        affiliations.append({
            "name": inst.get("display_name"),
            "id": inst.get("id"),
            "country_code": inst.get("country_code"),
            "type": inst.get("type"),
            "years": aff.get("years"),
        })
    
    for laff in data.get("last_known_institutions", []):
        last_known_institutions.append({
            "name": laff.get("display_name"),
            "id": laff.get("id"),
            "country_code": laff.get("country_code"),
            "type": laff.get("type"),
        })

    cur.execute("""
        UPDATE authors
        SET
            full_name = %s,
            works_count = %s,
            cited_by_count = %s,
            affiliations = %s,
            last_known_institutions = %s,
            topics = %s,
            topic_shares = %s,
            orcid = %s,
            external_ids = %s
        WHERE id = %s;
    """, (
        full_name,
        works,
        cited,
        affiliations,
        last_known_institutions,
        topics,
        topic_shares,
        orcid,
        ids,
        author_id
    ))

    return True, None


def enrich_authors_chunked(max_workers: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM authors ORDER BY id;")
    authors = cur.fetchall()

    log.info(f"Enriching {len(authors)} authors…")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(enrich_single_author, cur, author): author["id"]
            for author in authors
        }

        for f in tqdm(as_completed(futures), total=len(futures), desc="Authors"):
            _ok, _err = f.result()

    conn.commit()
    cur.close()
    conn.close()


# Paper Enrichment
def enrich_single_paper(cur, paper_id: str):
    data = fetcher(paper_id)

    # paper update fields
    abstract = data.get("abstract_inverted_index")
    title = data.get("title")
  
    # Concepts
    concepts_map: Dict[str, float] = {}
    for c in data.get("concepts", []):
        id = c.get("id").split("/")[-1]
        score = c.get("score")
        if not id or score is None or score <= 0.0:
            continue
        # If same name appears multiple times, keep max score
        prev = concepts_map.get(id, {"score": 0.0})["score"]
        if score > prev:
            concepts_map[id] = {
                "name": c.get("display_name"),
                "level": c.get("level"),
                "score": float(score)
            }

    cur.execute("""
        UPDATE papers
        SET
            title = %s,
            abstract = %s,
            concepts = %s
        WHERE id = %s;
    """, (
        title,
        abstract,
        concepts_map,
        paper_id
    ))

    return True


def enrich_papers_chunked(max_workers: int, concept_ids: list = None):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if concept_ids:
        cur.execute("""
            SELECT * FROM papers
            WHERE concepts ?| %s::text[]
            ORDER BY id;
        """, (concept_ids,))
    else:
        cur.execute("SELECT * FROM papers ORDER BY id;")

    papers = cur.fetchall()

    log.info(f"Enriching {len(papers)} papers…")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(enrich_single_paper, cur, p): p["id"]
            for p in papers
        }
        for f in tqdm(as_completed(futures), total=len(futures), desc="Papers"):
            f.result()

    conn.commit()
    cur.close()
    conn.close()


def enrich_openalex(enrich_authors: bool, enrich_papers: bool, enrich_concepts: bool, concept_ids: list, max_workers: int):
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
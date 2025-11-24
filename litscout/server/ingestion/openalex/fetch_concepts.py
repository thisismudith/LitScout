# server/ingestion/openalex/fetch_concepts.py

from typing import List, Dict, Any
import requests
import time
from server.database.db_utils import log
from server.ingestion.openalex.ingest import (
    ingest_openalex_concepts,
    ensure_openalex_tracking_table_global,
)

OPENALEX_CONCEPTS_URL = "https://api.openalex.org/concepts"
PER_PAGE_CONCEPTS = 200  # max per OpenAlex docs

def _extract_concept_id(openalex_url: str | None) -> str | None:
    """
    OpenAlex concept IDs look like 'https://openalex.org/C41008148'.
    This extracts 'C41008148' from that URL.
    """
    if not openalex_url:
        return None
    return openalex_url.rsplit("/", 1)[-1]


def fetch_concepts_for_field(field_name: str, limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetch up to `limit` concepts for a given field from OpenAlex.

    Uses the 'search' filter on display_name and sorts by works_count (desc).
    """
    concepts: List[Dict[str, Any]] = []
    page = 1

    log.info(f"[INGEST-OA] Fetching up to {limit} concepts for field '{field_name}'")

    while len(concepts) < limit:
        params = {
            "search": field_name,
            "per_page": PER_PAGE_CONCEPTS,
            "page": page,
            "sort": "works_count:desc",
        }

        resp = requests.get(OPENALEX_CONCEPTS_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            log.warn(
                f"[INGEST-OA] No more concept results for field '{field_name}' at page {page}."
            )
            break

        for c in results:
            concepts.append(c)
            if len(concepts) >= limit:
                break

        log.info(
            f"[INGEST-OA] Field '{field_name}': collected {len(concepts)} concepts so far..."
        )
        page += 1
        time.sleep(0.2)  # be nice to the API

    return concepts


def fetch_openalex_concept_ids_for_fields(
    fields: List[str],
    per_field_limit: int = 500,
) -> List[str]:
    """
    For each field in `fields`, fetch up to per_field_limit concepts from OpenAlex,
    then return a deduplicated list of concept IDs (CXXXX format), sorted by works_count desc.
    """
    all_concepts: Dict[str, Dict[str, Any]] = {}

    for field_name in fields:
        field_name = field_name.strip()
        if not field_name:
            continue

        concepts = fetch_concepts_for_field(field_name, limit=per_field_limit)
        for c in concepts:
            cid = _extract_concept_id(c.get("id"))
            if not cid:
                continue

            works_count = c.get("works_count", 0)
            # keep the version with higher works_count if duplicate
            if cid not in all_concepts or works_count > all_concepts[cid].get("works_count", 0):
                all_concepts[cid] = c

    log.info(
        f"[INGEST-OA] Total unique concepts collected for fields {fields}: "
        f"{len(all_concepts)}"
    )

    # Sort by works_count descending and return just IDs
    sorted_concepts = sorted(
        all_concepts.values(),
        key=lambda c: c.get("works_count", 0),
        reverse=True,
    )

    concept_ids = [
        _extract_concept_id(c.get("id"))
        for c in sorted_concepts
        if _extract_concept_id(c.get("id")) is not None
    ]

    return concept_ids


def ingest_openalex_from_fields(
    fields: List[str],
    pages: int = 1,
    max_workers: int | None = None,
    skip_existing: bool = False,
    per_field_limit: int = 500,
) -> None:
    """
    High-level helper:

    1) Fetch up to `per_field_limit` concepts per field from OpenAlex.
    2) Deduplicate + sort them.
    3) Call ingest_openalex_concepts to ingest them in parallel.
    """
    if not fields:
        log.warn("[INGEST-OA] No fields provided; nothing to ingest.")
        return

    log.info(
        f"[INGEST-OA] Resolving concepts for fields={fields}, "
        f"per_field_limit={per_field_limit}..."
    )

    concept_ids = fetch_openalex_concept_ids_for_fields(
        fields=fields,
        per_field_limit=per_field_limit,
    )

    if not concept_ids:
        log.warn(
            f"[INGEST-OA] No concepts found for fields={fields}; nothing to ingest."
        )
        return

    log.info(
        f"[INGEST-OA] Resolved {len(concept_ids)} unique concept IDs from fields={fields}."
    )
    ensure_openalex_tracking_table_global()

    ingest_openalex_concepts(
        concept_ids=concept_ids,
        pages=pages,
        max_workers=max_workers,
        skip_existing=skip_existing,
    )
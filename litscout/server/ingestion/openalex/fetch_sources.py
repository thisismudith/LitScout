# server/ingestion/openalex/fetch_sources.py

from typing import Any, Dict, List, Optional, Set, Tuple
import time
import requests
from colorama import Fore
from concurrent.futures import ThreadPoolExecutor, as_completed
from server.utils.progress import ProgressBar

from server.ingestion.openalex.ingest import ingest_openalex_sources_for_publisher
from server.utils.progress import ProgressBar
from server.logger import ColorLogger
from server.ingestion.openalex.normalizer import _shorten_id


OPENALEX_BASE = "https://api.openalex.org"
OPENALEX_CONCEPTS_URL = "https://api.openalex.org/concepts"
OPENALEX_SOURCES_URL = "https://api.openalex.org/sources"
HEADERS = {"User-Agent": "LitScout Ingest/1.0"}

log = ColorLogger("INGEST OA", Fore.GREEN, include_timestamps=True)


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, retries: int = 5, backoff_seconds: float = 1.0) -> Optional[Dict[str, Any]]:
    """
    Safe HTTP GET wrapper for OpenAlex with:
      - Retries
      - Backoff on failure
      - JSON validation
      
    Returns parsed json dict or `None` if unrecoverable error.
    """
    attempt = 1

    while attempt <= retries:
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=25)

            # Rate limit handling
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                log.warn(f"OpenAlex rate limited. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                attempt += 1
                continue

            # Success check
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict):
                log.error(f"Unexpected non-dict JSON at {url}")
                return None

            return data

        except Exception as e:
            if attempt == retries:
                log.error(f"Failed fetching {url} after {retries} attempts. Error: {e}")
                return None

            sleep_time = backoff_seconds * attempt
            log.warn(f"Error fetching {url}: {e} → retry {attempt}/{retries} in {sleep_time:.1f}s...")
            time.sleep(sleep_time)
            attempt += 1

    return None


def _resolve_field_to_concept_id(field_name: str) -> Optional[str]:
    """
    Resolve a human-friendly field (e.g., 'computer science', 'nlp')
    to a canonical OpenAlex concept ID (e.g., 'C41008148') using the
    OpenAlex Concepts Search API.

    Returns:
        concept_id (string starting with "C") or None if not found.
    """

    url = f"{OPENALEX_BASE}/concepts"
    params = {
        "search": field_name,
        "per-page": 5,  # Get top-ranked possible matches
        "sort": "works_count:desc",
    }

    data = _get_json(url, params)
    if not data:
        log.warn(f"No response from OpenAlex when resolving field '{field_name}'.")
        return None

    results = data.get("results", [])
    if not results:
        log.warn(f"No OpenAlex concept found for field '{field_name}'.")
        return None

    # Pick the highest works_count result (most authoritative)
    best = max(results, key=lambda r: r.get("works_count", 0))
    concept_id = best.get("id", "").replace("https://openalex.org/", "")

    log.info(
        f"Field '{field_name}' → concept {concept_id} "
        f"('{best.get('display_name')}', works_count={best.get('works_count')})"
    )

    return concept_id or None


def _resolve_field_to_concept_id(field_name: str) -> Optional[str]:
    """
    Given a human field label (e.g. "computer science", "economics"),
    find the best-matching OpenAlex CONCEPT and return its *short* ID,
    e.g. "C41008148".

    Strategy:
      - /concepts?search=<field_name>&per-page=1&sort=works_count:desc
      - pick the top result
    """
    params = {
        "search": field_name,
        "per-page": 1,
        "sort": "works_count:desc",
        "select": "id,display_name,description,works_count",
    }
    resp = requests.get(OPENALEX_CONCEPTS_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results") or []
    if not results:
        log.warn(f"No concept found for field={field_name!r}.")
        return None

    concept = results[0]
    full_id = concept.get("id")
    if not full_id:
        log.warn(f"Top concept for field={field_name!r} has no id; skipping.")
        return None

    short_id = _shorten_id(full_id)
    log.info(f"Field {field_name!r} → concept {short_id} ({concept.get('display_name')!r}, works_count={concept.get('works_count')}).")
    return short_id


def _collect_publishers_for_concept(field_name: str, concept_short_id: str, limit_publishers: int, per_page: int) -> List[str]:
    """
    Collect publishers (host organizations) that publish works tagged with a concept.

    Uses group_by=primary_location.source.host_organization to get
    the most active publishers for the field.
    """
    log.info(
        f"Collecting publishers for field '{field_name}' "
        f"(concept={concept_short_id}, limit={limit_publishers})..."
    )

    url = "https://api.openalex.org/works"
    params = {
        "filter": f"concept.id:{concept_short_id}",
        "group_by": "primary_location.source.host_organization",
        "sort": "count:desc",
        "per-page": min(per_page, limit_publishers),
    }

    data = _get_json(url, params)
    if not data or "group_by" not in data:
        log.warn(f"No publishers found via works for field '{field_name}'.")
        return []

    results: List[str] = []
    for group in data.get("group_by", []):
        key = group.get("key", "")
        if not key.startswith("https://openalex.org/"):
            continue
        pub_id = key.replace("https://openalex.org/", "")
        results.append(pub_id)
        if len(results) >= limit_publishers:
            break

    log.info(f"Field '{field_name}' → {len(results)} publisher IDs (limit now met)")
    return results


def ingest_sources_for_fields(
    fields: List[str], max_workers: int, publishers_per_field: int = 500,
    max_publishers: int = 1000, per_page_sources: int = 200,
) -> None:
    """
    ingest sources --fields "computer science" "economics" --limit 500

    Steps:
      1) For each field name (string):
           - Resolve it to an OpenAlex CONCEPT (C...) via /concepts?search=
           - From /sources?filter=x_concepts.id:<C...>, gather up to
             `publishers_per_field` unique publishers (P... from host_organization).
      2) Deduplicate publisher IDs across fields, capped at `max_publishers`.
      3) For each publisher ID, call ingest_sources_for_publisher(), which
         ingests all of that publisher's sources into our 'sources' table.
    """
    if not fields:
        log.warn("No fields passed to ingest_sources_for_fields; nothing to do.")
        return

    log.info(f"Starting sources ingestion for fields={fields!r}, publishers_per_field={publishers_per_field}, max_publishers={max_publishers}.")

    all_publishers_ordered: List[str] = []
    seen_publishers: Set[str] = set()

    for field_name in fields:
        if len(all_publishers_ordered) >= max_publishers:
            break

        concept_short = _resolve_field_to_concept_id(field_name)
        if not concept_short:
            continue

        remaining_for_field = min(publishers_per_field, max_publishers - len(all_publishers_ordered))
        if remaining_for_field <= 0:
            break

        field_publishers = _collect_publishers_for_concept(
            field_name=field_name, concept_short_id=concept_short,
            limit_publishers=remaining_for_field, per_page=per_page_sources,
        )

        for pid in field_publishers:
            if pid in seen_publishers:
                continue
            seen_publishers.add(pid)
            all_publishers_ordered.append(pid)

            if len(all_publishers_ordered) >= max_publishers:
                break

    total_publishers = len(all_publishers_ordered)
    if total_publishers == 0:
        log.warn(f"No publishers collected for fields={fields!r}. Check concept resolution or filters.")
        return

    log.info(
        f"Collected {total_publishers} unique publishers across fields={fields!r} "
        f"(max_publishers={max_publishers}). Beginning per-publisher ingestion "
        f"with up to {max_workers} workers..."
    )

    bar_desc = f"Ingesting sources ({len(fields)} fields)"

    def _ingest_single_publisher(pub_short_id: str) -> Tuple[str, bool, str]:
        """
        Worker for a single publisher.
        Returns (publisher_id, success_flag, error_message_if_any).
        """
        try:
            log.info(f"Ingesting sources for publisher={pub_short_id}...")
            ingest_openalex_sources_for_publisher(host_org_id=pub_short_id, per_page=per_page_sources)
            return pub_short_id, True, ""
        except Exception as e:
            # We log here and also propagate as result so caller can see failures.
            log.error(f"Error ingesting sources for publisher={pub_short_id}: {e}")
            return pub_short_id, False, str(e)

    failures: List[Tuple[str, str]] = []


    with ProgressBar(total=total_publishers, desc=bar_desc, unit="publisher") as bar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_pub = {
                executor.submit(_ingest_single_publisher, pub_id): pub_id
                for pub_id in all_publishers_ordered
            }

            for future in as_completed(future_to_pub):
                pub_id = future_to_pub[future]
                try:
                    _, ok, err_msg = future.result()
                    if not ok:
                        failures.append((pub_id, err_msg))
                except Exception as e:
                    # Catch anything unexpected that escaped _ingest_single_publisher
                    log.error(f"Unhandled error ingesting publisher={pub_id}: {e}")
                    failures.append((pub_id, str(e)))
                finally:
                    bar.update(1)

    if failures:
        log.warn(f"Completed sources ingestion with {len(failures)} failures out of {total_publishers} publishers.")
        for pub_id, msg in failures:
            log.warn(f"  - publisher={pub_id}, error={msg}")
    else:
        log.success(f"Completed sources ingestion for {total_publishers} publishers across fields={fields!r} (no failures).")
# server/ingestion/openalex/client.py

import time
from colorama import Fore
import requests
from server.logger import ColorLogger

BASE_URL = "https://api.openalex.org"
WORKS_URL = f"{BASE_URL}/works"

MAX_RETRIES = 5
BACKOFF_BASE = 1.5
INITIAL_DELAY = 1.0  # seconds

log = ColorLogger("INGEST OA", Fore.GREEN, include_timestamps=True)

def _get(url: str, params: dict | None = None) -> dict:
    """
    GET wrapper with retry + backoff for OpenAlex.

    Retries on:
      - 429 Too Many Requests (honors Retry-After if present)
      - 5xx server errors

    Returns:
        Parsed JSON dict from the response.
    """
    delay = INITIAL_DELAY
    last_resp: requests.Response | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=params, timeout=30)
        last_resp = resp

        # 429: rate limited
        if resp.status_code == 429:
            retry_after_header = resp.headers.get("Retry-After")
            if retry_after_header:
                try:
                    sleep_for = float(retry_after_header)
                except ValueError:
                    sleep_for = delay
            else:
                sleep_for = delay

            log.warn(
                f"[OA] 429 Too Many Requests (attempt {attempt}/{MAX_RETRIES}); "
                f"sleeping {sleep_for:.1f}s then retrying..."
            )
            time.sleep(sleep_for)
            delay *= BACKOFF_BASE
            continue

        # 5xx: transient server errors
        if 500 <= resp.status_code < 600:
            log.warn(
                f"[OA] {resp.status_code} from OpenAlex (attempt {attempt}/{MAX_RETRIES}); "
                f"sleeping {delay:.1f}s then retrying..."
            )
            time.sleep(delay)
            delay *= BACKOFF_BASE
            continue

        # Non-retry case: raise if error, then return JSON
        resp.raise_for_status()
        return resp.json()

    # If we exhaust retries, raise the last response's error
    if last_resp is not None:
        last_resp.raise_for_status()
        return last_resp.json()  # realistically unreachable, but for completeness

    # Fallback: should never happen
    raise RuntimeError("OpenAlex _get() failed without any response")


def iter_works_for_concept(concept_id: str, pages: int = 1):
    """
    Yield works for a concept from OpenAlex using the 'cursor' pagination.

    Args:
        concept_id: e.g. "C41008148"
        pages: how many pages to fetch (each ~200 works)
    """
    url = WORKS_URL
    cursor = "*"
    page_count = 0

    while page_count < pages:
        params = {
            "filter": f"concepts.id:{concept_id}",
            "per-page": 200,
            "cursor": cursor,
        }

        data = _get(url, params=params)
        results = data.get("results", [])
        if not results:
            break

        for w in results:
            yield w

        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break

        page_count += 1
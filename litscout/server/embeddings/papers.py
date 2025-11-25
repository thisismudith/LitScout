# server/embeddings/papers.py

from typing import List, Optional

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from colorama import Fore
from psycopg2.extras import RealDictCursor
from tqdm import tqdm

from server.database.db_utils import get_conn
from server.logger import ColorLogger
from openai import OpenAI

log = ColorLogger("EMBED", Fore.MAGENTA, include_timestamps=True)

# Embedding helpers
def embed_texts_openai(
    texts: List[str], api_key: Optional[str] = None,
    model_name: str = "text-embedding-3-large", batch_size: int = 64
) -> List[List[float]]:
    """
    Single-threaded convenience helper.
    Currently unused in the parallel path, but kept for potential reuse.
    """
    client = OpenAI(api_key=api_key)
    all_vectors: List[List[float]] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]

        # Backoff / simple retry loop
        for attempt in range(3):
            try:
                resp = client.embeddings.create(
                    model=model_name,
                    input=batch,
                )
                batch_vecs = [item.embedding for item in resp.data]
                all_vectors.extend(batch_vecs)
                break
            except Exception as e:
                if attempt == 2:
                    raise
                log.error(f"OpenAI embedding error (attempt {attempt+1}/3): {e}. Retrying...")
                time.sleep(2.0 * (attempt + 1))

    return all_vectors


def _embed_batch_openai(texts: List[str], api_key: str, model_name: str = "text-embedding-3-large", ) -> List[List[float]]:
    """
    Embed a single batch of texts using OpenAI.
    Designed to be run inside worker threads.
    """
    client = OpenAI(api_key=api_key)

    for attempt in range(3):
        try:
            resp = client.embeddings.create(
                model=model_name,
                input=texts,
            )
            return [item.embedding for item in resp.data]
        except Exception as e:
            if attempt == 2:
                raise
            wait = 2.0 * (attempt + 1)
            log.error(f"OpenAI embedding error (attempt {attempt+1}/3): {e}. Retrying in {wait:.1f}s...")
            time.sleep(wait)


# DB helpers
def _build_paper_text(row: dict) -> Optional[str]:
    """
    Build the text representation of a paper for embedding.

    Currently: title + abstract (+ optional conclusion if present).
    Returns None if there's effectively nothing to embed.
    """
    parts = []
    title = row.get("title")
    abstract = row.get("abstract")
    conclusion = row.get("conclusion")

    if title:
        parts.append(title)
    if abstract:
        parts.append(abstract)
    if conclusion:
        parts.append("Conclusion: " + conclusion)

    if not parts:
        return None

    return "\n\n".join(parts)


def _select_papers_needing_embeddings(cur, model_name: str, limit: Optional[int] = None):
    """
    Select papers that don't yet have an embedding for the given model.
    """
    params = [model_name]
    sql = """
        SELECT p.id, p.title, p.abstract, p.conclusion
        FROM papers p
        LEFT JOIN paper_embeddings e
            ON e.paper_id = p.id
           AND e.model_name = %s
        WHERE e.paper_id IS NULL
        ORDER BY p.id
    """
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    cur.execute(sql, params)
    return cur.fetchall()


def _insert_embeddings_batch(cur, model_name: str, paper_ids: List[int], embeddings: List[List[float]]):
    """
    Insert (or upsert) a batch of embeddings into paper_embeddings.
    """
    for pid, vec in zip(paper_ids, embeddings):
        cur.execute(
            """
            INSERT INTO paper_embeddings (paper_id, embedding, model_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (paper_id) DO UPDATE
            SET embedding = EXCLUDED.embedding,
                model_name = EXCLUDED.model_name,
                created_at = NOW();
            """,
            (pid, vec, model_name),
        )


def embed_missing_papers(
    max_workers: int, api_key: Optional[str] = None, model_name: str = "text-embedding-3-large",
    batch_size: int = 64, limit: Optional[int] = None,
) -> None:
    """
    Embed all papers that don't yet have embeddings for the given model.

    Args:
        max_workers: Number of parallel embedding workers to use.
        api_key:     OpenAI API key (or from env OPENAI_API_KEY).
        model_name:  The OpenAI embedding model name.
        batch_size:  How many texts per embedding API call.
        limit:       Optional upper bound on how many papers to embed (for testing).

    Behaviour:
        - Connects to DB using get_conn()
        - Finds papers without embeddings for `model_name`
        - Embeds in parallel batches (ThreadPoolExecutor)
        - Inserts/updates paper_embeddings
    """
    if api_key is None:
        api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        log.warn("OPENAI_API_KEY not found. Either set an environment variable  or provide an API key.")
        raise RuntimeError("An OpenAI API key was not found.")

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    log.info(f"Selecting papers without embeddings for model '{model_name}'...")
    papers = _select_papers_needing_embeddings(cur, model_name, limit=limit)

    if not papers:
        log.info("No papers need embeddings; everything is up to date.")
        cur.close()
        conn.close()
        return

    log.info(f"Found {len(papers)} papers to embed.")

    paper_ids: List[int] = []
    texts: List[str] = []

    for paper in papers:
        text = _build_paper_text(paper)
        if text is None:
            # Skip papers with no usable text
            continue
        paper_ids.append(paper["id"])
        texts.append(text)

    if not texts:
        log.warn("No usable text found in selected papers (all empty?). Nothing to embed.")
        cur.close()
        conn.close()
        return

    log.info(f"Actually embedding {len(texts)} papers (with non-empty text).")

    # Build batches (index, ids, texts)
    batches = []
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        batch_ids = paper_ids[i : i + batch_size]
        batches.append((i // batch_size, batch_ids, batch_texts))

    if not batches:
        log.warn("No batches to embed. Exiting.")
        cur.close()
        conn.close()
        return

    max_workers = max(1, min(max_workers, len(batches)))
    log.info(f"Embedding {len(batches)} batches with batch_size={batch_size}, max_workers={max_workers}...")

    # Parallel embedding; DB writes in the main thread
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {}
        for batch_index, batch_ids, batch_texts in batches:
            fut = executor.submit(_embed_batch_openai, batch_texts, api_key, model_name)
            future_to_batch[fut] = (batch_index, batch_ids)

        for fut in tqdm(as_completed(future_to_batch), total=len(future_to_batch), desc="Embedding batches"):
            batch_index, batch_ids = future_to_batch[fut]
            try:
                batch_vecs = fut.result()
            except Exception as e:
                log.error(f"Batch {batch_index} embedding FAILED: {e}")
                continue

            if len(batch_vecs) != len(batch_ids):
                log.error(f"Batch {batch_index} size mismatch: {len(batch_vecs)} embeddings vs {len(batch_ids)} paper_ids")
                continue

            _insert_embeddings_batch(cur, model_name, batch_ids, batch_vecs)
            conn.commit()

    log.success(f"Embedded {len(texts)} papers with model '{model_name}'")
    cur.close()
    conn.close()
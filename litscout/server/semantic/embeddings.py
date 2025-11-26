# server/semantic/embeddings.py

from typing import List, Optional

import time

from colorama import Fore
from psycopg2.extras import RealDictCursor

from server.globals import SEMANTIC_SEARCH_MODEL, SEMANTIC_SEARCH_MODEL_NAME, DEVICE
from server.database.db_utils import get_conn
from server.logger import ColorLogger
from server.utils.progress import create_progress_bar

log = ColorLogger("EMBED", Fore.MAGENTA, include_timestamps=True, include_threading_id=False)

# Embedding helpers
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


def _select_papers_needing_embeddings(cur, limit: Optional[int] = None):
    """
    Select papers that don't yet have an embedding for SEMANTIC_SEARCH_MODEL_NAME.
    """
    params = [SEMANTIC_SEARCH_MODEL_NAME]
    sql = """
        SELECT p.id, p.title, p.abstract, p.conclusion
        FROM papers p
        LEFT JOIN paper_embeddings e
            ON e.paper_id   = p.id
           AND e.model_name = %s
        WHERE e.paper_id IS NULL
        ORDER BY p.id
    """
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    cur.execute(sql, params)
    return cur.fetchall()


def _insert_embeddings_batch(cur, paper_ids: List[int], embeddings: List[List[float]]) -> None:
    """
    Insert (or upsert) a batch of embeddings into paper_embeddings.

    Assumes:
        - paper_embeddings has columns:
            paper_id        BIGINT
            model_name      TEXT
            embedding_vec   vector(384)
        - PRIMARY KEY (paper_id, model_name)
    """
    for pid, vec in zip(paper_ids, embeddings):
        cur.execute(
            """
            INSERT INTO paper_embeddings (paper_id, embedding_vec, model_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (paper_id, model_name) DO UPDATE
            SET embedding_vec = EXCLUDED.embedding_vec,
                created_at    = NOW();
            """,
            (pid, vec, SEMANTIC_SEARCH_MODEL_NAME),
        )


def embed_texts_local(texts: List[str], batch_size: int = 64) -> List[List[float]]:
    """
    Embed a list of texts using the shared local sentence-transformers model.

    Runs fully locally on CPU/GPU (no network, no API keys).
    Returns a list of embedding vectors (lists of floats).
    """
    all_vectors: List[List[float]] = []

    # We could call encode(texts, batch_size=batch_size) in one shot,
    # but this loop lets us add retries and progress if needed.
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]

        for attempt in range(3):
            try:
                vecs = SEMANTIC_SEARCH_MODEL.encode(
                    batch, batch_size=len(batch), show_progress_bar=False,
                    convert_to_numpy=True, normalize_embeddings=True
                )
                all_vectors.extend(vecs.tolist())
                break
            except Exception as e:
                if attempt == 2:
                    raise
                wait = 2.0 * (attempt + 1)
                log.warn(f"Error encoding batch attempt {attempt+1}/3): {e}. Retrying in {wait:.1f}s...")
                time.sleep(wait)

    return all_vectors


def embed_missing_papers(batch_size: int = 64, limit: Optional[int] = None) -> None:
    """
    Embed all papers that don't yet have embeddings for SEMANTIC_SEARCH_MODEL_NAME
    using the local sentence-transformers model.

    Args:
        batch_size:  Number of texts per model.encode call.
        limit:       Optional upper bound on how many papers to embed (for testing).
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    log.info(f"Selecting papers without embeddings for model label '{SEMANTIC_SEARCH_MODEL_NAME}'...")
    papers = _select_papers_needing_embeddings(cur, limit=limit)

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
            continue
        paper_ids.append(paper["id"])
        texts.append(text)

    if not texts:
        log.warn("No usable text found in selected papers (all empty?). Nothing to embed.")
        cur.close()
        conn.close()
        return

    log.info(f"Actually embedding {len(texts)} papers (with non-empty text) using '{SEMANTIC_SEARCH_MODEL_NAME}' on device '{DEVICE}'.")

    progress = create_progress_bar(total=len(texts), desc="Embedding papers", unit="papers")

    # Main embedding loop; we embed in batches and write each batch to DB
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        batch_ids = paper_ids[i : i + batch_size]

        try:
            batch_vecs = embed_texts_local(batch_texts, batch_size=len(batch_texts))
        except Exception as e:
            log.error(f"Embedding batch starting at index {i} FAILED: {e}")
            progress.update(len(batch_ids))
            continue

        if len(batch_vecs) != len(batch_ids):
            log.error(f"Batch size mismatch at index {i}: {len(batch_vecs)} embeddings vs {len(batch_ids)} paper_ids")
            progress.update(len(batch_ids))
            continue

        _insert_embeddings_batch(cur, batch_ids, batch_vecs)
        conn.commit()
        progress.update(len(batch_ids))

    progress.close()

    log.success(f"Embedded {len(texts)} papers using local model '{SEMANTIC_SEARCH_MODEL_NAME}' on device '{DEVICE}'.")
    cur.close()
    conn.close()
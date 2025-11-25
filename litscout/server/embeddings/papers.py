# server/embeddings/papers.py

from typing import List, Optional

import os
import time

from concurrent.futures import ThreadPoolExecutor  # kept in case you want later; currently unused
from colorama import Fore
from psycopg2.extras import RealDictCursor

from sentence_transformers import SentenceTransformer
import torch

from server.ingestion.db_writer import get_conn
from server.logger import ColorLogger
from server.utils.progress import create_progress_bar

log = ColorLogger("EMBED", Fore.MAGENTA, include_timestamps=True, include_threading_id=False)

# -------------------------------------------------------------------
# Model setup (local, GPU if available)
# -------------------------------------------------------------------

# HF model to actually load
HF_MODEL_NAME = os.getenv("LITSCOUT_EMBED_MODEL", "BAAI/bge-small-en-v1.5")

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"


# Embedding helpers
def embed_texts_local(texts: List[str], batch_size: int = 64) -> List[List[float]]:

    """
    Embed a list of texts using the local sentence-transformers model.
    Runs fully locally on CPU/GPU (no network, no API keys).
    Returns a list of embedding vectors (lists of floats).
    """
    all_vectors: List[List[float]] = []
    log.info(f"Loading local embedding model '{HF_MODEL_NAME}' on device '{DEVICE}'...")
    _model = SentenceTransformer(HF_MODEL_NAME, device=DEVICE)

    # We could just call encode(texts, batch_size=batch_size), but
    # this loop gives us an easy place to add retries if needed.
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]

        for attempt in range(3):
            try:
                vecs = _model.encode(
                    batch, batch_size=len(batch),
                    show_progress_bar=False, convert_to_numpy=True,
                    normalize_embeddings=True,  # good for cosine similarity / vector search
                )
                all_vectors.extend(vecs.tolist())
                break
            except Exception as e:
                if attempt == 2:
                    # After 3 failed attempts, give up
                    raise
                wait = 2.0 * (attempt + 1)
                log.warn(
                    f"[LOCAL-EMBED] Error encoding batch "
                    f"(attempt {attempt+1}/3): {e}. Retrying in {wait:.1f}s..."
                )
                time.sleep(wait)

    return all_vectors


# -------------------------------------------------------------------
# DB helpers
# -------------------------------------------------------------------

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
    Select papers that don't yet have an embedding for the given model_name.
    (model_name here is just the label you store in paper_embeddings.model_name)
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


def embed_missing_papers(model_name: str = "bge-small-en-v1.5-local", batch_size: int = 64, limit: Optional[int] = None) -> None:
    """
    Embed all papers that don't yet have embeddings for the given model_name
    using a local sentence-transformers model.

    Args:
        model_name:  Label to store in paper_embeddings.model_name
                     (does NOT need to equal HF_MODEL_NAME, but usually should).
        batch_size:  Number of texts per model.encode call.
        limit:       Optional upper bound on how many papers to embed (for testing).
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    log.info(f"Selecting papers without embeddings for model label '{model_name}'...")
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
            continue
        paper_ids.append(paper["id"])
        texts.append(text)

    if not texts:
        log.warn(
            "No usable text found in selected papers (all empty?). "
            "Nothing to embed."
        )
        cur.close()
        conn.close()
        return

    log.info(f"Actually embedding {len(texts)} papers (with non-empty text).")

    # We'll embed in batches and insert per batch
    progress = create_progress_bar(
        total=len(texts),
        desc="Embedding papers",
        unit="papers",
    )

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        batch_ids = paper_ids[i : i + batch_size]

        try:
            batch_vecs = embed_texts_local(batch_texts, batch_size=len(batch_texts))
        except Exception as e:
            log.error(f"Embedding batch starting at index {i} FAILED: {e}")
            # Skip this batch but continue with the rest
            progress.update(len(batch_ids))
            continue

        if len(batch_vecs) != len(batch_ids):
            log.error(
                f"Batch size mismatch at index {i}: "
                f"{len(batch_vecs)} embeddings vs {len(batch_ids)} paper_ids"
            )
            progress.update(len(batch_ids))
            continue

        _insert_embeddings_batch(cur, model_name, batch_ids, batch_vecs)
        conn.commit()
        progress.update(len(batch_ids))

    progress.close()

    log.success(
        f"Embedded {len(texts)} papers with model label '{model_name}' "
        f"using local model '{HF_MODEL_NAME}' on device '{DEVICE}'."
    )
    cur.close()
    conn.close()
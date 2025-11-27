# server/semantic/embeddings.py

from typing import List, Optional, Callable, Any

import time

from colorama import Fore
from psycopg2.extras import RealDictCursor

from server.globals import SEMANTIC_SEARCH_MODEL, SEMANTIC_SEARCH_MODEL_NAME, DEVICE
from server.database.db_utils import get_conn
from server.logger import ColorLogger
from server.utils.progress import create_progress_bar

log = ColorLogger("EMBED", Fore.MAGENTA, include_timestamps=True, include_threading_id=False)


# ======================= text builders =======================

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

    if title: parts.append(title)
    if abstract: parts.append(abstract)
    if conclusion: parts.append("Conclusion: " + conclusion)

    return "\n\n".join(parts) or None


def _build_concept_text(row: dict) -> Optional[str]:
    """
    Build the text representation of a concept for embedding.

    Currently: name + description (if present).
    Returns None if there's effectively nothing to embed.
    """
    text_parts: List[str] = []
    name = row.get("name", "")
    description = row.get("description")

    if name: text_parts.append(name)
    if description: text_parts.append(description)

    text = "\n\n".join(text_parts).strip()
    return text or None


def _select_concepts_needing_embeddings(cur, limit: Optional[int] = None):
    """
    Select concepts that don't yet have an embedding for SEMANTIC_SEARCH_MODEL_NAME.
    """
    params = [SEMANTIC_SEARCH_MODEL_NAME]
    sql = """
        SELECT c.id, c.name, c.description
        FROM concepts c
        LEFT JOIN concept_embeddings e
            ON e.concept_id = c.id
           AND e.model_name = %s
        WHERE e.concept_id IS NULL
        ORDER BY c.id
    """
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    cur.execute(sql, params)
    return cur.fetchall()


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


def _insert_embeddings_batch(cur, type: str, ids: List[Any], embeddings: List[List[float]]) -> None:
    """
    Insert (or upsert) a batch of embeddings into {type}_embeddings.
    """
    for pid, vec in zip(ids, embeddings):
        cur.execute(
            f"""
            INSERT INTO {type}_embeddings ({type}_id, embedding_vec, model_name)
            VALUES (%s, %s, %s)
            ON CONFLICT ({type}_id, model_name) DO UPDATE
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

    for i in range(0, len(texts), batch_size):
        batch = texts[i: i + batch_size]

        for attempt in range(3):
            try:
                vecs = SEMANTIC_SEARCH_MODEL.encode(
                    batch, batch_size=len(batch), show_progress_bar=False,
                    convert_to_numpy=True, normalize_embeddings=True,
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


def _embed_missing_entities(
    *, entity_label: str, unit_label: str, embed_type: str, select_fn: Callable[[Any, Optional[int]], List[dict]],
    text_builder: Callable[[dict], Optional[str]], batch_size: int, limit: Optional[int]
) -> None:
    """
    Generic implementation for embedding "missing" entities (papers/concepts/...).
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    log.info(f"Selecting {entity_label} without embeddings for model label '{SEMANTIC_SEARCH_MODEL_NAME}'...")
    rows = select_fn(cur, limit=limit)

    if not rows:
        log.info(f"No {entity_label} need embeddings; everything is up to date.")
        cur.close()
        conn.close()
        return

    log.info(f"Found {len(rows)} {entity_label} to embed.")

    ids: List[Any] = []
    texts: List[str] = []

    for row in rows:
        text = text_builder(row)
        if text is None:
            continue
        ids.append(row["id"])
        texts.append(text)

    if not texts:
        log.warn(f"No usable text found in selected {entity_label} (all empty?). Nothing to embed.")
        cur.close()
        conn.close()
        return

    log.info(
        f"Actually embedding {len(texts)} {entity_label} (with non-empty text) "
        f"using '{SEMANTIC_SEARCH_MODEL_NAME}' on device '{DEVICE}'."
    )

    progress = create_progress_bar(total=len(texts), desc=f"Embedding {unit_label}", unit=unit_label)

    # Main embedding loop; we embed in batches and write each batch to DB
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i: i + batch_size]
        batch_ids = ids[i: i + batch_size]

        try:
            batch_vecs = embed_texts_local(batch_texts, batch_size=len(batch_texts))
        except Exception as e:
            log.error(f"Embedding batch starting at index {i} FAILED: {e}")
            progress.update(len(batch_ids))
            continue

        if len(batch_vecs) != len(batch_ids):
            log.error(f"Batch size mismatch at index {i}: {len(batch_vecs)} embeddings vs {len(batch_ids)} ids")
            progress.update(len(batch_ids))
            continue

        _insert_embeddings_batch(cur, embed_type, batch_ids, batch_vecs)
        conn.commit()
        progress.update(len(batch_ids))

    progress.close()

    log.success(f"Embedded {len(texts)} {entity_label} using local model '{SEMANTIC_SEARCH_MODEL_NAME}' on device '{DEVICE}'.")
    cur.close()
    conn.close()


def embed_missing_concepts(batch_size: int = 64, limit: Optional[int] = None) -> None:
    _embed_missing_entities(
        entity_label="concepts", unit_label="concepts", embed_type="concept",
        select_fn=_select_concepts_needing_embeddings, text_builder=_build_concept_text,
        batch_size=batch_size, limit=limit,
    )


def embed_missing_papers(batch_size: int = 64, limit: Optional[int] = None) -> None:
    _embed_missing_entities(
        entity_label="papers", unit_label="papers", embed_type="paper",
        select_fn=_select_papers_needing_embeddings, text_builder=_build_paper_text,
        batch_size=batch_size, limit=limit,
    )
# server/semantic/search.py

from typing import List, Dict, Any

from psycopg2.extras import RealDictCursor

from server.globals import SEMANTIC_SEARCH_MODEL, SEMANTIC_SEARCH_MODEL_NAME
from server.database.db_utils import get_conn
from server.semantic.auto_index import ensure_paper_embedding_index
from server.logger import ColorLogger
from colorama import Fore

log = ColorLogger("SEARCH", tag_color=Fore.CYAN, include_timestamps=False)

# Cache (lists, probes) so we don't re-run auto-tuning for every search
_INDEX_TUNING: Dict[str, Any] = {
    "lists": None,
    "probes": None,
    "initialized": False,
}


def _ensure_index_once():
    """
    Run auto-index tuning only once per process and cache the result.
    """
    if _INDEX_TUNING["initialized"]:
        return

    conn = get_conn()
    try:
        lists, probes = ensure_paper_embedding_index(conn, dry_run=False)
        _INDEX_TUNING["lists"] = lists
        _INDEX_TUNING["probes"] = probes
        _INDEX_TUNING["initialized"] = True
        log.info(f"Semantic search index tuned: lists={lists}, probes={probes}.")
    finally:
        conn.close()


def _cosine_distance_to_score(distance: float) -> float:
    """
    pgvector <-> returns an L2 distance by default with vector_l2_ops.
    You can convert it to a 'similarity' score in [0, 1] for display.
    Very rough heuristic: score = 1 / (1 + distance)
    """
    return 1.0 / (1.0 + float(distance))


def search_papers(query: str, top_k: int = 10) -> List[Dict[str, Any]]:
    """
    Semantic search over papers using pgvector + IVFFLAT.

    Steps:
      1. Ensure index is tuned (only once per process).
      2. Embed query with SEMANTIC_SEARCH_MODEL.
      3. Use Postgres/pgvector to run ANN search:
         ORDER BY embedding_vec <-> query_vector LIMIT top_k.
    """
    _ensure_index_once()

    # 1) Embed query
    q_vec = SEMANTIC_SEARCH_MODEL.encode([query], convert_to_numpy=True, normalize_embeddings=True, show_progress_bar=False)[0]
    q_vec_list = q_vec.tolist()

    # 2) Query DB using pgvector ANN
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    lists = _INDEX_TUNING.get("lists") or 100
    probes = _INDEX_TUNING.get("probes") or 10

    # Set probes for this session/transaction
    # (LOCAL ensures it doesn't leak into other sessions)
    cur.execute("SET LOCAL ivfflat.probes = %s;", (probes,))

    cur.execute(
        """
        SELECT
            p.id,
            p.title,
            p.abstract,
            p.external_ids,
            e.embedding_vec <-> %s::vector AS distance
        FROM paper_embeddings e
        JOIN papers p ON p.id = e.paper_id
        WHERE e.embedding_vec IS NOT NULL
        AND e.model_name = %s
        ORDER BY e.embedding_vec <-> %s::vector
        LIMIT %s;
        """,
        (q_vec_list, SEMANTIC_SEARCH_MODEL_NAME, q_vec_list, top_k),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    results: List[Dict[str, Any]] = []
    for r in rows:
        dist = float(r["distance"])
        score = _cosine_distance_to_score(dist)
        results.append(
            {
                "paper_id": r["id"],
                "title": r["title"],
                "abstract": r["abstract"],
                "external_ids": r["external_ids"],
                "distance": dist,
                "score": score,
            }
        )

    log.info(f"Search '{query}' → {len(results)} results (top_k={top_k}, probes={probes}, lists={lists}).")
    return results
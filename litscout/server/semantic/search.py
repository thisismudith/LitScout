# litscout/server/semantic/search.py

from typing import List, Dict, Any, Tuple, Set

from psycopg2.extras import RealDictCursor

from server.globals import SEMANTIC_SEARCH_MODEL, SEMANTIC_SEARCH_MODEL_NAME
from server.database.db_utils import get_conn
from server.semantic.auto_index import (
    ensure_paper_embedding_index,
    ensure_concept_embedding_index,
)
from server.logger import ColorLogger
from colorama import Fore

log = ColorLogger("SEARCH", tag_color=Fore.CYAN, include_timestamps=False)

# Cache (lists, probes) so we don't re-run auto-tuning for every search
_INDEX_TUNING: Dict[str, Dict[str, Any]] = {
    "paper": {"lists": None, "probes": None, "initialized": False},
    "concept": {"lists": None, "probes": None, "initialized": False},
}


def _ensure_index_once(index_type: str) -> None:
    """
    Run auto-index tuning only once per process and cache the result.

    index_type: "paper" | "concept"
    """
    state = _INDEX_TUNING[index_type]
    if state["initialized"]:
        return

    conn = get_conn()
    try:
        if index_type == "paper":
            lists, probes = ensure_paper_embedding_index(conn, dry_run=False)
        elif index_type == "concept":
            lists, probes = ensure_concept_embedding_index(conn, dry_run=False)
        else:
            raise ValueError(f"Unknown index_type={index_type}")

        state["lists"] = lists
        state["probes"] = probes
        state["initialized"] = True
        log.info(f"{index_type.capitalize()} semantic index tuned: lists={lists}, probes={probes}.")
    finally:
        conn.close()


def _get_index_params(index_type: str) -> Tuple[int, int]:
    """
    Return (lists, probes) for the given index type, with sensible defaults.
    """
    state = _INDEX_TUNING.get(index_type, {})
    lists = state.get("lists") or 100
    probes = state.get("probes") or 10
    return lists, probes


def _embed_query(query: str) -> List[float]:
    """
    Embed a single query string using the global SEMANTIC_SEARCH_MODEL.
    """
    vec = SEMANTIC_SEARCH_MODEL.encode([query], convert_to_numpy=True, normalize_embeddings=True, show_progress_bar=False)[0]
    return vec.tolist()


def _cosine_distance_to_score(distance: float) -> float:
    """
    pgvector <-> returns an L2 distance by default with vector_l2_ops.
    Convert it to a 'similarity' score in [0, 1] for display.
    Very rough heuristic: score = 1 / (1 + distance)
    """
    return 1.0 / (1.0 + float(distance))


def search_papers(query: str, limit: int = 10, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Semantic search over papers using pgvector + IVFFLAT. Follows pagination.
    """
    _ensure_index_once("paper")

    # 1) Embed query
    q_vec_list = _embed_query(query)

    # 2) Query DB using pgvector ANN
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    lists, probes = _get_index_params("paper")

    # Set probes for this session/transaction
    cur.execute("SET LOCAL ivfflat.probes = %s;", (probes,))

    cur.execute(
        """
        SELECT
            p.id,
            p.title,
            p.abstract,
            p.external_ids,
            p.source_id,
            e.embedding_vec <-> %s::vector AS distance
        FROM paper_embeddings e
        JOIN papers p ON p.id = e.paper_id
        WHERE e.embedding_vec IS NOT NULL
          AND e.model_name = %s
        ORDER BY e.embedding_vec <-> %s::vector
        LIMIT %s
        OFFSET %s;
        """,
        (q_vec_list, SEMANTIC_SEARCH_MODEL_NAME, q_vec_list, limit, offset),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    results: List[Dict[str, Any]] = []
    for r in rows:
        dist = float(r["distance"])
        sim = _cosine_distance_to_score(dist)
        results.append(
            {
                "paper_id": r["id"],
                "title": r["title"],
                "abstract": r["abstract"],
                "external_ids": r["external_ids"],
                "source_id": r["source_id"],
                "distance": dist,
                "similarity": sim,
            }
        )

    log.info(
        f"[papers] Search '{query}' → {len(results)} results "
        f"(limit={limit}, offset={offset}, probes={probes}, lists={lists})."
    )
    return results


def search_concepts(query: str, top_k: int = 10) -> List[Dict[str, Any]]:
    """
    Semantic search over concepts using concept_embeddings.

    Returns a list:
        [
          {
            "concept_id": str,
            "name": str,
            "distance": float,
            "similarity": float,
          },
          ...
        ]
    """
    _ensure_index_once("concept")

    q_vec_list = _embed_query(query)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    lists, probes = _get_index_params("concept")
    cur.execute("SET LOCAL ivfflat.probes = %s;", (probes,))

    cur.execute(
        """
        SELECT
            ce.concept_id,
            c.name,
            c.description,
            ce.embedding_vec <-> %s::vector AS distance
        FROM concept_embeddings ce
        JOIN concepts c ON c.id = ce.concept_id
        WHERE ce.embedding_vec IS NOT NULL
          AND ce.model_name = %s
        ORDER BY ce.embedding_vec <-> %s::vector
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
        sim = _cosine_distance_to_score(dist)
        results.append(
            {
                "concept_id": r["concept_id"],
                "name": r["name"],
                "description": r["description"],
                "distance": dist,
                "similarity": sim,
            }
        )

    log.info(
        f"Search '{query}' → {len(results)} concepts "
        f"(top_k={top_k}, probes={probes}, lists={lists})."
    )
    return results


def search_papers_via_concepts(
    query: str, top_k_concepts: int = 10, top_k_papers_per_concept: int = 10,
    limit: int = 10, offset: int = 0
) -> Dict[str, Any]:
    """
    Concept-driven semantic search.

    1. Use search_concepts() to get top_k_concepts concepts for the query.
    2. For each of those concepts, find up to top_k_papers_per_concept papers
       where that concept appears (papers.concepts JSONB contains concept_id).
    3. For each (concept, paper):

           matching_score_for_concept =
               similarity(query, concept) * concept_score_in_paper

       where concept_score_in_paper comes from papers.concepts[concept_id].

    4. For each paper, compute:

           total_score = (sum of matching_score_for_concept over the
                          top_k_concepts) / top_k_concepts
    
    Follows pagination.
    """
    # Step 1: get top concepts
    concepts = search_concepts(query, top_k=top_k_concepts)
    if not concepts:
        log.info(f"Search '{query}' → 0 concepts, skipping paper join.")
        return {
            "concepts": [],
            "papers": [],
            "limit": limit,
            "offset": offset,
            "total_papers": 0,
        }

    concept_ids = [c["concept_id"] for c in concepts]
    concept_sims = [c["similarity"] for c in concepts]

    # Step 2: join those concepts against papers.concepts JSONB
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        WITH concept_params AS (
            SELECT
                unnest(%s::text[]) AS concept_id,
                unnest(%s::double precision[]) AS similarity
        ),
        paper_concepts AS (
            SELECT
                cp.concept_id,
                cp.similarity AS concept_similarity,
                p.id AS paper_id,
                p.title,
                p.abstract,
                p.external_ids,
                p.source_id,
                (p.concepts::jsonb -> cp.concept_id ->> 'score')::float AS concept_score_in_paper,
                cp.similarity * (p.concepts::jsonb -> cp.concept_id ->> 'score')::float AS matching_score
            FROM papers p
            JOIN concept_params cp
              ON p.concepts::jsonb ? cp.concept_id
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY concept_id
                    ORDER BY matching_score DESC
                ) AS rn
            FROM paper_concepts
        )
        SELECT
            concept_id,
            concept_similarity,
            paper_id,
            title,
            abstract,
            external_ids,
            source_id,
            concept_score_in_paper,
            matching_score
        FROM ranked
        WHERE rn <= %s
        ORDER BY concept_id, matching_score DESC;
        """,
        (
            concept_ids,
            concept_sims,
            top_k_papers_per_concept,
        ),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Prepare concept map starting from base concept info
    concepts_map: Dict[str, Dict[str, Any]] = {
        c["concept_id"]: {
            "concept_id": c["concept_id"],
            "name": c["name"],
            "description": c["description"],
            "distance": c["distance"],
            "similarity": c["similarity"],
            "papers": [],
        }
        for c in concepts
    }

    # Aggregate scores by paper
    paper_scores: Dict[int, Dict[str, Any]] = {}

    for r in rows:
        concept_id = r["concept_id"]
        paper_id = int(r["paper_id"])

        # Per-concept paper list
        paper_entry = {
            "paper_id": paper_id,
            "title": r["title"],
            "abstract": r["abstract"],
            "external_ids": r["external_ids"],
            "source_id": r["source_id"],
            "concept_score_in_paper": float(r["concept_score_in_paper"]),
            "matching_score": float(r["matching_score"]),
        }
        if concept_id in concepts_map:
            concepts_map[concept_id]["papers"].append(paper_entry)

        # Aggregate per paper across concepts
        if paper_id not in paper_scores:
            paper_scores[paper_id] = {
                "paper_id": paper_id,
                "title": r["title"],
                "abstract": r["abstract"],
                "external_ids": r["external_ids"],
                "source_id": r["source_id"],
                "score_sum": 0.0,
                "matched_concepts_count": 0,
            }
        paper_scores[paper_id]["score_sum"] += float(r["matching_score"])
        paper_scores[paper_id]["matched_concepts_count"] += 1

    # Compute final total_score = (sum of matching_score) / top_k_concepts
    all_papers: List[Dict[str, Any]] = []
    if top_k_concepts > 0:
        for p in paper_scores.values():
            # matched = p["matched_concepts_count"]
            # if matched > 0:
            #     total_score = p["score_sum"] / float(matched)
            # else:
            #     total_score = 0.0
            total_score = p["score_sum"] / float(top_k_concepts)

            all_papers.append(
                {
                    "paper_id": p["paper_id"],
                    "title": p["title"],
                    "abstract": p["abstract"],
                    "external_ids": p["external_ids"],
                    "source_id": p["source_id"],
                    "total_score": total_score,
                }
            )
        all_papers.sort(key=lambda x: x["total_score"], reverse=True)

    total_papers = len(all_papers)
    start = offset
    end = offset + limit
    paginated_papers = all_papers[start:end]

    concept_list = list(concepts_map.values())
    lists, probes = _get_index_params("concept")

    log.info(
        f"[concepts+pairs] Search '{query}' → "
        f"{len(concept_list)} concepts, {total_papers} total papers, "
        f"returning {len(paginated_papers)} (limit={limit}, offset={offset}, "
        f"top_k_concepts={top_k_concepts}, "
        f"top_k_papers_per_concept={top_k_papers_per_concept}, "
        f"probes={probes}, lists={lists})."
    )

    return {
        "concepts": concept_list,
        "papers": paginated_papers,
        "limit": limit,
        "offset": offset,
        "total_papers": total_papers,
    }


def _compute_missing_concept_scores_for_papers(paper_ids: Set[int], concept_sim_map: Dict[str, float], concepts_count: int) -> Dict[int, float]:
    """
    Computes concept-score for papers missing concept ranking.

    Formula:
        avg( concept_similarity * paper_concept_score )
        over matched concepts.
    """

    if not paper_ids or not concept_sim_map:
        return {}

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT id, concepts
        FROM papers
        WHERE id = ANY(%s);
    """, (list(paper_ids),))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    concepts_by_paper = {int(r["id"]): r["concepts"] or {} for r in rows}

    result = {}

    for pid in paper_ids:
        pc = concepts_by_paper.get(pid, {})
        score_sum = 0.0
        # matched = 0

        for concept_id, similarity in concept_sim_map.items():
            raw_obj = pc.get(concept_id)

            if not isinstance(raw_obj, dict):
                continue

            score_val = raw_obj.get("score")
            if score_val is None:
                continue

            weight = float(score_val)

            score_sum += similarity * weight
            # matched += 1

        # result[pid] = (score_sum / matched) if matched else 0.0
        result[pid] = score_sum / float(concepts_count)

    return result



def _compute_missing_paper_scores_for_papers(query: str, paper_ids: Set[int]) -> Dict[int, float]:
    """
    For papers that are missing a direct semantic paper_score, compute it using
    the pgvector distance between query embedding and paper_embeddings.embedding_vec.
    """
    if not paper_ids:
        return {}

    q_vec_list = _embed_query(query)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT
            paper_id,
            embedding_vec <-> %s::vector AS distance
        FROM paper_embeddings
        WHERE model_name = %s
          AND embedding_vec IS NOT NULL
          AND paper_id = ANY(%s);
        """,
        (q_vec_list, SEMANTIC_SEARCH_MODEL_NAME, list(paper_ids)),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    result: Dict[int, float] = {}
    for r in rows:
        pid = int(r["paper_id"])
        dist = float(r["distance"])
        result[pid] = _cosine_distance_to_score(dist)

    # If some IDs have no embedding row, default to 0.0
    for pid in paper_ids:
        if pid not in result:
            result[pid] = 0.0

    return result


def search_papers_hybrid(
    query: str, limit: int = 10, offset: int = 0,
    *, paper_weight: float = 0.6, concept_weight: float = 0.4,
    top_k_concepts: int = 10, top_k_papers_per_concept: int = 10
) -> Dict[str, Any]:
    """
    Hybrid search that combines:
      - direct semantic paper search (search_papers)
      - concept-driven search (search_papers_via_concepts)

    For a given query:
      1. Fetch the top (offset + limit) papers from search_papers (no offset).
      2. Fetch the top (offset + limit) papers from search_papers_via_concepts (no offset),
         along with the top concepts and their similarity scores.
      3. For each unique paper_id in the union:
           - Ensure we have a paper_score:
               * from search_papers, or
               * computed via _compute_missing_paper_scores_for_papers
           - Ensure we have a concept_score:
               * from search_papers_via_concepts, or
               * computed via _compute_missing_concept_scores_for_papers using
                 the paper's concepts JSON and the top concepts {id: similarity}.

      4. Compute combined_score:

           combined_score =
               paper_weight   * paper_score
             + concept_weight * concept_score

      5. Sort all unique papers by combined_score (desc),
         then return results[offset : offset + limit].

    Follows pagination.
    """
    # We need enough candidates from each source to fill the requested window
    base_limit = offset + limit
    if base_limit <= 0:
        base_limit = limit

    # 1) direct semantic paper search (top base_limit from rank 1)
    paper_results = search_papers(query, limit=base_limit, offset=0)

    # 2) concept-driven search (top base_limit from rank 1)
    concept_results = search_papers_via_concepts(
        query=query, top_k_concepts=top_k_concepts, top_k_papers_per_concept=top_k_papers_per_concept,
        limit=base_limit, offset=0,
    )
    concept_papers = concept_results["papers"]
    concept_list = concept_results["concepts"]

    # Build maps
    paper_map: Dict[int, Dict[str, Any]] = {p["paper_id"]: p for p in paper_results}
    concept_map: Dict[int, Dict[str, Any]] = {p["paper_id"]: p for p in concept_papers}
    concept_sim_map: Dict[str, float] = {c["concept_id"]: float(c["similarity"]) for c in concept_list}

    # Collect all unique paper_ids
    all_ids: Set[int] = set(paper_map.keys()) | set(concept_map.keys())

    # Determine which IDs are missing scores
    missing_concept_ids: Set[int] = {pid for pid in all_ids if pid not in concept_map}
    missing_paper_ids: Set[int] = {pid for pid in all_ids if pid not in paper_map}

    # Compute missing concept scores using paper.concepts JSON and concept_sim_map
    missing_concept_scores = _compute_missing_concept_scores_for_papers(missing_concept_ids, concept_sim_map, concepts_count=top_k_concepts)

    # Compute missing paper scores using paper_embeddings
    missing_paper_scores = _compute_missing_paper_scores_for_papers(query, missing_paper_ids)

    combined_list: List[Dict[str, Any]] = []
    for pid in all_ids:
        p_info = paper_map.get(pid)
        c_info = concept_map.get(pid)

        # paper_score:
        if p_info is not None:
            paper_score = float(p_info["similarity"])
        else:
            paper_score = missing_paper_scores.get(pid, 0.0)

        # concept_score:
        if c_info is not None:
            concept_score = float(c_info["total_score"])
        else:
            concept_score = missing_concept_scores.get(pid, 0.0)

        combined_score = paper_weight * paper_score + concept_weight * concept_score

        # Prefer metadata from direct paper search if available, else from concept search
        meta_source = p_info or c_info
        combined_list.append(
            {
                "paper_id": pid,
                "title": meta_source["title"],
                "abstract": meta_source["abstract"],
                "external_ids": meta_source["external_ids"],
                "source_id": meta_source["source_id"],
                "paper_score": paper_score,
                "concept_score": concept_score,
                "combined_score": combined_score,
            }
        )

    # Sort by combined_score desc
    combined_list.sort(key=lambda x: x["combined_score"], reverse=True)

    total_papers = len(combined_list)
    start = offset
    end = offset + limit
    paginated = combined_list[start:end]

    log.info(
        f"[hybrid] Search '{query}' → {total_papers} unique papers, "
        f"returning {len(paginated)} (limit={limit}, offset={offset}, "
        f"paper_weight={paper_weight}, concept_weight={concept_weight}, "
        f"top_k_concepts={top_k_concepts}, "
        f"top_k_papers_per_concept={top_k_papers_per_concept})."
    )

    return {
        "papers": paginated,
        "limit": limit,
        "offset": offset,
        "total_papers": total_papers,
    }

def search_sources_from_papers(
    query: str, *, limit: int, offset: int, paper_weight: float = 0.6, concept_weight: float = 0.4,
    top_k_concepts: int = 10, top_k_papers_per_concept: int = 10
) -> Dict[str, Any]:
    """
    Hybrid search that combines:
      - direct semantic paper search (search_papers)
      - concept-driven search (search_papers_via_concepts)

    Aggregates results at the source level.

    See search_papers_hybrid() for detailed description of the hybrid approach.
    """
    hybrid_results = search_papers_hybrid(
        query=query, limit=1000000000, offset=0, paper_weight=paper_weight,
        concept_weight=concept_weight, top_k_concepts=top_k_concepts, top_k_papers_per_concept=top_k_papers_per_concept
    )

    # Aggregate by source_id
    source_map: Dict[str, Dict[str, Any]] = {}

    for p in hybrid_results["papers"]:
        source_id = p.get("source_id")
        if not source_id:
            continue

        if source_id not in source_map:
            source_map[source_id] = {
                "source_id": source_id,
                "papers": [],
                "aggregate_score": 0.0,
            }

        source_entry = source_map[source_id]
        if str(p["paper_id"]) not in source_entry["papers"]:
            source_entry["papers"].append(str(p["paper_id"]))
            source_entry["aggregate_score"] += p["combined_score"]

    # Convert to list and sort by aggregate_score desc
    source_list = list(source_map.values())
    source_list.sort(key=lambda x: x["aggregate_score"], reverse=True)

    total_sources = len(source_list)
    start = offset
    end = offset + limit
    paginated_sources = source_list[start:end]

    log.info(
        f"Search yielded {total_sources} unique sources, "
        f"returning {len(paginated_sources)} (limit={limit}, offset={offset})."
    )

    return {
        "sources": paginated_sources,
        "limit": limit,
        "offset": offset,
        "total_sources": total_sources,
    }
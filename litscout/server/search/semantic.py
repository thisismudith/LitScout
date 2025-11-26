# server/search/semantic.py

"""
Semantic search for research papers using embeddings.

This module provides functionality to search papers by semantic similarity
using pre-computed embeddings stored in the database.
"""

from dataclasses import dataclass
from typing import List, Optional
import math

from colorama import Fore
from psycopg2.extras import RealDictCursor

from server.embeddings.papers import embed_texts_local, HF_MODEL_NAME
from server.ingestion.db_writer import get_conn
from server.logger import ColorLogger

log = ColorLogger("SEARCH", Fore.GREEN, include_timestamps=True, include_threading_id=False)


@dataclass
class SearchResult:
    """Represents a single search result."""
    paper_id: int
    title: str
    abstract: Optional[str]
    year: Optional[int]
    doi: Optional[str]
    score: float  # Cosine similarity score (0-1, higher is better)


def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """
    Compute cosine similarity between two vectors.
    
    Both vectors are assumed to be normalized (as per the embedding model),
    so the cosine similarity is simply their dot product.
    """
    if len(vec1) != len(vec2):
        return 0.0
    
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    
    # Since embeddings are normalized, the dot product equals cosine similarity
    # But let's be safe and normalize if needed
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return dot_product / (norm1 * norm2)


def _embed_query(query: str) -> List[float]:
    """
    Embed a search query using the same model as paper embeddings.
    
    Args:
        query: The search query text.
        
    Returns:
        A normalized embedding vector.
    """
    log.info(f"Embedding query: '{query[:50]}{'...' if len(query) > 50 else ''}'")
    
    # Use the same embedding function as papers
    embeddings = embed_texts_local([query], batch_size=1)
    
    if not embeddings:
        raise ValueError("Failed to generate embedding for query")
    
    return embeddings[0]


def semantic_search(
    query: str,
    model_name: str = "bge-small-en-v1.5-local",
    top_k: int = 10,
    min_score: float = 0.0,
) -> List[SearchResult]:
    """
    Search for papers semantically similar to the given query.
    
    This function:
    1. Embeds the query using the same model as paper embeddings
    2. Retrieves all paper embeddings from the database
    3. Computes cosine similarity between query and each paper
    4. Returns top-k results sorted by similarity
    
    Args:
        query: The search query (natural language description of the topic).
        model_name: The embedding model name to match in the database.
        top_k: Maximum number of results to return.
        min_score: Minimum similarity score threshold (0-1).
        
    Returns:
        List of SearchResult objects sorted by similarity (highest first).
    """
    if not query or not query.strip():
        log.warn("Empty query provided")
        return []
    
    # 1. Embed the query
    query_embedding = _embed_query(query.strip())
    
    # 2. Fetch paper embeddings and metadata from the database
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    log.info(f"Fetching paper embeddings for model '{model_name}'...")
    
    cur.execute(
        """
        SELECT 
            pe.paper_id,
            pe.embedding,
            p.title,
            p.abstract,
            p.year,
            p.doi
        FROM paper_embeddings pe
        JOIN papers p ON pe.paper_id = p.id
        WHERE pe.model_name = %s;
        """,
        (model_name,),
    )
    
    rows = cur.fetchall()
    
    if not rows:
        log.warn(f"No paper embeddings found for model '{model_name}'")
        cur.close()
        conn.close()
        return []
    
    log.info(f"Found {len(rows)} papers with embeddings")
    
    # 3. Compute similarity scores
    results: List[SearchResult] = []
    
    for row in rows:
        paper_embedding = row["embedding"]
        
        # Compute cosine similarity
        score = _cosine_similarity(query_embedding, paper_embedding)
        
        if score >= min_score:
            results.append(SearchResult(
                paper_id=row["paper_id"],
                title=row["title"],
                abstract=row["abstract"],
                year=row["year"],
                doi=row["doi"],
                score=score,
            ))
    
    cur.close()
    conn.close()
    
    # 4. Sort by score (descending) and return top-k
    results.sort(key=lambda r: r.score, reverse=True)
    
    top_results = results[:top_k]
    
    log.success(f"Found {len(top_results)} relevant papers (top {top_k} of {len(results)} above threshold)")
    
    return top_results


def format_search_results(results: List[SearchResult], verbose: bool = False) -> str:
    """
    Format search results for display.
    
    Args:
        results: List of SearchResult objects.
        verbose: If True, include abstracts in output.
        
    Returns:
        Formatted string representation of results.
    """
    if not results:
        return "No results found."
    
    result_word = "result" if len(results) == 1 else "results"
    lines = [f"Found {len(results)} {result_word}:\n"]
    
    for i, result in enumerate(results, 1):
        lines.append(f"{i}. [{result.score:.3f}] {result.title}")
        
        if result.year:
            lines.append(f"   Year: {result.year}")
        
        if result.doi:
            lines.append(f"   DOI: {result.doi}")
        
        if verbose and result.abstract:
            # Truncate abstract for display
            abstract = result.abstract[:300]
            if len(result.abstract) > 300:
                abstract += "..."
            lines.append(f"   Abstract: {abstract}")
        
        lines.append("")  # Empty line between results
    
    return "\n".join(lines)

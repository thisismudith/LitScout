# server/main.py

"""
FastAPI application for LitScout semantic search API.
"""

from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel, Field

from server.search.semantic import semantic_search, SearchResult as InternalSearchResult


app = FastAPI(
    title="LitScout API",
    description="AI-powered semantic search engine for research papers",
    version="0.1.0",
)


class SearchResultResponse(BaseModel):
    """Response model for a single search result."""
    paper_id: int = Field(..., description="Unique identifier of the paper")
    title: str = Field(..., description="Title of the paper")
    abstract: Optional[str] = Field(None, description="Abstract of the paper")
    year: Optional[int] = Field(None, description="Publication year")
    doi: Optional[str] = Field(None, description="DOI identifier")
    score: float = Field(..., description="Similarity score (0-1, higher is better)")


class SearchResponse(BaseModel):
    """Response model for search results."""
    query: str = Field(..., description="The original search query")
    total_results: int = Field(..., description="Number of results returned")
    results: List[SearchResultResponse] = Field(..., description="List of matching papers")


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "message": "LitScout API is running"}


@app.get("/search", response_model=SearchResponse)
async def search_papers(
    q: str = Query(..., min_length=1, description="Search query (natural language description of the topic)"),
    top_k: int = Query(10, ge=1, le=100, description="Maximum number of results to return"),
    min_score: float = Query(0.0, ge=0.0, le=1.0, description="Minimum similarity score threshold"),
    model: str = Query("bge-small-en-v1.5-local", description="Embedding model name to use"),
):
    """
    Search for research papers using semantic similarity.
    
    This endpoint embeds your query using the same model as the paper embeddings
    and returns papers ranked by cosine similarity.
    
    **Example queries:**
    - "machine learning for medical diagnosis"
    - "natural language processing transformers"
    - "computer vision object detection"
    """
    try:
        results = semantic_search(
            query=q,
            model_name=model,
            top_k=top_k,
            min_score=min_score,
        )
        
        response_results = [
            SearchResultResponse(
                paper_id=r.paper_id,
                title=r.title,
                abstract=r.abstract,
                year=r.year,
                doi=r.doi,
                score=r.score,
            )
            for r in results
        ]
        
        return SearchResponse(
            query=q,
            total_results=len(response_results),
            results=response_results,
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy"}

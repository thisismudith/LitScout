# litscout/client/views.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from flask import Blueprint, current_app, render_template, request

main_bp = Blueprint("main", __name__)


def _normalize_paper_results(raw: Any, limit: int) -> Tuple[List[Dict[str, Any]], int | None]:
    """
    Normalize paper results regardless of whether they come from:
      - search_papers (list)
      - search_papers_hybrid (dict with "papers", "total_papers")
    """
    if raw is None:
        return [], None

    if isinstance(raw, dict) and "papers" in raw:
        papers = raw.get("papers", [])
        total = raw.get("total_papers")
        return papers, total

    if isinstance(raw, list):
        # No explicit total known; infer "has next" via len == limit if you want.
        return raw, None

    return [], None


def _normalize_venue_results(raw: Any, limit: int) -> Tuple[List[Dict[str, Any]], int | None]:
    """
    Normalize venue/source results from search_sources_from_papers.
    """
    if raw is None:
        return [], None

    if isinstance(raw, dict) and "sources" in raw:
        sources = raw.get("sources", [])
        total = raw.get("total_sources")
        return sources, total

    if isinstance(raw, list):
        return raw, None

    return [], None


def _derive_authors_from_papers(
    papers: List[Dict[str, Any]],
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Build a simple "top authors" list from paper results.
    Assumes each paper may have an 'authors' field like:
      - list of strings, or
      - list of { 'name': '...' } dicts.

    This is heuristic and will gracefully degrade if authors are missing.
    """
    from collections import Counter

    counter = Counter()

    for p in papers:
        authors = p.get("authors") or []
        # Normalize to list of names
        if isinstance(authors, list):
            for a in authors:
                if isinstance(a, str):
                    name = a.strip()
                elif isinstance(a, dict):
                    name = (a.get("name") or "").strip()
                else:
                    name = ""
                if name:
                    counter[name] += 1

    # Build sorted list
    all_authors = [
        {"name": name, "paper_count": count}
        for name, count in counter.most_common()
    ]
    total = len(all_authors)

    start = offset
    end = offset + limit
    paginated = all_authors[start:end]

    return paginated, total


@main_bp.route("/", methods=["GET"])
def index():
    """
    Main search UI.
    """
    api = current_app.litscout_api

    # Query params
    q = request.args.get("q", "").strip()
    mode = request.args.get("mode", "query").lower()  # "query" or "upload" (upload handled via POST->redirect)
    search_type = request.args.get("search_type", "hybrid").lower()  # papers | concepts | hybrid

    # Pagination params (each panel independent)
    paper_page = max(int(request.args.get("paper_page", 1) or 1), 1)
    author_page = max(int(request.args.get("author_page", 1) or 1), 1)
    venue_page = max(int(request.args.get("venue_page", 1) or 1), 1)

    limit = 10
    paper_offset = (paper_page - 1) * limit
    author_offset = (author_page - 1) * limit
    venue_offset = (venue_page - 1) * limit

    # Weights (for hybrid / venue searches)
    try:
        paper_weight = float(request.args.get("paper_weight", 0.8))
    except ValueError:
        paper_weight = 0.8

    try:
        concept_weight = float(request.args.get("concept_weight", 0.2))
    except ValueError:
        concept_weight = 0.2

    # Normalise weights to sum = 1.0
    if paper_weight + concept_weight != 1.0:
        if paper_weight == 0.4:
            paper_weight = 1.0 - concept_weight
        else:
            concept_weight = 1.0 - paper_weight

    paper_results: List[Dict[str, Any]] = []
    paper_total: int | None = None

    concept_results: List[Dict[str, Any]] = []
    concept_total: int | None = None

    venue_results: List[Dict[str, Any]] = []
    venue_total: int | None = None

    authors_results: List[Dict[str, Any]] = []
    authors_total: int = 0

    if q:
        # ------------------------------------------------------------------
        # 1) Papers / Concepts tile (large left panel)
        # ------------------------------------------------------------------
        if search_type == "concepts":
            # Show concept results in main tile
            concepts_raw = api.search(
                query=q,
                type="concepts",
                limit=limit,
                offset=paper_offset,
            )
            # For concepts, api.search should return a list
            if isinstance(concepts_raw, list):
                concept_results = concepts_raw
                concept_total = None  # unknown; could infer via next-page presence
        else:
            # "papers" or "hybrid"
            papers_raw = api.search(
                query=q,
                type=search_type,
                limit=limit,
                offset=paper_offset,
                concepts_limit=10,
                paper_weight=paper_weight,
                concept_weight=concept_weight,
            )
            paper_results, paper_total = _normalize_paper_results(papers_raw, limit)

            # ------------------------------------------------------------------
            # 2) Authors tile (derived from paper_results)
            # ------------------------------------------------------------------
            authors_results, authors_total = _derive_authors_from_papers(
                paper_results, limit=limit, offset=author_offset
            )

        # ----------------------------------------------------------------------
        # 3) Venues tile (always uses venue search)
        # ----------------------------------------------------------------------
        venues_raw = api.search(
            query=q,
            type="venue",
            limit=limit,
            offset=venue_offset,
            concepts_limit=10,
            paper_weight=paper_weight,
            concept_weight=concept_weight,
        )
        venue_results, venue_total = _normalize_venue_results(venues_raw, limit)

    return render_template(
        "index.html",
        query=q,
        mode=mode,
        search_type=search_type,
        paper_weight=paper_weight,
        concept_weight=concept_weight,
        # results
        paper_results=paper_results,
        paper_total=paper_total,
        concept_results=concept_results,
        concept_total=concept_total,
        venue_results=venue_results,
        venue_total=venue_total,
        authors_results=authors_results,
        authors_total=authors_total,
        # pagination
        paper_page=paper_page,
        author_page=author_page,
        venue_page=venue_page,
        limit=limit,
    )


@main_bp.route("/upload", methods=["POST"])
def upload_and_search():
    """
    Handle 'upload research paper' mode:
      - reads uploaded text file (or any text-ish content)
      - uses its content as query string
      - redirects to main index with mode=upload & q=extracted_text
    """
    file = request.files.get("paper_file")
    if not file or file.filename == "":
        # Just redirect back with no query
        return render_template(
            "index.html",
            query="",
            mode="upload",
            search_type="hybrid",
            paper_results=[],
            paper_total=None,
            concept_results=[],
            concept_total=None,
            venue_results=[],
            venue_total=None,
            authors_results=[],
            authors_total=0,
            paper_page=1,
            author_page=1,
            venue_page=1,
            limit=10,
        )

    # Simple text extraction (assumes text or decodable content)
    raw_bytes = file.read()
    try:
        content = raw_bytes.decode("utf-8", errors="ignore")
    except Exception:
        content = ""

    # Truncate extremely long uploads to keep things fast
    content = content[:8000]

    from urllib.parse import urlencode
    params = {
        "q": content,
        "mode": "upload",
        "search_type": "hybrid",
        "paper_page": 1,
        "author_page": 1,
        "venue_page": 1,
    }
    url = f"/?{urlencode(params)}"
    from flask import redirect

    return redirect(url)
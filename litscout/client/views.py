# server/webapp/views.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple
from PyPDF2 import PdfReader

from flask import (
    Blueprint,
    current_app,
    render_template,
    request,
    jsonify,
)

from psycopg2.extras import RealDictCursor
from server.database.db_utils import get_conn

main_bp = Blueprint("main", __name__)


def _normalize_paper_results(raw: Any) -> Tuple[List[Dict[str, Any]], int | None]:
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
        return raw, None

    return [], None


def _normalize_venue_results(raw: Any) -> Tuple[List[Dict[str, Any]], int | None]:
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


@main_bp.route("/", methods=["GET"])
def index():
    """
    Main UI shell. Results are fetched via JS from /api/search/*.
    """
    return render_template("index.html")


# -------------------------- API: PAPERS --------------------------


@main_bp.post("/api/search/papers")
def api_search_papers():
    """
    POST JSON:
    {
      "query": "...",
      "search_type": "hybrid" | "papers",
      "limit": 10,
      "offset": 0,
      "paper_weight": 0.8,
      "concept_weight": 0.2
    }
    """
    api = current_app.litscout_api

    data = request.get_json(force=True) or {}

    q = (data.get("query") or "").strip()
    search_type = (data.get("search_type") or "hybrid").lower()
    limit = int(data.get("limit") or 10)
    offset = int(data.get("offset") or 0)

    paper_weight = float(data.get("paper_weight") or 0.8)
    concept_weight = float(data.get("concept_weight") or 0.2)

    # Normalize weights to sum to 1.0
    if paper_weight + concept_weight != 1.0:
        if paper_weight == 0.4:
            paper_weight = 1.0 - concept_weight
        else:
            concept_weight = 1.0 - paper_weight

    if not q:
        return jsonify(
            {
                "papers": [],
                "total_papers": 0,
            }
        )

    if search_type == "papers":
        raw = api.search(
            query=q,
            type="papers",
            limit=limit,
            offset=offset,
        )
    else:
        # default → hybrid
        raw = api.search(
            query=q,
            type="hybrid",
            limit=limit,
            offset=offset,
            concepts_limit=10,
            paper_weight=paper_weight,
            concept_weight=concept_weight,
        )

    papers, total = _normalize_paper_results(raw)

    # You may want to ensure each paper has a 'score' field
    # that the UI can turn into XX.YY% match.
    for p in papers:
        if "combined_score" in p and p["combined_score"] is not None:
            p["score"] = float(p["combined_score"])
        elif "similarity" in p and p["similarity"] is not None:
            p["score"] = float(p["similarity"])
        else:
            p["score"] = 0.0

        # Make sure we always have a list of concept names for tags.
        # Expected: top_concepts = ["Machine learning", "Causal inference", ...]
        # If not present, the front-end will degrade gracefully.
        if "top_concepts" not in p and "concepts" in p and isinstance(p["concepts"], list):
            # You can keep it as-is; UI only needs a list of strings.
            pass

    return jsonify(
        {
            "papers": papers,
            "total_papers": total if total is not None else len(papers),
        }
    )


# -------------------------- API: VENUES --------------------------


@main_bp.post("/api/search/venues")
def api_search_venues():
    """
    POST JSON:
    {
      "query": "...",
      "paper_weight": 0.8,
      "concept_weight": 0.2
    }

    Uses the 'venue' search type in LitScoutAPI, then enriches
    each venue with metadata from the 'sources' table.
    """
    api = current_app.litscout_api

    data = request.get_json(force=True) or {}

    q = (data.get("query") or "").strip()

    paper_weight = float(data.get("paper_weight") or 0.8)
    concept_weight = float(data.get("concept_weight") or 0.2)

    # Normalize weights
    if paper_weight + concept_weight != 1.0:
        if paper_weight == 0.4:
            paper_weight = 1.0 - concept_weight
        else:
            concept_weight = 1.0 - paper_weight

    if not q:
        return jsonify(
            {
                "venues": [],
                "total_sources": 0,
            }
        )

    raw = api.search(
        query=q,
        type="venue",
        concepts_limit=10,
        paper_weight=paper_weight,
        concept_weight=concept_weight,
    )

    venues, total = _normalize_venue_results(raw)

    # Enrich from sources table
    source_ids = [f"https://openalex.org/{v.get('source_id')}" for v in venues if v.get("source_id")]
    meta_by_id: Dict[str, Dict[str, Any]] = {}
    if source_ids:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT
                id,
                name,
                host_organization_name,
                homepage_url
            FROM sources
            WHERE id = ANY(%s)
            """,
            (source_ids,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        print(rows[0:5])
        for r in rows:
            sid = r["id"]
            meta_by_id[sid.split("/")[-1]] = {
                "name": r.get("name"),
                "host_organization_name": r.get("host_organization_name"),
                "homepage_url": r.get("homepage_url"),
                "openalex_url": sid,
            }

    # Attach metadata + standardize score field
    for v in venues:
        sid = v.get("source_id")
        meta = meta_by_id.get(sid, {})
        v["name"] = meta.get("name") or sid
        v["host_organization_name"] = meta.get("host_organization_name")
        v["openalex_url"] = meta.get("openalex_url") or (f"https://openalex.org/{sid}" if sid else None)
        # For UI label "Total Score: XXXX.YY"
        v["total_score"] = float(v.get("aggregate_score") or 0.0)

    return jsonify(
        {
            "venues": venues,
            "total_sources": total if total is not None else len(venues),
        }
    )

@main_bp.post("/api/search/authors")
def api_search_authors():
    api = current_app.litscout_api

    data = request.get_json(force=True) or {}
    query = (data.get("query") or "").strip()
    limit = int(data.get("limit") or 10)
    offset = int(data.get("offset") or 0)
    paper_weight = float(data.get("paper_weight") or 0.8)
    concept_weight = float(data.get("concept_weight") or 0.2)
    concepts_limit = int(data.get("concepts_limit") or 10)

    if not query:
        return jsonify(
            {
                "authors": [],
                "limit": limit,
                "offset": offset,
                "total_authors": 0,
            }
        )

    raw = api.search(
        query=query,
        type="author",
        limit=limit,
        offset=offset,
        paper_weight=paper_weight,
        concept_weight=concept_weight,
        concepts_limit=concepts_limit,
    )

    return jsonify(raw)


@main_bp.route("/api/upload_query", methods=["POST"])
def api_upload_query():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    filename = file.filename or ""
    content_type = file.mimetype or ""

    text = ""
    try:
        if filename.lower().endswith(".pdf") or content_type == "application/pdf":
            reader = PdfReader(file)
            for page in reader.pages:
                text += page.extract_text() or ""
        else:
            text = file.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return jsonify({"error": f"Failed to read file: {e}"}), 500

    text = text.strip()
    if not text:
        return jsonify({"error": "No text could be extracted from the file."}), 400

    preview = " ".join(text.split())

    return jsonify({
        "ok": True,
        "query": preview,
    })
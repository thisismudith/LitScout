# server/ingestion/openalex/normalizer.py

from typing import Dict, Any, Optional, List

from server.ingestion.models import (
    NormalizedPaper,
    NormalizedAuthor,
    NormalizedVenue,
    NormalizedVenueInstance,
)

def _reconstruct_abstract(inverted_index: Dict[str, List[int]]) -> str:
    """
    OpenAlex may give abstract as inverted index: {word: [positions...]}.
    This reconstructs it into a plain text abstract.
    """
    if not inverted_index:
        return ""

    max_pos = max(pos for positions in inverted_index.values() for pos in positions)
    tokens: List[Optional[str]] = [None] * (max_pos + 1)

    for word, positions in inverted_index.items():
        for pos in positions:
            tokens[pos] = word

    return " ".join(token for token in tokens if token is not None)


def normalize_openalex_work(work: Dict[str, Any]) -> NormalizedPaper:
    # Title
    raw_title = work.get("title") or work.get("display_name")
    title = raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else "(untitled)"

    host_venue = work.get("host_venue") or {}
    concepts = work.get("concepts") or []
    primary_field = concepts[0]["display_name"] if concepts else None

    # Abstract
    abstract = work.get("abstract")
    if not abstract:
        inv = work.get("abstract_inverted_index")
        if inv:
            abstract = _reconstruct_abstract(inv)

    # Venue normalization
    venue = None
    venue_instance = None
    if host_venue.get("display_name"):
        vt_raw = (host_venue.get("type") or "").lower()
        if vt_raw in ("journal", "book-series"):
            venue_type = "journal"
        elif vt_raw in ("conference", "proceedings"):
            venue_type = "conference"
        else:
            # default guess
            venue_type = "journal"

        venue = NormalizedVenue(
            name=host_venue.get("display_name"),
            short_name=host_venue.get("abbreviated_title"),
            venue_type=venue_type,
            homepage_url=host_venue.get("homepage_url"),
            location=None,
            rank_label=None,
            external_ids={
                "openalex": host_venue.get("id"),
            },
        )

        year = work.get("publication_year")
        venue_instance = NormalizedVenueInstance(venue=venue, year=year)


    # Concepts
    concepts_map: Dict[str, float] = {}
    for c in work.get("concepts", []):
        id = c.get("id").split("/")[-1]
        score = c.get("score")
        if not id or score is None or score <= 0.0:
            continue
        # If same name appears multiple times, keep max score
        prev = concepts_map.get(id, {"score": 0.0})["score"]
        if score > prev:
            concepts_map[id] = {
                "name": c.get("display_name"),
                "level": c.get("level"),
                "score": float(score)
            }

    # Authors
    authorships = work.get("authorships") or []
    authors: List[NormalizedAuthor] = []
    author_order: List[int] = []
    is_corr: List[bool] = []

    for idx, auth in enumerate(authorships, start=1):
        author_obj = auth.get("author") or {}
        name = author_obj.get("display_name")

        if not name:
            continue

        try:
            na = NormalizedAuthor(
                full_name=name,
                works_counted=0,
                cited_by_count=0,
                orcid=author_obj.get("orcid"),
                affiliations=[],
                last_known_institutions=[],
                topics=[],
                topic_shares=[],
                external_ids={
                    "openalex": author_obj.get("id").split("/")[-1],
                },
            )
        except AttributeError:
            # Skip authors with no id
            continue

        authors.append(na)
        author_order.append(idx)
        is_corr.append(False)  # still no explicit “corresponding author” flag
    

    return NormalizedPaper(
        title=title,
        abstract=abstract,
        conclusion=None,
        year=work.get("publication_year"),
        publication_date=work.get("publication_date"),
        doi=work.get("doi"),
        field=primary_field,
        language=work.get("language"),
        venue=venue,
        venue_instance=venue_instance,
        authors=authors,
        author_order=author_order,
        referenced_works=work.get("referenced_works", []),
        related_works=work.get("related_works", []),
        concepts=concepts_map,
        external_ids={"openalex": work.get("id").split("/")[-1]},
        is_corresponding_flags=is_corr,
    )

# for idx, auth in enumerate(authorships, start=1):
#         try:
#             author = _get(f"https://api.openalex.org/{auth.get('author').get('id').split('/')[-1]}")
#         except AttributeError:
#             continue

#         name = author.get("display_name")
#         if not name:
#             continue

#         institutions = author.get("affiliations") or []
#         affiliations = []
#         last_known_institutions = []

#         for institution in institutions:
#             inst = institution.get("institution") or {}
#             affiliations.append({
#                 "name": inst.get("display_name"),
#                 "country_code": inst.get("country_code"),
#                 "type": inst.get("type"),
#                 "id": inst.get("id"),
#                 "years": institution.get("years"),
#             })

#         for last_inst in author.get("last_known_institutions", []):
#             last_known_institutions.append({
#                 "name": last_inst.get("display_name"),
#                 "country_code": last_inst.get("country_code"),
#                 "type": last_inst.get("type"),
#                 "id": last_inst.get("id"),
#             })

#         ids = author.get("ids") or {}

#         na = NormalizedAuthor(
#             full_name=name,
#             affiliations=affiliations,
#             last_known_institutions=last_known_institutions,
#             works_counted=author.get("works_count"),
#             cited_by_count=author.get("cited_by_count"),
#             topic_shares=auth.get("topic_share"),
#             orcid=ids.get("orcid"),
#             external_ids={
#                 "openalex": author.get("id"),
#                 "orcid": ids.get("orcid"),
#                 "scopus": ids.get("scopus"),
#                 "semantic_scholar": ids.get("semantic_scholar"),
#             },
#         )
#         authors.append(na)
#         author_order.append(idx)
#         is_corr.append(False)  # OpenAlex doesn't expose this cleanly
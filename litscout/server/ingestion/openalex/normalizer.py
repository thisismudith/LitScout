# server/ingestion/openalex/normalizer.py

from typing import Dict, Any, Optional, List

from server.ingestion.models import (
    NormalizedPaper,
    NormalizedAuthor,
    NormalizedSource,
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


def _shorten_id(openalex_id: str) -> str:
    """
    Convert 'https://openalex.org/S123456789' -> 'S123456789'.
    """
    if not openalex_id:
        return ""
    return openalex_id.rstrip("/").split("/")[-1]


def normalize_openalex_work(work: Dict[str, Any]) -> NormalizedPaper:
    # Source
    loc = work.get("primary_location") or {}
    src = loc.get("source") or {}

    source_id = _shorten_id(src.get("id"))
    publisher_id = _shorten_id(src.get("host_organization"))

    # Title
    raw_title = work.get("title") or work.get("display_name")
    title = raw_title.strip() if isinstance(raw_title, str) and raw_title.strip() else "(untitled)"

    concepts = work.get("concepts") or []
    primary_field = concepts[0]["display_name"] if concepts else None

    # Abstract
    abstract = work.get("abstract")
    if not abstract:
        inv = work.get("abstract_inverted_index")
        if inv:
            abstract = _reconstruct_abstract(inv)

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
        authors=authors,
        author_order=author_order,
        referenced_works=work.get("referenced_works", []),
        related_works=work.get("related_works", []),
        concepts=concepts_map,
        external_ids={"openalex": work.get("id").split("/")[-1]},
        is_corresponding_flags=is_corr,
        source_id=source_id,
        publisher_id=publisher_id,
    )

def normalize_openalex_source(src: Dict[str, Any]) -> Dict[str, Any]:
    """
    Take a raw OpenAlex Source object and produce a dict matching our DB schema.
    https://docs.openalex.org/api-entities/sources/source-object
    """
    ids = src.get("ids") or {}
    host_org = src.get("host_organization") or {}
    host_org_name = src.get("host_organization_name")

    issn_l = src.get("issn_l")
    issn_list = src.get("issn") or []

    summary_stats = src.get("summary_stats") or {}
    topics = src.get("topics") or src.get("x_concepts") or []
    counts_by_year = src.get("counts_by_year") or []

    is_oa = src.get("is_oa")
    is_in_doaj = src.get("is_in_doaj")
    homepage_url = src.get("homepage_url")

    created_date = src.get("created_date")
    updated_date = src.get("updated_date")

    return NormalizedSource(
        id=src.get("id"),
        name=src.get("display_name"),
        source_type=src.get("type"),
        host_organization_id=host_org or ids.get("publisher"),
        host_organization_name=host_org_name,
        country_code=src.get("country_code"),
        issn_l=issn_l,
        issn=issn_list,
        is_oa=is_oa,
        is_in_doaj=is_in_doaj,
        works_count=src.get("works_count"),
        cited_by_count=src.get("cited_by_count"),
        summary_stats=summary_stats,
        topics=topics,
        counts_by_year=counts_by_year,
        homepage_url=homepage_url,
        created_date=created_date,
        updated_date=updated_date,
    )
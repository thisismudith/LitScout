# server/ingestion/openalex/normalizer.py

from typing import Dict, Any
from server.ingestion.models import NormalizedPaper, NormalizedAuthor, NormalizedVenue, NormalizedVenueInstance


def normalize_openalex_work(work: Dict[str, Any]) -> NormalizedPaper:
    host_venue = work.get("host_venue") or {}
    concepts = work.get("concepts") or []
    primary_field = concepts[0]["display_name"] if concepts else None

    # Venue
    venue = None
    venue_instance = None
    if host_venue.get("id"):
        venue_type_raw = (host_venue.get("type") or "").lower()
        if venue_type_raw in ("journal", "book-series"):
            venue_type = "journal"
        elif venue_type_raw in ("conference", "proceedings"):
            venue_type = "conference"
        else:
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

    # Authors
    authorships = work.get("authorships") or []
    authors = []
    author_order = []
    is_corr = []

    for idx, auth in enumerate(authorships, start=1):
        author = auth.get("author") or {}
        institutions = auth.get("institutions") or []
        affiliation = institutions[0].get("display_name") if institutions else None
        ids = author.get("ids") or {}

        na = NormalizedAuthor(
            full_name=author.get("display_name"),
            affiliation=affiliation,
            orcid=ids.get("orcid"),
            external_ids={
                "openalex": author.get("id"),
                "orcid": ids.get("orcid"),
                "scopus": ids.get("scopus"),
                "semantic_scholar": ids.get("semantic_scholar"),
            },
        )
        authors.append(na)
        author_order.append(idx)
        is_corr.append(False)

    return NormalizedPaper(
        title=work.get("title"),
        abstract=work.get("abstract"),
        year=work.get("publication_year"),
        publication_date=work.get("publication_date"),
        doi=work.get("doi"),
        field=primary_field,
        language=work.get("language"),
        venue=venue,
        venue_instance=venue_instance,
        authors=authors,
        author_order=author_order,
        is_corresponding_flags=is_corr,
        external_ids={"openalex": work.get("id")},
    )

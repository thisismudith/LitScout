# server/ingestion/models.py

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass
class NormalizedVenue:
    name: str
    short_name: Optional[str]
    venue_type: str            # "conference" or "journal"
    homepage_url: Optional[str]
    location: Optional[str]
    rank_label: Optional[str]
    external_ids: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedVenueInstance:
    venue: NormalizedVenue
    year: Optional[int]


@dataclass
class NormalizedAuthor:
    full_name: str
    works_counted: Optional[int]
    cited_by_count: Optional[int]
    orcid: Optional[str]
    affiliations: List[Dict[str, Any]] = field(default_factory=list)
    last_known_institutions: List[Dict[str, Any]] = field(default_factory=list)
    topics: List[Dict[str, Any]] = field(default_factory=list)
    topic_shares: List[Dict[str, Any]] = field(default_factory=list)
    external_ids: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NormalizedPaper:
    title: str
    abstract: Optional[str]
    conclusion: Optional[str]
    year: Optional[int]
    publication_date: Optional[str]
    doi: Optional[str]
    field: Optional[str]
    language: Optional[str]
    venue: Optional[NormalizedVenue]
    venue_instance: Optional[NormalizedVenueInstance]
    authors: List[NormalizedAuthor] = field(default_factory=list)
    author_order: List[int] = field(default_factory=list)
    referenced_works: List[str] = field(default_factory=list)
    related_works: List[str] = field(default_factory=list)
    concepts: Dict[str, Any] = field(default_factory=dict)
    external_ids: Dict[str, Any] = field(default_factory=dict)
    is_corresponding_flags: List[bool] = field(default_factory=list)
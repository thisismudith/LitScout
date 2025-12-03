# litscout/server/ingestion/models.py

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


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
    source_id: Optional[str]
    publisher_id: Optional[str]
    authors: List[NormalizedAuthor] = field(default_factory=list)
    author_order: List[int] = field(default_factory=list)
    referenced_works: List[str] = field(default_factory=list)
    related_works: List[str] = field(default_factory=list)
    concepts: Dict[str, Any] = field(default_factory=dict)
    external_ids: Dict[str, Any] = field(default_factory=dict)
    is_corresponding_flags: List[bool] = field(default_factory=list)

@dataclass
class NormalizedSource:
    id: str
    name: str
    source_type: Optional[str]
    host_organization_id: Optional[str]
    host_organization_name: Optional[str]
    country_code: Optional[str]
    issn_l: Optional[str]
    is_oa: Optional[bool]
    is_in_doaj: Optional[bool]
    works_count: Optional[int]
    cited_by_count: Optional[int]
    homepage_url: Optional[str]
    created_date: Optional[str]
    updated_date: Optional[str]
    issn: List[str] = field(default_factory=list)
    summary_stats: Dict[str, Any] = field(default_factory=dict)
    topics: List[Dict[str, Any]] = field(default_factory=list)
    counts_by_year: List[Dict[str, Any]] = field(default_factory=list)
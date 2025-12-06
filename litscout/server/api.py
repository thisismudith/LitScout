# litscout/server/api.py

from __future__ import annotations

from typing import List, Dict, Any, Tuple, Union
import fastapi
import uvicorn

from colorama import Fore

from server.logger import ColorLogger
from server.globals import ENV_VARIABLES, DEFAULT_MAX_WORKERS, SEMANTIC_SEARCH_MODEL_NAME

from server.semantic.embeddings import embed_missing_concepts, embed_missing_papers
from server.ingestion.openalex.enrich import enrich_openalex
from server.database.db_manager import start_postgres, stop_postgres, init_database
from server.ingestion.openalex.ingest import ingest_openalex_concept, ingest_source
from server.ingestion.openalex.fetch_concepts import ingest_openalex_from_fields
from server.ingestion.openalex.fetch_sources import ingest_sources_from_papers
from server.semantic.search import (
    search_papers,
    search_concepts,
    search_papers_hybrid,
    search_authors_from_papers,
    search_sources_from_papers,
)

class LitScoutAPI:
    """Main API class for LitScout functionalities."""


    def __init__(self, db_config: Dict[str, str] = ENV_VARIABLES) -> None:
        """Initialize the LitScout API with an optional database configuration."""
        self.log = ColorLogger("API", tag_color=Fore.GREEN, include_timestamps=False)

        if db_config is None:
            db_config = ENV_VARIABLES

        for key, value in ENV_VARIABLES.items():
            db_config.setdefault(key, value)

        self.db_config = db_config


    # Database lifecycle methods
    def init_database(self, force: bool = False) -> None:
        """Initialize the postgres database."""

        init_database(db_name=self.db_config.get("name"), db_user=self.db_config.get("user"),
                      db_host=self.db_config.get("host"), db_port=self.db_config.get("port"),
                      db_password=self.db_config.get("password"), force=force)


    def start_database(self) -> None:
        """Start the postgres database."""

        start_postgres(host=self.db_config.get("host"), port=self.db_config.get("port"))


    def stop_database(self) -> None:
        """Stop the postgres database."""

        stop_postgres()
    

    # Ingestion methods
    def ingest_openalex_concept(
        self,
        concept_id: str,
        pages: int = 1,
        verify: bool = False,
    ) -> bool:
        """Ingest papers from a single OpenAlex concept by its ID."""

        self.log.info(f"Starting OpenAlex ingestion for concept {concept_id} ({pages} pages)...")
        return ingest_openalex_concept(
            concept_id=concept_id,
            pages=pages,
            verify=verify,
        )


    def ingest_openalex_concepts(
        self,
        fields: List[str] = [], 
        pages: int = 1,
        skip_existing: bool = False,
        verify: bool = False,
        per_field_limit: int = 500,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> Dict[str, Union[int, List[Tuple[str, str]]]]:
        """Ingest papers from multiple OpenAlex concepts specified by fields."""

        fields = [f.strip() for f in fields if f.strip()]
        if not fields:
            self.log.error("No valid fields provided. Use --fields 'computer science' 'economics' ...")
            return

        self.log.info(
            f"Starting OpenAlex multi-field ingestion for fields={fields}, pages={pages}, max_workers={max_workers}, "
            f"skip_existing={skip_existing}, per_field_limit={per_field_limit}, verify={verify}..."
        )

        # {success: int, failed: int, failed_ids: List[Tuple[str, str]]}
        return ingest_openalex_from_fields(
            fields=fields,
            max_workers=max_workers,
            pages=pages,
            skip_existing=skip_existing,
            per_field_limit=per_field_limit,
            verify=verify,
        )


    def ingest_openalex_source(
        self,
        source_id: str,
    ) -> bool:
        """Ingest a single OpenAlex source by its ID."""

        self.log.info(f"Starting OpenAlex sources ingestion for publisher {source_id}...")
        return ingest_source(source_id=source_id)


    def ingest_openalex_sources_from_papers(
        self,
        batch_size: int = 10,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> Dict[str, Union[int, List[str]]]:
        """Ingest multiple OpenAlex sources from existing papers in the database."""

        self.log.info(f"Starting OpenAlex sources ingestion with a batch size of {batch_size} and {max_workers} workers...")

        # {processed: int, total_missing: int, missing_ids: List[str]}
        return ingest_sources_from_papers(batch_size=batch_size, max_workers=max_workers)


    # Semantic methods
    def enrich(
        self,
        authors: bool = False,
        concepts: bool = False,
        papers: bool = False,
        concept_ids: List[str] = [],
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> Dict[str, Union[Dict[str, Union[int, List[Tuple[str, str]]]]], None]:
        """Enrich database entities with semantic embeddings."""

        if not (authors or concepts or papers):
            self.log.error("No entity type specified for enrichment. Specify at least one of authors, concepts, or papers.")
            return False
        
        concept_ids = [c.strip() for c in concept_ids if c.strip()]
        parts = []

        if authors:
            parts.append("authors")

        if papers:
            papers_msg = "papers"
            if concept_ids:
                papers_msg += f" (only concepts: {' '.join(concept_ids)})"
            parts.append(papers_msg)

        if concepts:
            parts.append("concepts")

        self.log.info(f"Starting OpenAlex enrichment for {', '.join(parts)} with {max_workers} workers...")

        return enrich_openalex(
            enrich_authors=authors,
            enrich_papers=papers,
            enrich_concepts=concepts,
            concept_ids=concept_ids,
            max_workers=max_workers,
        )


    def embed_papers(
        self,
        batch_size: int = 64,
        limit: int = None,
        force: bool = False,
    ) -> Dict[str, int]:
        """Perform semantic embeddings for a list of paper IDs."""

        # {success: int, failed: int}
        return embed_missing_papers(
            batch_size=batch_size,
            limit=limit,
            force=force,
        )


    def embed_concepts(
        self,
        model: str = SEMANTIC_SEARCH_MODEL_NAME,
        batch_size: int = 64,
        limit: int = None,
        force: bool = False,
    ) -> Dict[str, int]:
        """Perform semantic embeddings for a list of concept IDs."""
        return embed_missing_concepts(
            batch_size=batch_size,
            limit=limit,
            force=force,
        )

    def search(
        self,
        query: str,
        type: str = "hybrid",
        limit: int = 10,
        offset: int = 0,
        paper_weight: float = 0.8,
        concept_weight: float = 0.2,
        concepts_limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Perform semantic search for papers based on a text query."""

        type = type.casefold()

        if type == "papers":
            return search_papers(query=query, limit=limit, offset=offset)
        
        elif type == "concepts":
            return search_concepts(query=query, limit=limit, offset=offset)
        
        elif type == "hybrid":
            if paper_weight + concept_weight != 1.0:
                if paper_weight == 0.4:
                    paper_weight = 1.0 - concept_weight
                else:
                    concept_weight = 1.0 - paper_weight

                self.log.warn(
                    f"paper_weight and concept_weight must sum to 1.0; "
                    f"adjusted to paper_weight={paper_weight}, concept_weight={concept_weight}."
                )

            return search_papers_hybrid(
                query=query,
                limit=limit,
                offset=offset,
                paper_weight=paper_weight,
                concept_weight=concept_weight,
                top_k_concepts=concepts_limit,
                top_k_papers_per_concept=limit,
            )
        
        elif type == "venue":
            if paper_weight + concept_weight != 1.0:
                if paper_weight == 0.4:
                    paper_weight = 1.0 - concept_weight
                else:
                    concept_weight = 1.0 - paper_weight

                self.log.warn(
                    f"paper_weight and concept_weight must sum to 1.0; "
                    f"adjusted to paper_weight={paper_weight}, concept_weight={concept_weight}."
                )
            
            return search_sources_from_papers(
                query=query,
                paper_weight=paper_weight,
                concept_weight=concept_weight,
                top_k_concepts=concepts_limit,
                top_k_papers_per_concept=limit,
            )
        elif type == "author":
            if paper_weight + concept_weight != 1.0:
                if paper_weight == 0.4:
                    paper_weight = 1.0 - concept_weight
                else:
                    concept_weight = 1.0 - paper_weight

                self.log.warn(
                    f"paper_weight and concept_weight must sum to 1.0; "
                    f"adjusted to paper_weight={paper_weight}, concept_weight={concept_weight}."
                )

            return search_authors_from_papers(
                query=query,
                limit=limit,
                offset=offset,
                paper_weight=paper_weight,
                concept_weight=concept_weight,
                top_k_concepts=concepts_limit,
                top_k_papers_per_concept=limit,
            )
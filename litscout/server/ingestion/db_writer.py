# server/ingestion/db_writer.py

from typing import List
from psycopg2 import OperationalError
from psycopg2.extras import Json
from colorama import Fore

from server.database.db_utils import (
    ENV_DB_NAME, ENV_DB_USER, ENV_DB_PASSWORD,
    ENV_DB_HOST, ENV_DB_PORT, _connect_with_optional_prompt,
)
from server.ingestion.models import NormalizedVenue, NormalizedAuthor, NormalizedPaper
from server.logger import ColorLogger

log = ColorLogger("DB", tag_color=Fore.BLUE, include_timestamps=False)

def get_conn():
    """
    Get a psycopg2 connection for ingestion.

    Silent on success to avoid log spam when used by many threads.
    """

    try:
        dbname = ENV_DB_NAME
        user = ENV_DB_USER
        password = ENV_DB_PASSWORD
        host = ENV_DB_HOST
        port = ENV_DB_PORT

        conn, _ = _connect_with_optional_prompt(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            purpose=f"ingestion connection to '{dbname}'",
        )
        conn.autocommit = False
        return conn
    except OperationalError as e:
        # Only log on real failure
        log.error(
            f"[INGEST] Failed to connect to database '{dbname}' as '{user}'."
        )
        log.error(str(e))
        raise

# ---------------------------------------------------------
# VENUE + VENUE INSTANCES
# ---------------------------------------------------------
def upsert_venue(cur, venue: NormalizedVenue) -> int:
    """
    Insert or reuse a venue.
    Prefer external_ids->'openalex'
    """
    openalex_id = (venue.external_ids or {}).get("openalex")

    if openalex_id:
        cur.execute(
            "SELECT id FROM venues WHERE external_ids ->> 'openalex' = %s;",
            (openalex_id,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    # fallback by name
    cur.execute(
        """
        INSERT INTO venues (name, short_name, venue_type, homepage_url,
                            location, rank_label, external_ids)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE
          SET short_name   = EXCLUDED.short_name,
              venue_type   = EXCLUDED.venue_type,
              homepage_url = EXCLUDED.homepage_url,
              location     = EXCLUDED.location,
              rank_label   = EXCLUDED.rank_label,
              external_ids = EXCLUDED.external_ids
        RETURNING id;
        """,
        (
            venue.name,
            venue.short_name,
            venue.venue_type,
            venue.homepage_url,
            venue.location,
            venue.rank_label,
            Json(venue.external_ids),
        ),
    )
    (venue_id,) = cur.fetchone()
    return venue_id


def get_or_create_venue_instance(cur, venue_id: int, year: int | None):
    """
    Create or reuse a specific venue instance (venue + year)
    """
    if year is None:
        return None

    cur.execute(
        """
        SELECT id FROM venue_instances
        WHERE venue_id = %s AND year = %s;
        """,
        (venue_id, year),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO venue_instances (venue_id, year)
        VALUES (%s, %s)
        RETURNING id;
        """,
        (venue_id, year),
    )
    (vi_id,) = cur.fetchone()
    return vi_id


# CONCEPTS
def upsert_concept(cur, concept_id: str, name: str, level: int) -> int:
    """
    Insert or reuse a concept by OpenAlex ID.
    """
    cur.execute(
        "SELECT id FROM concepts WHERE id = %s;",
        (concept_id,),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO concepts (id, name, level)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE
            SET id      = EXCLUDED.id,
                name    = EXCLUDED.name,
                level   = EXCLUDED.level
        RETURNING id;
        """,
        (concept_id, name, level),
    )
    (cid,) = cur.fetchone()
    return cid

# AUTHORS
def upsert_author(cur, author: NormalizedAuthor) -> int:
    """
    Insert or reuse an author.
    Prefer external_ids->openalex, else insert new.
    """
    ext = author.external_ids or {}
    oa = ext.get("openalex")

    if oa:
        cur.execute(
            "SELECT id FROM authors WHERE external_ids ->> 'openalex' = %s;",
            (oa,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute(
        """
        INSERT INTO authors
            (full_name, affiliations, last_known_institutions,
            topic_shares, orcid, external_ids)
        VALUES 
            (%s, %s, %s,
            %s, %s, %s)
        ON CONFLICT (orcid) DO UPDATE
            SET full_name               = EXCLUDED.full_name,
              affiliations              = EXCLUDED.affiliations,
              last_known_institutions   = EXCLUDED.last_known_institutions,
              topic_shares              = EXCLUDED.topic_shares,
              orcid                     = EXCLUDED.orcid,
              external_ids              = EXCLUDED.external_ids
        RETURNING id;
        """,
        (author.full_name, Json(author.affiliations), Json(author.last_known_institutions),
         Json(author.topic_shares), author.orcid, Json(ext)
        ),
    )
    (author_id,) = cur.fetchone()
    return author_id


# PAPERS
def upsert_paper(cur, p: NormalizedPaper, venue_id, venue_instance_id) -> int:
    """
    Insert or reuse a paper.
    Priority:
        1. DOI
        2. external_ids->openalex
    Updates venue_id & venue_instance_id if found.
    """

    doi = p.doi
    oa = (p.external_ids or {}).get("openalex")

    # match by DOI
    if doi:
        cur.execute(
            """
            INSERT INTO papers
                (title, abstract, conclusion, year, publication_date,
                doi, field, language, referenced_works, related_works, 
                venue_id, venue_instance_id, concepts, external_ids)
            VALUES
                (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
            ON CONFLICT (doi) DO UPDATE
               SET title            = EXCLUDED.title,
                   abstract         = EXCLUDED.abstract,
                   conclusion       = EXCLUDED.conclusion,
                   year             = EXCLUDED.year,
                   publication_date = EXCLUDED.publication_date,
                   field            = EXCLUDED.field,
                   language         = EXCLUDED.language,
                   referenced_works = EXCLUDED.referenced_works,
                   related_works    = EXCLUDED.related_works,
                   venue_id         = EXCLUDED.venue_id,
                   venue_instance_id= EXCLUDED.venue_instance_id,
                   concepts         = EXCLUDED.concepts,
                   external_ids     = EXCLUDED.external_ids
            RETURNING id;
            """,
            (
                p.title, p.abstract, p.conclusion, p.year, p.publication_date,
                p.doi, p.field, p.language, p.referenced_works, p.related_works,
                venue_id, venue_instance_id, Json(p.concepts), Json(p.external_ids),
            ),
        )
        cur.execute("SELECT id FROM papers WHERE doi=%s;", (doi,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute(
                """
                UPDATE papers
                SET venue_id = COALESCE(%s, venue_id),
                    venue_instance_id = COALESCE(%s, venue_instance_id),
                    external_ids = external_ids || %s
                WHERE id=%s;
                """,
                (venue_id, venue_instance_id, Json(p.external_ids), pid),
            )
            return pid

    # match by OpenAlex ID
    if oa:
        cur.execute(
            "SELECT id FROM papers WHERE external_ids ->> 'openalex'=%s;",
            (oa,),
        )
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute(
                """
                UPDATE papers
                SET venue_id = COALESCE(%s, venue_id),
                    venue_instance_id = COALESCE(%s, venue_instance_id),
                    external_ids = external_ids || %s
                WHERE id=%s;
                """,
                (venue_id, venue_instance_id, Json(p.external_ids), pid),
            )
            return pid

    # new insert
    cur.execute(
        """
        INSERT INTO papers
            (title, abstract, conclusion, year, publication_date,
             doi, field, language, referenced_works, related_works, 
             venue_id, venue_instance_id, concepts, external_ids)
        VALUES
            (%s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s,
             %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            p.title, p.abstract, p.conclusion, p.year, p.publication_date,
            p.doi, p.field, p.language, p.referenced_works, p.related_works,
            venue_id, venue_instance_id, Json(p.concepts), Json(p.external_ids),
        ),
    )
    (pid,) = cur.fetchone()
    return pid


# PAPER-AUTHORS
def insert_paper_authors(cur, paper_id, p: NormalizedPaper, author_ids: List[int]):
    """
    Insert/Upsert rows in paper_authors.
    """
    for idx, author_obj in enumerate(p.authors):

        author_id = author_ids[idx]
        order = p.author_order[idx] if idx < len(p.author_order) else idx + 1
        corr = p.is_corresponding_flags[idx] if idx < len(p.is_corresponding_flags) else False

        cur.execute(
            """
            INSERT INTO paper_authors (paper_id, author_id, author_order, is_corresponding)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (paper_id, author_id) DO UPDATE
              SET author_order = EXCLUDED.author_order,
                  is_corresponding = EXCLUDED.is_corresponding;
            """,
            (paper_id, author_id, order, corr),
        )
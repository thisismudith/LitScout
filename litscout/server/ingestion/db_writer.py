# server/ingestion/db_writer.py

from typing import Dict
from psycopg2.extras import Json

from server.ingestion.models import NormalizedPaper, NormalizedAuthor, NormalizedVenue
from server.logger import ColorLogger
from colorama import Fore
from server.database.db_utils import (
    ENV_DB_NAME,
    ENV_DB_USER,
    ENV_DB_PASSWORD,
    ENV_DB_HOST,
    ENV_DB_PORT,
    _connect_with_optional_prompt,
)

log = ColorLogger("INGESTION", tag_color=Fore.GREEN, include_timestamps=False)

def get_conn():
    log.info(f"Connecting to database '{ENV_DB_NAME}' as user '{ENV_DB_USER}'...")
    
    conn, _ = _connect_with_optional_prompt(
        dbname=ENV_DB_NAME,
        user=ENV_DB_USER,
        password=ENV_DB_PASSWORD,
        host=ENV_DB_HOST,
        port=ENV_DB_PORT,
        purpose=f"ingestion connection to '{ENV_DB_NAME}'",
    )
    conn.autocommit = False
    log.success(f"Connected to '{ENV_DB_NAME}'.")
    return conn


def ensure_source(cur, name: str) -> int:
    cur.execute("SELECT id FROM sources WHERE name = %s;", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO sources (name) VALUES (%s) RETURNING id;", (name,))
    (sid,) = cur.fetchone()
    return sid


def upsert_venue(cur, venue: NormalizedVenue) -> int:
    openalex_id = (venue.external_ids or {}).get("openalex")
    if openalex_id:
        cur.execute(
            "SELECT id FROM venues WHERE external_ids ->> 'openalex' = %s;",
            (openalex_id,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute(
        """
        INSERT INTO venues (name, short_name, venue_type, homepage_url,
                            location, rank_label, external_ids)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE
          SET short_name = EXCLUDED.short_name,
              venue_type = EXCLUDED.venue_type,
              homepage_url = EXCLUDED.homepage_url,
              location = EXCLUDED.location,
              rank_label = EXCLUDED.rank_label,
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


def get_or_create_venue_instance(cur, venue_id: int, year: int | None) -> int | None:
    if year is None:
        return None
    cur.execute(
        "SELECT id FROM venue_instances WHERE venue_id = %s AND year = %s;",
        (venue_id, year),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO venue_instances (venue_id, year) VALUES (%s, %s) RETURNING id;",
        (venue_id, year),
    )
    (vi_id,) = cur.fetchone()
    return vi_id


def upsert_author(cur, author: NormalizedAuthor) -> int:
    ext = author.external_ids or {}
    openalex_id = ext.get("openalex")

    if openalex_id:
        cur.execute(
            "SELECT id FROM authors WHERE external_ids ->> 'openalex' = %s;",
            (openalex_id,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute(
        """
        INSERT INTO authors (full_name, affiliation, orcid, external_ids)
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """,
        (author.full_name, author.affiliation, author.orcid, Json(author.external_ids)),
    )
    (author_id,) = cur.fetchone()
    return author_id


def upsert_paper(cur, p: NormalizedPaper, venue_id: int | None, venue_instance_id: int | None) -> int:
    doi = p.doi
    oa_id = (p.external_ids or {}).get("openalex")

    if doi:
        cur.execute("SELECT id FROM papers WHERE doi = %s;", (doi,))
        row = cur.fetchone()
        if row:
            return row[0]
    if oa_id:
        cur.execute(
            "SELECT id FROM papers WHERE external_ids ->> 'openalex' = %s;",
            (oa_id,),
        )
        row = cur.fetchone()
        if row:
            return row[0]

    cur.execute(
        """
        INSERT INTO papers
            (title, abstract, year, publication_date,
             doi, field, language,
             venue_id, venue_instance_id, external_ids)
        VALUES
            (%s, %s, %s, %s,
             %s, %s, %s,
             %s, %s, %s)
        RETURNING id;
        """,
        (
            p.title,
            p.abstract,
            p.year,
            p.publication_date,
            p.doi,
            p.field,
            p.language,
            venue_id,
            venue_instance_id,
            Json(p.external_ids),
        ),
    )
    (paper_id,) = cur.fetchone()
    return paper_id


def insert_paper_authors(cur, paper_id: int, p: NormalizedPaper, author_ids: Dict[int, int]):
    for idx, author in enumerate(p.authors):
        order = p.author_order[idx] if idx < len(p.author_order) else idx + 1
        is_corr = p.is_corresponding_flags[idx] if idx < len(p.is_corresponding_flags) else False
        author_id = author_ids[idx]

        cur.execute(
            """
            INSERT INTO paper_authors (paper_id, author_id, author_order, is_corresponding)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (paper_id, author_id) DO UPDATE
              SET author_order = EXCLUDED.author_order,
                  is_corresponding = EXCLUDED.is_corresponding;
            """,
            (paper_id, author_id, order, is_corr),
        )


def insert_paper_source(cur, paper_id: int, source_id: int, external_id: str, url: str | None):
    cur.execute(
        """
        INSERT INTO paper_sources (paper_id, source_id, external_id, url)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (paper_id, source_id) DO NOTHING;
        """,
        (paper_id, source_id, external_id, url),
    )
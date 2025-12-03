# server/ingestion/db_writer.py

from typing import Dict, List, Any
from psycopg2.extras import Json
from colorama import Fore

from server.database.db_utils import get_conn
from server.ingestion.models import NormalizedAuthor, NormalizedPaper, NormalizedSource
from server.logger import ColorLogger

log = ColorLogger("DB", tag_color=Fore.BLUE, include_timestamps=False)

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
def upsert_paper(cur, p: NormalizedPaper) -> int:
    """
    Insert or reuse a paper.
    Priority:
        1. DOI
        2. external_ids->openalex
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
                concepts, external_ids)
            VALUES
                (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s)
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
                concepts         = EXCLUDED.concepts,
                external_ids     = EXCLUDED.external_ids
            RETURNING id;
            """,
            (
                p.title, p.abstract, p.conclusion, p.year, p.publication_date,
                p.doi, p.field, p.language, p.referenced_works, p.related_works,
                Json(p.concepts), Json(p.external_ids),
            ),
        )
        cur.execute("SELECT id FROM papers WHERE doi=%s;", (doi,))
        row = cur.fetchone()
        if row:
            pid = row[0]
            cur.execute(
                """
                UPDATE papers
                SET external_ids = external_ids || %s
                WHERE id=%s;
                """,
                (Json(p.external_ids), pid),
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
                SET external_ids = external_ids || %s
                WHERE id=%s;
                """,
                (Json(p.external_ids), pid),
            )
            return pid

    # new insert
    cur.execute(
        """
        INSERT INTO papers
            (title, abstract, conclusion, year, publication_date,
             doi, field, language, referenced_works, related_works, 
             concepts, external_ids)
        VALUES
            (%s, %s, %s, %s, %s, 
             %s, %s, %s, %s, %s,
             %s, %s)
        RETURNING id;
        """,
        (
            p.title, p.abstract, p.conclusion, p.year, p.publication_date,
            p.doi, p.field, p.language, p.referenced_works, p.related_works,
            Json(p.concepts), Json(p.external_ids),
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

def upsert_sources_batch(records: List[NormalizedSource]) -> None:
    """
    UPSERT a batch of normalized sources into the 'sources' table.
    """
    if not records:
        return

    conn = get_conn()
    cur = conn.cursor()

    for record in records:
        insert_sql = """
            INSERT INTO sources (
                id,
                display_name,
                source_type,
                host_organization_id,
                host_organization_name,
                country_code,
                issn_l,
                issn,
                is_oa,
                is_in_doaj,
                works_count,
                cited_by_count,
                summary_stats,
                topics,
                counts_by_year,
                homepage_url,
                created_date,
                updated_date,
                raw_json
            )
            VALUES (
                %(id)s,
                %(display_name)s,
                %(source_type)s,
                %(host_organization_id)s,
                %(host_organization_name)s,
                %(country_code)s,
                %(issn_l)s,
                %(issn)s,
                %(is_oa)s,
                %(is_in_doaj)s,
                %(works_count)s,
                %(cited_by_count)s,
                %(summary_stats)s,
                %(topics)s,
                %(counts_by_year)s,
                %(homepage_url)s,
                %(created_date)s,
                %(updated_date)s,
                %(raw_json)s
            )
            ON CONFLICT (id) DO UPDATE
            SET
                display_name = EXCLUDED.display_name,
                source_type = EXCLUDED.source_type,
                host_organization_id = EXCLUDED.host_organization_id,
                host_organization_name = EXCLUDED.host_organization_name,
                country_code = EXCLUDED.country_code,
                issn_l = EXCLUDED.issn_l,
                issn = EXCLUDED.issn,
                is_oa = EXCLUDED.is_oa,
                is_in_doaj = EXCLUDED.is_in_doaj,
                works_count = EXCLUDED.works_count,
                cited_by_count = EXCLUDED.cited_by_count,
                summary_stats = EXCLUDED.summary_stats,
                topics = EXCLUDED.topics,
                counts_by_year = EXCLUDED.counts_by_year,
                homepage_url = EXCLUDED.homepage_url,
                created_date = EXCLUDED.created_date,
                updated_date = EXCLUDED.updated_date,
                raw_json = EXCLUDED.raw_json;
        """
        cur.execute(insert_sql, {
            'id': record.id,
            'display_name': record.name,
            'source_type': record.source_type,
            'host_organization_id': record.host_organization_id,
            'host_organization_name': record.host_organization_name,
            'country_code': record.country_code,
            'issn_l': record.issn_l,
            'issn': record.issn,
            'is_oa': record.is_oa,
            'is_in_doaj': record.is_in_doaj,
            'works_count': record.works_count,
            'cited_by_count': record.cited_by_count,
            'summary_stats': Json(record.summary_stats),
            'topics': Json(record.topics),
            'counts_by_year': Json(record.counts_by_year),
            'homepage_url': record.homepage_url,
            'created_date': record.created_date,
            'updated_date': record.updated_date,
            'raw_json': Json(record.raw_json),
        })

    conn.commit()
    cur.close()
    conn.close()
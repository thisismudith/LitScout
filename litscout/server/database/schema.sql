CREATE EXTENSION IF NOT EXISTS vector;

CREATE TYPE venue_type AS ENUM ('conference', 'journal');

-- Concepts
CREATE TABLE concepts (
    id                  TEXT PRIMARY KEY,  -- OpenAlex concept ID
    name                TEXT NOT NULL,
    level               INTEGER,
    description         TEXT,
    works_count         INTEGER,
    cited_by_count      INTEGER,
    related_concepts    JSONB
);

CREATE TABLE concept_embeddings (
    concept_id      TEXT REFERENCES concepts(id) ON DELETE CASCADE,
    model_name      TEXT NOT NULL,
    embedding_vec   vector(768) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (concept_id, model_name)
);

-- Authors
CREATE TABLE authors (
    id                      BIGSERIAL PRIMARY KEY,
    full_name               TEXT NOT NULL,
    works_counted           INTEGER,
    cited_by_count          INTEGER,
    orcid                   TEXT UNIQUE,
    affiliations            JSONB,
    last_known_institutions JSONB,
    topics                  JSONB,
    topic_shares            JSONB,
    external_ids            JSONB,
    cluster_ids             BIGINT[] NOT NULL DEFAULT '{}',
    cluster_ids_weightage   REAL[]   NOT NULL DEFAULT '{}',

    CONSTRAINT authors_clusters_same_length
        CHECK (cardinality(cluster_ids) = cardinality(cluster_ids_weightage))
);

-- Sources (Venues)
CREATE TABLE sources (
    id                  TEXT PRIMARY KEY,       -- OpenAlex source ID, e.g. 'https://openalex.org/S123...'
    name        TEXT NOT NULL,
    source_type         TEXT NOT NULL,          -- 'journal', 'conference', 'repository', etc.
    host_organization_id   TEXT,
    host_organization_name TEXT,
    country_code        TEXT,
    issn_l              TEXT,
    issn                TEXT[],                 -- all ISSNs as array of text
    is_oa               BOOLEAN,
    is_in_doaj          BOOLEAN,
    works_count         INTEGER,
    cited_by_count      INTEGER,
    summary_stats       JSONB,                  -- 2yr_mean_citedness, h_index, i10_index, etc.
    topics              JSONB,                  -- OpenAlex topics/topics_share if you want them
    counts_by_year      JSONB,                  -- time series of works_count/cited_by_count
    homepage_url        TEXT,
    created_date        TIMESTAMPTZ,
    updated_date        TIMESTAMPTZ
);


-- Papers
CREATE TABLE papers (
    id                      BIGSERIAL PRIMARY KEY,
    title                   TEXT NOT NULL,
    abstract                TEXT,
    conclusion              TEXT,
    year                    INTEGER,
    publication_date        DATE,
    doi                     TEXT UNIQUE,
    field                   TEXT,
    language                TEXT,
    referenced_works        TEXT[] NOT NULL DEFAULT '{}',
    related_works           TEXT[] NOT NULL DEFAULT '{}',
    concepts                JSONB,
    cluster_ids             BIGINT[] NOT NULL DEFAULT '{}',
    cluster_ids_weightage   REAL[] NOT NULL DEFAULT '{}',
    external_ids            JSONB NOT NULL,
    source_id               TEXT,
    publisher_id            TEXT,

    CONSTRAINT papers_clusters_same_length
        CHECK (cardinality(cluster_ids) = cardinality(cluster_ids_weightage))
);

-- Many-to-many: which authors wrote which papers, in what order
CREATE TABLE paper_authors (
    paper_id        BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    author_id       BIGINT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    author_order    INTEGER NOT NULL, -- Starts from 1
    is_corresponding BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (paper_id, author_id)
);

-- Embeddings for papers (one row per paper, per model)
CREATE TABLE paper_embeddings (
    paper_id        BIGINT REFERENCES papers(id) ON DELETE CASCADE,
    model_name      TEXT NOT NULL,
    embedding_vec   vector(768) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (paper_id, model_name)
);

-- Author Specialties / Research Areas
CREATE TABLE specialties (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,              -- 'Computer Vision', 'NLP', ...
    parent_id   BIGINT REFERENCES specialties(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE author_specialties (
    author_id       BIGINT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    specialty_id    BIGINT NOT NULL REFERENCES specialties(id) ON DELETE CASCADE,
    PRIMARY KEY (author_id, specialty_id)
);

-- Clusters for papers and authors
CREATE TABLE clusters (
    id          BIGSERIAL PRIMARY KEY,
    label       TEXT,  -- human-readable
    object_type TEXT NOT NULL,  -- 'paper', 'author', or 'both'
    algorithm   TEXT NOT NULL,  -- e.g. 'umap+hdbscan_v1'
    params      JSONB,  -- hyperparameters snapshot
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexing for faster lookups

-- Lookup embeddings by model name
CREATE INDEX idx_paper_embeddings_model
    ON paper_embeddings(model_name);

-- Lookup all papers for an author
CREATE INDEX IF NOT EXISTS idx_paper_authors_paper
    ON paper_authors(paper_id);

-- Lookup all authors for a paper
CREATE INDEX IF NOT EXISTS idx_paper_authors_author
    ON paper_authors(author_id);

-- Lookup papers by year
CREATE INDEX IF NOT EXISTS idx_papers_year
    ON papers(year);

-- Fast "papers in cluster X" queries
CREATE INDEX IF NOT EXISTS idx_papers_cluster_ids_gin
    ON papers USING GIN (cluster_ids);

-- Fast "authors in cluster X" queries
CREATE INDEX IF NOT EXISTS idx_authors_cluster_ids_gin
    ON authors USING GIN (cluster_ids);
CREATE TYPE venue_type AS ENUM ('conference', 'journal');

-- Authors
CREATE TABLE authors (
    id                      BIGSERIAL PRIMARY KEY,
    full_name               TEXT NOT NULL,
    affiliation             TEXT,
    orcid                   TEXT UNIQUE,
    external_ids            JSONB,
    cluster_ids             BIGINT[] NOT NULL DEFAULT '{}',
    cluster_ids_weightage   REAL[]   NOT NULL DEFAULT '{}',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT authors_clusters_same_length
        CHECK (cardinality(cluster_ids) = cardinality(cluster_ids_weightage))
);

-- Venues (conferences / journals)
CREATE TABLE venues (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    short_name      TEXT,
    venue_type      venue_type NOT NULL,
    homepage_url    TEXT,
    location        TEXT,
    rank_label      TEXT,
    external_ids    JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Specific yearly / edition instances of a venue
CREATE TABLE venue_instances (
    id                      BIGSERIAL PRIMARY KEY,
    venue_id                BIGINT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
    year                    INTEGER NOT NULL,
    submission_deadline     DATE,
    notification_date       DATE,
    camera_ready_deadline   DATE,
    start_date              DATE,
    end_date                DATE,
    location_override       TEXT,  -- if this year's location differs
    notes                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (venue_id, year)
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
    venue_id                BIGINT REFERENCES venues(id),
    venue_instance_id       BIGINT REFERENCES venue_instances(id),
    external_ids            JSONB NOT NULL
);

-- Many-to-many: which authors wrote which papers, in what order
CREATE TABLE paper_authors (
    paper_id        BIGINT NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    author_id       BIGINT NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    author_order    INTEGER NOT NULL, -- Starts from 1
    is_corresponding BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (paper_id, author_id)
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

-- Lookup all papers for an author
CREATE INDEX IF NOT EXISTS idx_paper_authors_paper
    ON paper_authors(paper_id);

-- Lookup all authors for a paper
CREATE INDEX IF NOT EXISTS idx_paper_authors_author
    ON paper_authors(author_id);

-- Lookup papers by year
CREATE INDEX IF NOT EXISTS idx_papers_year
    ON papers(year);

-- Deadline-based queries for conferences
CREATE INDEX IF NOT EXISTS idx_venue_instances_deadline
    ON venue_instances(submission_deadline);

-- Fast "papers in cluster X" queries
-- CREATE INDEX IF NOT EXISTS idx_papers_cluster_ids_gin
--     ON papers USING GIN (cluster_ids);

-- -- Fast "authors in cluster X" queries
-- CREATE INDEX IF NOT EXISTS idx_authors_cluster_ids_gin
--     ON authors USING GIN (cluster_ids);
-- RAG / pgvector: knowledge-grounded agent (LLD §19)
--
-- Requires the pgvector extension. In managed Postgres (RDS, Cloud SQL, Supabase)
-- the extension is pre-installed. For self-hosted Docker, use the pgvector image:
--   image: pgvector/pgvector:pg16
--
-- If you see "ERROR: extension "vector" does not exist", run as superuser:
--   CREATE EXTENSION IF NOT EXISTS vector;
-- then re-run this migration.

CREATE EXTENSION IF NOT EXISTS vector;

-- One row per indexed resolved case.
-- Populated asynchronously after a case transitions to APPROVED or AUTO_RESOLVED.
-- case_uid is both the PK and the FK, allowing deterministic UPSERT without a
-- separate surrogate key.
CREATE TABLE case_embeddings (
    case_uid    UUID        PRIMARY KEY
                            REFERENCES recon_cases(case_uid) ON DELETE CASCADE,
    match_type  TEXT        NOT NULL,
    resolution  TEXT        NOT NULL,
    content     TEXT        NOT NULL,        -- verbatim text that was embedded (for audit / re-index)
    embedding   vector(384) NOT NULL,        -- AllMiniLmL6V2 dimension
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- IVFFlat approximate cosine index.
-- lists=100 is correct for up to ~1 M vectors.
-- Run REINDEX CONCURRENTLY after a bulk back-fill to rebuild cluster centroids.
CREATE INDEX idx_ce_embedding  ON case_embeddings
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX idx_ce_match_type ON case_embeddings(match_type);
CREATE INDEX idx_ce_resolution  ON case_embeddings(resolution);

-- RAG / pgvector: knowledge-grounded agent (LLD §19)
-- Reference copy — authoritative version is recon-ledger/src/main/resources/db/migration/V3__rag.sql

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE case_embeddings (
    case_uid    UUID        PRIMARY KEY
                            REFERENCES recon_cases(case_uid) ON DELETE CASCADE,
    match_type  TEXT        NOT NULL,
    resolution  TEXT        NOT NULL,
    content     TEXT        NOT NULL,
    embedding   vector(384) NOT NULL,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ce_embedding  ON case_embeddings
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX idx_ce_match_type ON case_embeddings(match_type);
CREATE INDEX idx_ce_resolution  ON case_embeddings(resolution);

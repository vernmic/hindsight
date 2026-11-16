-- Minimal real public schema for the Q8 writer/queue database controls.
--
-- This is the subset of the Hindsight public schema that the maintained
-- operator SQL actually touches (writer journal/apply/rollback, page fetch,
-- queue completion linkage). It is a real PostgreSQL schema, not a stub: the
-- tests run the shipped domain SQL against it, and the temp-table clones are
-- created with LIKE public.<table>, exactly as they are against a live server.
--
-- Column set and types follow the Hindsight baseline (uuid ids, text bank ids,
-- text[] tags, jsonb metadata) restricted to the columns these tools read or
-- write. Anything outside that subset is deliberately absent.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
    id            text NOT NULL,
    bank_id       text NOT NULL,
    original_text text,
    retain_params jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, bank_id)
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id     text PRIMARY KEY,
    document_id  text,
    bank_id      text NOT NULL,
    chunk_index  integer,
    chunk_text   text,
    content_hash text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memory_units (
    id                      uuid PRIMARY KEY,
    bank_id                 text NOT NULL,
    document_id             text,
    chunk_id                text,
    text                    text NOT NULL,
    embedding               vector(384),
    context                 text,
    event_date              timestamptz,
    occurred_start          timestamptz,
    occurred_end            timestamptz,
    mentioned_at            timestamptz,
    fact_type               text NOT NULL DEFAULT 'world'
                            CHECK (fact_type IN ('world', 'experience', 'observation')),
    confidence_score        double precision,
    access_count            integer NOT NULL DEFAULT 0,
    consolidated_at         timestamptz,
    observation_scopes      jsonb,
    tags                    varchar[] NOT NULL DEFAULT '{}',
    metadata                jsonb NOT NULL DEFAULT '{}',
    proof_count             integer DEFAULT 1,
    source_memory_ids       uuid[],
    history                 jsonb DEFAULT '[]',
    text_signals            jsonb,
    consolidation_failed_at timestamptz,
    search_vector           tsvector,
    edited_at               timestamptz,
    attachment_ids          uuid[],
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memory_links (
    from_unit_id uuid,
    to_unit_id   uuid,
    link_type    text,
    entity_id    uuid,
    weight       double precision,
    created_at   timestamptz NOT NULL DEFAULT now(),
    bank_id      text
);

CREATE TABLE IF NOT EXISTS entities (
    id             uuid PRIMARY KEY,
    bank_id        text NOT NULL,
    canonical_name text,
    metadata       jsonb DEFAULT '{}',
    first_seen     timestamptz NOT NULL DEFAULT now(),
    last_seen      timestamptz NOT NULL DEFAULT now(),
    mention_count  integer NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS unit_entities (
    unit_id   uuid,
    entity_id uuid
);

CREATE TABLE IF NOT EXISTS observation_history (
    id             bigserial PRIMARY KEY,
    observation_id uuid,
    bank_id        text,
    content        jsonb,
    changed_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS async_operations (
    operation_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    bank_id         text,
    operation_type  text,
    status          text,
    result_metadata jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz
);

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- control plane
-- =========================================================

CREATE TABLE crawl_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    triggered_by TEXT NOT NULL DEFAULT 'system',
    config_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    partitions_total INT NOT NULL DEFAULT 0,
    partitions_done INT NOT NULL DEFAULT 0,
    partitions_failed INT NOT NULL DEFAULT 0,
    notes TEXT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_crawl_run_status ON crawl_run(status);
CREATE INDEX idx_crawl_run_started_at ON crawl_run(started_at DESC);

CREATE TABLE crawl_partition (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    crawl_run_id UUID NOT NULL REFERENCES crawl_run(id) ON DELETE CASCADE,
    parent_partition_id UUID NULL REFERENCES crawl_partition(id) ON DELETE SET NULL,
    partition_key TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    depth INT NOT NULL DEFAULT 0,
    split_dimension TEXT NULL,
    split_value TEXT NULL,
    planner_policy_version TEXT NOT NULL DEFAULT 'v1',
    is_terminal BOOLEAN NOT NULL DEFAULT TRUE,
    is_saturated BOOLEAN NOT NULL DEFAULT FALSE,
    coverage_status TEXT NOT NULL DEFAULT 'unassessed',
    pages_total_expected INT NULL,
    pages_processed INT NOT NULL DEFAULT 0,
    items_seen INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NULL,
    finished_at TIMESTAMPTZ NULL,
    last_error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_crawl_partition_run_key UNIQUE (crawl_run_id, partition_key),
    CONSTRAINT uq_crawl_partition_run_scope_key UNIQUE (crawl_run_id, scope_key)
);

CREATE INDEX idx_crawl_partition_run_id
    ON crawl_partition(crawl_run_id);

CREATE INDEX idx_crawl_partition_status
    ON crawl_partition(status);

CREATE INDEX idx_crawl_partition_parent_partition_id
    ON crawl_partition(parent_partition_id);

CREATE INDEX idx_crawl_partition_coverage_status
    ON crawl_partition(coverage_status);

-- =========================================================
-- api logging / raw
-- =========================================================

CREATE TABLE api_request_log (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    crawl_partition_id UUID NULL REFERENCES crawl_partition(id) ON DELETE SET NULL,
    request_type TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT 'GET',
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_headers_json JSONB NULL,
    status_code INT NOT NULL,
    latency_ms INT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_received_at TIMESTAMPTZ NULL,
    error_type TEXT NULL,
    error_message TEXT NULL
);

CREATE INDEX idx_api_request_log_requested_at
    ON api_request_log(requested_at DESC);

CREATE INDEX idx_api_request_log_status_code
    ON api_request_log(status_code);

CREATE INDEX idx_api_request_log_run_id
    ON api_request_log(crawl_run_id);

CREATE INDEX idx_api_request_log_partition_id
    ON api_request_log(crawl_partition_id);

CREATE TABLE raw_api_payload (
    id BIGSERIAL PRIMARY KEY,
    api_request_log_id BIGINT NOT NULL REFERENCES api_request_log(id) ON DELETE CASCADE,
    endpoint_type TEXT NOT NULL,
    entity_hh_id TEXT NULL,
    payload_json JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_api_payload_request_log_id
    ON raw_api_payload(api_request_log_id);

CREATE INDEX idx_raw_api_payload_entity_hh_id
    ON raw_api_payload(entity_hh_id);

CREATE INDEX idx_raw_api_payload_received_at
    ON raw_api_payload(received_at DESC);

-- =========================================================
-- dictionaries / reference data
-- =========================================================

CREATE TABLE area (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_area_id TEXT NOT NULL,
    name TEXT NOT NULL,
    parent_area_id UUID NULL REFERENCES area(id) ON DELETE SET NULL,
    level INT NULL,
    path_text TEXT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_area_hh_area_id UNIQUE (hh_area_id)
);

CREATE INDEX idx_area_parent_area_id
    ON area(parent_area_id);

CREATE TABLE professional_role (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_professional_role_id TEXT NOT NULL,
    name TEXT NOT NULL,
    category_name TEXT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_professional_role_hh_professional_role_id UNIQUE (hh_professional_role_id)
);

CREATE TABLE dictionary_sync_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dictionary_name TEXT NOT NULL,
    status TEXT NOT NULL,
    etag TEXT NULL,
    source_status_code INT NULL,
    notes TEXT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_dictionary_sync_run_name
    ON dictionary_sync_run(dictionary_name);

CREATE INDEX idx_dictionary_sync_run_started_at
    ON dictionary_sync_run(started_at DESC);

-- =========================================================
-- domain core
-- =========================================================

CREATE TABLE employer (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_employer_id TEXT NOT NULL,
    name TEXT NOT NULL,
    alternate_url TEXT NULL,
    site_url TEXT NULL,
    area_id UUID NULL REFERENCES area(id) ON DELETE SET NULL,
    is_trusted BOOLEAN NULL,
    raw_first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_employer_hh_employer_id UNIQUE (hh_employer_id)
);

CREATE INDEX idx_employer_name
    ON employer(name);

CREATE INDEX idx_employer_area_id
    ON employer(area_id);

CREATE TABLE vacancy (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hh_vacancy_id TEXT NOT NULL,
    employer_id UUID NULL REFERENCES employer(id) ON DELETE SET NULL,
    area_id UUID NULL REFERENCES area(id) ON DELETE SET NULL,
    name_current TEXT NOT NULL,
    published_at TIMESTAMPTZ NULL,
    created_at_hh TIMESTAMPTZ NULL,
    archived_at_hh TIMESTAMPTZ NULL,
    alternate_url TEXT NULL,
    employment_type_code TEXT NULL,
    schedule_type_code TEXT NULL,
    experience_code TEXT NULL,
    source_type TEXT NOT NULL DEFAULT 'hh_api',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_vacancy_hh_vacancy_id UNIQUE (hh_vacancy_id)
);

CREATE INDEX idx_vacancy_employer_id
    ON vacancy(employer_id);

CREATE INDEX idx_vacancy_area_id
    ON vacancy(area_id);

CREATE INDEX idx_vacancy_published_at
    ON vacancy(published_at DESC);

CREATE TABLE vacancy_professional_role (
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    professional_role_id UUID NOT NULL REFERENCES professional_role(id) ON DELETE CASCADE,
    PRIMARY KEY (vacancy_id, professional_role_id)
);

CREATE INDEX idx_vacancy_prof_role_role_id
    ON vacancy_professional_role(professional_role_id);

-- =========================================================
-- history / state tracking
-- =========================================================

CREATE TABLE vacancy_seen_event (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    crawl_run_id UUID NOT NULL REFERENCES crawl_run(id) ON DELETE CASCADE,
    crawl_partition_id UUID NOT NULL REFERENCES crawl_partition(id) ON DELETE CASCADE,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    list_position INT NULL,
    short_hash TEXT NOT NULL,
    short_payload_ref_id BIGINT NULL REFERENCES raw_api_payload(id) ON DELETE SET NULL,
    CONSTRAINT uq_vse_seen UNIQUE (vacancy_id, crawl_partition_id, seen_at)
);

CREATE INDEX idx_vacancy_seen_event_vacancy_id
    ON vacancy_seen_event(vacancy_id);

CREATE INDEX idx_vacancy_seen_event_run_id
    ON vacancy_seen_event(crawl_run_id);

CREATE INDEX idx_vacancy_seen_event_partition_id
    ON vacancy_seen_event(crawl_partition_id);

CREATE INDEX idx_vacancy_seen_event_seen_at
    ON vacancy_seen_event(seen_at DESC);

CREATE TABLE vacancy_current_state (
    vacancy_id UUID PRIMARY KEY REFERENCES vacancy(id) ON DELETE CASCADE,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    seen_count INT NOT NULL DEFAULT 1,
    consecutive_missing_runs INT NOT NULL DEFAULT 0,
    is_probably_inactive BOOLEAN NOT NULL DEFAULT FALSE,
    last_seen_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    last_short_hash TEXT NULL,
    last_detail_hash TEXT NULL,
    last_detail_fetched_at TIMESTAMPTZ NULL,
    detail_fetch_status TEXT NOT NULL DEFAULT 'not_requested',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_vacancy_current_state_last_seen_at
    ON vacancy_current_state(last_seen_at DESC);

CREATE INDEX idx_vacancy_current_state_inactive
    ON vacancy_current_state(is_probably_inactive);

CREATE INDEX idx_vacancy_current_state_detail_status
    ON vacancy_current_state(detail_fetch_status);

CREATE TABLE vacancy_snapshot (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    snapshot_type TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    crawl_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    short_hash TEXT NULL,
    detail_hash TEXT NULL,
    short_payload_ref_id BIGINT NULL REFERENCES raw_api_payload(id) ON DELETE SET NULL,
    detail_payload_ref_id BIGINT NULL REFERENCES raw_api_payload(id) ON DELETE SET NULL,
    normalized_json JSONB NULL,
    change_reason TEXT NULL
);

CREATE INDEX idx_vacancy_snapshot_vacancy_id
    ON vacancy_snapshot(vacancy_id);

CREATE INDEX idx_vacancy_snapshot_captured_at
    ON vacancy_snapshot(captured_at DESC);

CREATE INDEX idx_vacancy_snapshot_detail_hash
    ON vacancy_snapshot(detail_hash);

CREATE TABLE detail_fetch_attempt (
    id BIGSERIAL PRIMARY KEY,
    vacancy_id UUID NOT NULL REFERENCES vacancy(id) ON DELETE CASCADE,
    crawl_run_id UUID NULL REFERENCES crawl_run(id) ON DELETE SET NULL,
    reason TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL,
    error_message TEXT NULL
);

CREATE INDEX idx_detail_fetch_attempt_vacancy_id
    ON detail_fetch_attempt(vacancy_id);

CREATE INDEX idx_detail_fetch_attempt_status
    ON detail_fetch_attempt(status);

CREATE INDEX idx_detail_fetch_attempt_requested_at
    ON detail_fetch_attempt(requested_at DESC);

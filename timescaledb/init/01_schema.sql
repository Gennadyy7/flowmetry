CREATE TABLE IF NOT EXISTS application (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    owner_id INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_application_name ON application (name);

CREATE TABLE metrics_info (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    type TEXT NOT NULL CHECK (type IN ('counter', 'gauge', 'histogram')),
    attributes JSONB NOT NULL,

    explicit_bounds DOUBLE PRECISION[],
    application_id BIGINT REFERENCES application(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX CONCURRENTLY idx_metrics_info_unique
ON metrics_info (
    name,
    attributes,
    COALESCE(explicit_bounds, '{}'::DOUBLE PRECISION[])
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_metrics_info_app_id ON metrics_info (application_id);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER,
    session_key VARCHAR(64),
    action TEXT NOT NULL CHECK (
        action IN ('login', 'view_metrics', 'export_data', 'view_applications', 'view_audit')
    ),
    target TEXT,
    ip_address INET,
    user_agent TEXT,
    status_code SMALLINT,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_user ON audit_log (user_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_timestamp ON audit_log (timestamp DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_session ON audit_log (session_key);
CREATE TABLE metrics_info (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    unit TEXT,
    type TEXT NOT NULL CHECK (type IN ('counter', 'gauge', 'histogram')),
    attributes JSONB NOT NULL,

    explicit_bounds DOUBLE PRECISION[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX CONCURRENTLY idx_metrics_info_unique
ON metrics_info (
    name,
    attributes,
    COALESCE(explicit_bounds, '{}'::DOUBLE PRECISION[])
);
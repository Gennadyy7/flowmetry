CREATE TABLE metrics_values (
    time TIMESTAMPTZ NOT NULL,
    metric_id BIGINT NOT NULL REFERENCES metrics_info(id) ON DELETE CASCADE,
    value DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('metrics_values', 'time');

CREATE TABLE metrics_histograms (
    time TIMESTAMPTZ NOT NULL,
    metric_id BIGINT NOT NULL REFERENCES metrics_info(id) ON DELETE CASCADE,
    sum DOUBLE PRECISION NOT NULL,
    count BIGINT NOT NULL,
    bucket_counts INT[] NOT NULL
);

SELECT create_hypertable('metrics_histograms', 'time');
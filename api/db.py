from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import json
import logging
from typing import Any, cast

import asyncpg

from api.config import settings
from api.schemas import DBMetric, MetricType

logger = logging.getLogger(__name__)


class TimescaleDB:
    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None
        logger.info('The database is initialized')

    async def connect(self) -> None:
        if self._pool is not None:
            logger.debug('TimescaleDB pool already exists')
            return
        logger.info(
            'Connecting to TimescaleDB',
            extra={
                'host': settings.DB_HOST,
                'port': settings.DB_PORT,
                'database': settings.POSTGRES_DB,
                'user': settings.POSTGRES_USER,
            },
        )
        try:
            self._pool = await asyncpg.create_pool(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                min_size=settings.DB_MIN_POOL_SIZE,
                max_size=settings.DB_MAX_POOL_SIZE,
                command_timeout=settings.DB_COMMAND_TIMEOUT,
                ssl=settings.DB_SSL_MODE,
            )
            logger.info('Connected to TimescaleDB')
        except Exception:
            logger.exception('Failed to connect to TimescaleDB')
            raise

    async def close(self) -> None:
        if self._pool is not None:
            logger.info('Closing TimescaleDB connection pool')
            await self._pool.close()
            self._pool = None
            logger.info('TimescaleDB connection pool closed')

    @asynccontextmanager
    async def _get_connection(self) -> AsyncIterator[asyncpg.pool.PoolConnectionProxy]:
        if self._pool is None:
            raise RuntimeError('TimescaleDB not connected')
        async with self._pool.acquire() as conn:
            yield conn

    @staticmethod
    def _parse_attributes(raw_attrs: Any) -> dict[str, Any]:
        if isinstance(raw_attrs, str):
            parsed = json.loads(raw_attrs)
            if not isinstance(parsed, dict):
                raise ValueError(
                    f'Expected JSON object, got {type(parsed).__name__}: {parsed}'
                )
            return cast(dict[str, Any], parsed)
        if hasattr(raw_attrs, 'keys'):
            return {str(k): v for k, v in raw_attrs.items()}
        raise ValueError(f'Unsupported attributes type: {type(raw_attrs)}')

    async def fetch_all_metrics(self, lookback_minutes: int = 5) -> list[DBMetric]:
        async with self._get_connection() as conn:
            values = await conn.fetch(
                """
                SELECT
                    i.name,
                    i.description,
                    i.unit,
                    i.type,
                    i.attributes,
                    v.value,
                    v.time
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE v.time >= NOW() - ($1 * INTERVAL '1 minute')
                ORDER BY v.time DESC
                """,
                lookback_minutes,
            )

            histograms = await conn.fetch(
                """
                SELECT
                    i.name,
                    i.description,
                    i.unit,
                    i.attributes,
                    h.sum,
                    h.count,
                    h.bucket_counts,
                    i.explicit_bounds,
                    h.time
                FROM metrics_info i
                JOIN metrics_histograms h ON i.id = h.metric_id
                WHERE h.time >= NOW() - ($1 * INTERVAL '1 minute')
                ORDER BY h.time DESC
                """,
                lookback_minutes,
            )

        result: list[DBMetric] = []
        for row in values:
            result.append(
                DBMetric(
                    name=row['name'],
                    description=row['description'] or '',
                    unit=row['unit'] or '',
                    type=MetricType(row['type']),
                    attributes=self._parse_attributes(row['attributes']),
                    value=float(row['value']),
                    time=row['time'],
                )
            )
        for row in histograms:
            result.append(
                DBMetric(
                    name=row['name'],
                    description=row['description'] or '',
                    unit=row['unit'] or '',
                    type=MetricType.HISTOGRAM,
                    attributes=self._parse_attributes(row['attributes']),
                    sum=float(row['sum']),
                    count=int(row['count']),
                    bucket_counts=list(row['bucket_counts'] or []),
                    explicit_bounds=list(row['explicit_bounds'] or []),
                    time=row['time'],
                )
            )
        return result

    async def fetch_metric_instant(
        self,
        metric_name: str,
        labels: dict[str, str],
        timestamp: float,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'

        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (i.id)
                    i.name,
                    i.attributes,
                    v.value,
                    v.time
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type IN ('counter', 'gauge')
                  AND v.time <= to_timestamp($3)
                ORDER BY i.id, v.time DESC
                """,
                metric_name,
                labels_json,
                timestamp,
            )

        return [
            (
                row['name'],
                self._parse_attributes(row['attributes']),
                float(row['value']),
                row['time'],
            )
            for row in rows
            if row['value'] is not None
        ]

    async def fetch_timeseries_gauge(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
        step_seconds: int,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'
        step_interval = timedelta(seconds=step_seconds)

        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    i.name,
                    i.attributes,
                    time_bucket($4::interval, v.time) AS bucket,
                    AVG(v.value) AS value
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'gauge'
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($5)
                GROUP BY i.id, i.name, i.attributes, bucket
                ORDER BY bucket ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                step_interval,
                end_ts,
            )

        return [
            (
                row['name'],
                self._parse_attributes(row['attributes']),
                float(row['value']),
                row['bucket'],
            )
            for row in rows
            if row['value'] is not None
        ]

    async def fetch_timeseries_rate(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
        step_seconds: int,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'
        step_interval = timedelta(seconds=step_seconds)

        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    i.name,
                    i.attributes,
                    time_bucket($4::interval, v.time) AS bucket,
                    (last(v.value, v.time) - first(v.value, v.time)) / $5 AS value
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'counter'
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($6)
                GROUP BY i.id, i.name, i.attributes, bucket
                ORDER BY bucket ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                step_interval,
                step_seconds,
                end_ts,
            )

        return [
            (
                row['name'],
                self._parse_attributes(row['attributes']),
                float(row['value']),
                row['bucket'],
            )
            for row in rows
            if row['value'] is not None and row['value'] >= 0
        ]

    async def fetch_all_label_names(self) -> list[str]:
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                'SELECT DISTINCT jsonb_object_keys(attributes) AS key FROM metrics_info'
            )
            return sorted({'__name__'} | {row['key'] for row in rows})

    async def fetch_label_values(self, label_name: str) -> list[str]:
        async with self._get_connection() as conn:
            if label_name == '__name__':
                rows = await conn.fetch('SELECT DISTINCT name FROM metrics_info')
                return sorted({row['name'] for row in rows})
            rows = await conn.fetch(
                'SELECT DISTINCT value FROM metrics_info, jsonb_each_text(attributes) WHERE key = $1',
                label_name,
            )
            return sorted({row['value'] for row in rows})


timescale_db = TimescaleDB()

from datetime import datetime
import json
import logging
from typing import Any

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

    async def fetch_all_metrics(self, lookback_minutes: int = 5) -> list[DBMetric]:
        if self._pool is None:
            raise RuntimeError('TimescaleDB not connected')

        async with self._pool.acquire() as conn:
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
            attrs = row['attributes']
            if isinstance(attrs, str):
                attrs = json.loads(attrs)
            result.append(
                DBMetric(
                    name=row['name'],
                    description=row['description'] or '',
                    unit=row['unit'] or '',
                    type=MetricType(row['type']),
                    attributes=attrs,
                    value=float(row['value']),
                    time=row['time'],
                )
            )
        for row in histograms:
            attrs = row['attributes']
            if isinstance(attrs, str):
                attrs = json.loads(attrs)
            result.append(
                DBMetric(
                    name=row['name'],
                    description=row['description'] or '',
                    unit=row['unit'] or '',
                    type=MetricType.HISTOGRAM,
                    attributes=attrs,
                    sum=float(row['sum']),
                    count=int(row['count']),
                    bucket_counts=list(row['bucket_counts'])
                    if row['bucket_counts']
                    else [],
                    explicit_bounds=list(row['explicit_bounds'])
                    if row['explicit_bounds']
                    else [],
                    time=row['time'],
                )
            )
        return result

    async def fetch_metric_timeseries(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        if self._pool is None:
            raise RuntimeError('TimescaleDB not connected')

        labels_json = json.dumps(labels) if labels else '{}'

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    i.name,
                    i.attributes,
                    v.value,
                    v.time
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($4)
                ORDER BY v.time ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                end_ts,
            )

        result = []
        for row in rows:
            attrs = row['attributes']
            if isinstance(attrs, str):
                attrs = json.loads(attrs)
            result.append((row['name'], attrs, float(row['value']), row['time']))
        return result

    async def fetch_all_label_names(self) -> list[str]:
        if self._pool is None:
            raise RuntimeError('TimescaleDB not connected')
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT jsonb_object_keys(attributes) AS key
                FROM metrics_info;
            """)
            labels = {row['key'] for row in rows}
            labels.add('__name__')
            return sorted(labels)

    async def fetch_label_values(self, label_name: str) -> list[str]:
        if self._pool is None:
            raise RuntimeError('TimescaleDB not connected')
        async with self._pool.acquire() as conn:
            if label_name == '__name__':
                rows = await conn.fetch('SELECT DISTINCT name FROM metrics_info;')
                return sorted({row['name'] for row in rows})
            rows = await conn.fetch(
                """
                SELECT DISTINCT value
                FROM metrics_info, jsonb_each_text(attributes)
                WHERE key = $1;
            """,
                label_name,
            )
            return sorted({row['value'] for row in rows})


timescale_db = TimescaleDB()

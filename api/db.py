import json
import logging

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
                WHERE v.time >= NOW() - INTERVAL '%s minutes'
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
                WHERE h.time >= NOW() - INTERVAL '%s minutes'
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


timescale_db = TimescaleDB()

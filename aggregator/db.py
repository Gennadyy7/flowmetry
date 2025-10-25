import logging
from typing import cast

import asyncpg

from aggregator.config import settings
from aggregator.schemas import MetricPoint, MetricType

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

    async def insert_metric(self, point: MetricPoint) -> None:
        if self._pool is None:
            raise RuntimeError('TimescaleDB not connected')
        timestamp_sec = point.timestamp_nano / 1_000_000_000.0
        async with self._pool.acquire() as conn:
            metric_id = await self._get_or_create_metric_id(conn, point)
            if point.type in (MetricType.COUNTER, MetricType.GAUGE):
                if point.value is None:
                    raise ValueError(f"{point.type.value} metric must have 'value'")
                await conn.execute(
                    """
                    INSERT INTO metrics_values (time, metric_id, value)
                    VALUES (to_timestamp($1), $2, $3)
                    """,
                    timestamp_sec,
                    metric_id,
                    float(point.value),
                )
            elif point.type == MetricType.HISTOGRAM:
                if (
                    point.sum is None
                    or point.count is None
                    or point.bucket_counts is None
                ):
                    raise ValueError(
                        "Histogram metric must have 'sum', 'count', and 'bucket_counts'"
                    )
                await conn.execute(
                    """
                    INSERT INTO metrics_histograms (time, metric_id, sum, count, bucket_counts)
                    VALUES (to_timestamp($1), $2, $3, $4, $5)
                    """,
                    timestamp_sec,
                    metric_id,
                    point.sum,
                    point.count,
                    point.bucket_counts,
                )
        logger.debug(
            'Metric inserted into TimescaleDB',
            extra={'metric_name': point.name, 'type': point.type.value},
        )

    @staticmethod
    async def _get_or_create_metric_id(
        conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy, point: MetricPoint
    ) -> int:
        explicit_bounds = (
            point.explicit_bounds if point.type == MetricType.HISTOGRAM else None
        )
        stmt = """
            INSERT INTO metrics_info (
                name, description, unit, type, attributes, explicit_bounds
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ON CONFLICT (name, attributes, COALESCE(explicit_bounds, '{}'::DOUBLE PRECISION[]))
            DO NOTHING
            RETURNING id;
        """
        row = await conn.fetchrow(
            stmt,
            point.name,
            point.description,
            point.unit,
            point.type.value,
            point.attributes,
            explicit_bounds,
        )
        if row is not None:
            return cast(int, row['id'])
        existing = await conn.fetchrow(
            """
            SELECT id FROM metrics_info
            WHERE name = $1
              AND attributes = $2
              AND COALESCE(explicit_bounds, '{}'::DOUBLE PRECISION[]) = COALESCE($3, '{}'::DOUBLE PRECISION[])
            """,
            point.name,
            point.attributes,
            explicit_bounds,
        )
        if existing is None:
            raise RuntimeError('Failed to get or create metric_id')
        return cast(int, existing['id'])

    async def close(self) -> None:
        if self._pool is not None:
            logger.info('Closing TimescaleDB connection pool')
            await self._pool.close()
            self._pool = None
            logger.info('TimescaleDB connection pool closed')


timescale_db = TimescaleDB()

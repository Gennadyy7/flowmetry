from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
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

    async def fetch_series(
        self,
        matchers: list[str] | None = None,
    ) -> list[dict[str, str]]:
        async with self._get_connection() as conn:
            if matchers:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT name, attributes
                    FROM metrics_info
                    WHERE name = ANY($1)
                    """,
                    matchers,
                )
            else:
                rows = await conn.fetch(
                    'SELECT DISTINCT name, attributes FROM metrics_info'
                )

        result = []
        for row in rows:
            labels = {'__name__': row['name']}
            labels.update(self._parse_attributes(row['attributes']))
            result.append({k: str(v) for k, v in labels.items()})
        return result

    async def fetch_counter_raw_values(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
    ) -> list[tuple[datetime, float]]:
        labels_json = json.dumps(labels) if labels else '{}'
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT v.time, v.value
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'counter'
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($4)
                ORDER BY v.time ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                end_ts,
            )
        return [
            (row['time'], float(row['value']))
            for row in rows
            if row['value'] is not None
        ]

    async def fetch_gauge_values(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
    ) -> list[tuple[datetime, float]]:
        labels_json = json.dumps(labels) if labels else '{}'
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT v.time, v.value
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'gauge'
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($4)
                ORDER BY v.time ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                end_ts,
            )
        return [
            (row['time'], float(row['value']))
            for row in rows
            if row['value'] is not None
        ]

    async def fetch_metric_instant(
        self,
        metric_name: str,
        labels: dict[str, str],
        timestamp: float,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'

        logger.debug(
            'Executing SQL',
            extra={
                'sql': """
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
                'params': [metric_name, labels_json, timestamp],
            },
        )

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

    async def fetch_timeseries_for_range(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
        step_seconds: int,
        function: str = 'raw',
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        if function in ('rate', 'increase'):
            return await self._fetch_counter_rate_or_increase(
                metric_name, labels, start_ts, end_ts, step_seconds, function
            )

        metric_type = await self._get_metric_type(metric_name, labels)

        if metric_type == 'gauge':
            return await self._fetch_gauge_aggregated(
                metric_name, labels, start_ts, end_ts, step_seconds
            )
        elif metric_type == 'counter':
            return await self._fetch_counter_raw_aggregated(
                metric_name, labels, start_ts, end_ts, step_seconds
            )
        else:  # histogram — not supported in raw yet
            return []

    async def _get_metric_type(
        self,
        metric_name: str,
        labels: dict[str, str],
    ) -> str:
        labels_json = json.dumps(labels) if labels else '{}'
        async with self._get_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT type FROM metrics_info
                WHERE name = $1 AND attributes @> $2::jsonb
                LIMIT 1
                """,
                metric_name,
                labels_json,
            )
        if not row:
            raise ValueError(f"Metric '{metric_name}' with labels {labels} not found")
        return str(row['type'])

    async def _fetch_counter_raw_aggregated(
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
                    last(v.value, v.time) AS value  -- Последнее значение в бакете
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'counter'
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

    async def _fetch_gauge_aggregated(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
        step_seconds: int,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'
        step_interval = timedelta(seconds=step_seconds)

        logger.debug(
            'Executing gauge query',
            extra={
                'metric_name': metric_name,
                'labels_json': labels_json,
                'start_ts': start_ts,
                'end_ts': end_ts,
                'step_interval': str(step_interval),
            },
        )

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

        logger.debug(f'Gauge query returned {len(rows)} rows')

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

    async def _fetch_counter_rate_or_increase(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
        step_seconds: int,
        function: str,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        raw_points = await self.fetch_counter_raw_values(
            metric_name, labels, start_ts, end_ts
        )
        if not raw_points:
            return []

        start_dt = datetime.fromtimestamp(start_ts, tz=UTC)
        end_dt = datetime.fromtimestamp(end_ts, tz=UTC)
        current = start_dt

        result = []
        while current <= end_dt:
            bucket_end = current + timedelta(seconds=step_seconds)
            window_points = [
                (ts, val) for ts, val in raw_points if current <= ts <= bucket_end
            ]
            if len(window_points) >= 2:
                if function == 'rate':
                    value = self.calculate_rate_with_resets(window_points, step_seconds)
                else:
                    value = self.calculate_increase_with_resets(window_points)
            elif window_points:
                value = 0.0
            else:
                current = bucket_end
                continue

            result.append((metric_name, labels, value, current))
            current = bucket_end

        return result

    @staticmethod
    def calculate_rate_with_resets(
        points: Sequence[tuple[datetime, float]],
        window_seconds: float,
    ) -> float:
        if len(points) < 2:
            return 0.0

        total_delta = 0.0
        for i in range(1, len(points)):
            prev_val = points[i - 1][1]
            curr_val = points[i][1]
            delta = curr_val - prev_val
            if delta < 0:
                delta = curr_val
            total_delta += delta

        return total_delta / window_seconds

    @staticmethod
    def calculate_increase_with_resets(
        points: Sequence[tuple[datetime, float]],
    ) -> float:
        if len(points) < 2:
            return 0.0

        total_delta = 0.0
        for i in range(1, len(points)):
            prev_val = points[i - 1][1]
            curr_val = points[i][1]
            delta = curr_val - prev_val
            if delta < 0:
                delta = curr_val
            total_delta += delta

        return total_delta

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

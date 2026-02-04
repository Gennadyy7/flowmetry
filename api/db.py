from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
import json
import logging
from typing import Any, cast

import asyncpg

from api.config import settings

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
        lookback_seconds: int = 0,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        if function in ('rate', 'increase'):
            if lookback_seconds <= 0:
                raise ValueError(
                    f"Parameter 'lookback_seconds' must be positive for function '{function}'"
                )
            return await self._fetch_counter_rate_or_increase(
                metric_name,
                labels,
                start_ts,
                end_ts,
                step_seconds,
                function,
                lookback_seconds,
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
                SELECT DISTINCT ON (i.id, time_bucket($4::interval, v.time))
                    i.name,
                    i.attributes,
                    time_bucket($4::interval, v.time) AS bucket,
                    v.value
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'counter'
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($5)
                ORDER BY i.id, time_bucket($4::interval, v.time), v.time DESC
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
                SELECT DISTINCT ON (i.id, time_bucket($4::interval, v.time))
                    i.name,
                    i.attributes,
                    time_bucket($4::interval, v.time) AS bucket,
                    v.value
                FROM metrics_info i
                JOIN metrics_values v ON i.id = v.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'gauge'
                  AND v.time >= to_timestamp($3)
                  AND v.time <= to_timestamp($5)
                ORDER BY i.id, time_bucket($4::interval, v.time), v.time DESC
                """,
                metric_name,
                labels_json,
                start_ts,
                step_interval,
                end_ts,
            )

        logger.debug(
            f'Gauge query returned {len(rows)} rows',
            extra={'rows_sample': rows[:3] if rows else []},
        )

        result = [
            (
                row['name'],
                self._parse_attributes(row['attributes']),
                float(row['value']),
                row['bucket'],
            )
            for row in rows
            if row['value'] is not None
        ]

        if result and logger.isEnabledFor(logging.DEBUG):
            values = [val for _, _, val, _ in result]
            logger.debug(
                'Gauge values statistics',
                extra={
                    'count': len(values),
                    'min': min(values) if values else None,
                    'max': max(values) if values else None,
                    'avg': sum(values) / len(values) if values else None,
                    'first_few': values[:5],
                },
            )

        return result

    async def _fetch_counter_rate_or_increase(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
        step_seconds: int,
        function: str,
        lookback_seconds: int,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'
        async with self._get_connection() as conn:
            metric_rows = await conn.fetch(
                """
                SELECT id, name, attributes
                FROM metrics_info
                WHERE name = $1
                  AND attributes @> $2::jsonb
                  AND type = 'counter'
                """,
                metric_name,
                labels_json,
            )
        if not metric_rows:
            return []

        metric_ids = [row['id'] for row in metric_rows]

        all_points_dict = await self.fetch_counter_raw_values_for_metrics_batch(
            metric_ids, start_ts - lookback_seconds, end_ts
        )

        result = []
        start_dt = datetime.fromtimestamp(start_ts, tz=UTC)
        end_dt = datetime.fromtimestamp(end_ts, tz=UTC)
        lookback_delta = timedelta(seconds=lookback_seconds)

        for metric_row in metric_rows:
            metric_id = metric_row['id']
            metric_attrs = self._parse_attributes(metric_row['attributes'])

            raw_points = all_points_dict.get(metric_id, [])
            if not raw_points:
                continue

            current = start_dt
            while current <= end_dt:
                window_start = current - lookback_delta
                window_end = current

                window_points = [
                    (ts, val)
                    for ts, val in raw_points
                    if window_start <= ts <= window_end
                ]

                if len(window_points) >= 2:
                    if function == 'rate':
                        value = self.calculate_counter_rate(
                            window_points, lookback_seconds, window_start, window_end
                        )
                    else:  # 'increase'
                        value = self.calculate_counter_increase(
                            window_points, window_start, window_end
                        )
                else:
                    value = 0.0

                result.append((metric_name, metric_attrs, value, current))
                current += timedelta(seconds=step_seconds)

        return result

    @classmethod
    def calculate_counter_rate(
        cls,
        points: Sequence[tuple[datetime, float]],
        range_seconds: float,
        range_start: datetime,
        range_end: datetime,
    ) -> float:
        if range_seconds <= 0:
            return 0.0

        raw_increase = cls.calculate_counter_increase_raw(points)
        extrapolated_increase = cls._apply_extrapolation(
            raw_increase, points, range_start, range_end, is_counter=True
        )

        return extrapolated_increase / range_seconds

    @staticmethod
    def calculate_counter_increase_raw(
        points: Sequence[tuple[datetime, float]],
    ) -> float:
        if len(points) < 2:
            return 0.0

        increase = points[-1][1] - points[0][1]
        for i in range(1, len(points)):
            if points[i][1] < points[i - 1][1]:
                increase += points[i - 1][1]
        return increase

    @staticmethod
    def _apply_extrapolation(
        raw_value: float,
        points: Sequence[tuple[datetime, float]],
        range_start: datetime,
        range_end: datetime,
        is_counter: bool = True,
    ) -> float:
        if len(points) < 2 or raw_value == 0.0:
            return raw_value

        first_t = points[0][0]
        last_t = points[-1][0]
        sampled_interval = (last_t - first_t).total_seconds()

        if sampled_interval <= 0:
            return raw_value

        num_samples_minus_one = len(points) - 1
        average_duration_between_samples = sampled_interval / num_samples_minus_one

        duration_to_start = abs((first_t - range_start).total_seconds())
        duration_to_end = abs((range_end - last_t).total_seconds())

        extrapolation_threshold = average_duration_between_samples * 1.1

        if duration_to_start >= extrapolation_threshold:
            duration_to_start = average_duration_between_samples / 2.0

        if is_counter and raw_value > 0 and points[0][1] >= 0:
            duration_to_zero = sampled_interval * (points[0][1] / raw_value)
            if duration_to_zero < duration_to_start:
                duration_to_start = duration_to_zero

        if duration_to_end >= extrapolation_threshold:
            duration_to_end = average_duration_between_samples / 2.0

        total_covered = sampled_interval + duration_to_start + duration_to_end
        factor = total_covered / sampled_interval

        return raw_value * factor

    @classmethod
    def calculate_counter_increase(
        cls,
        points: Sequence[tuple[datetime, float]],
        range_start: datetime,
        range_end: datetime,
    ) -> float:
        raw_increase = cls.calculate_counter_increase_raw(points)
        return cls._apply_extrapolation(
            raw_increase, points, range_start, range_end, is_counter=True
        )

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

    async def fetch_counter_raw_values_for_metrics_batch(
        self,
        metric_ids: list[int],
        start_ts: float,
        end_ts: float,
    ) -> dict[int, list[tuple[datetime, float]]]:
        if not metric_ids:
            return {}

        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT v.metric_id, v.time, v.value
                FROM metrics_values v
                WHERE v.metric_id = ANY($1)
                  AND v.time >= to_timestamp($2)
                  AND v.time <= to_timestamp($3)
                ORDER BY v.metric_id, v.time ASC
                """,
                metric_ids,
                start_ts,
                end_ts,
            )

        result: dict[int, list[tuple[datetime, float]]] = {}
        for row in rows:
            metric_id = row['metric_id']
            if metric_id not in result:
                result[metric_id] = []
            if row['value'] is not None:
                result[metric_id].append((row['time'], float(row['value'])))

        return result

    async def fetch_histogram_data(
        self,
        metric_name: str,
        labels: dict[str, str],
        start_ts: float,
        end_ts: float,
    ) -> list[tuple[str, dict[str, Any], list[int], float, int, list[float], datetime]]:
        labels_json = json.dumps(labels) if labels else '{}'

        logger.debug(
            'Fetching histogram data from DB',
            extra={
                'metric_name': metric_name,
                'start_ts': start_ts,
                'end_ts': end_ts,
                'labels_count': len(labels),
            },
        )

        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    i.name,
                    i.attributes,
                    i.explicit_bounds,
                    h.bucket_counts,
                    h.sum,
                    h.count,
                    h.time
                FROM metrics_info i
                JOIN metrics_histograms h ON i.id = h.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'histogram'
                  AND h.time >= to_timestamp($3)
                  AND h.time <= to_timestamp($4)
                ORDER BY h.time ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                end_ts,
            )

        logger.debug(
            f'Fetched {len(rows)} histogram data rows',
            extra={'metric_name': metric_name},
        )

        result = []
        for row in rows:
            if not row['bucket_counts'] or not row['explicit_bounds']:
                logger.warning(
                    'Skipping histogram row with invalid data',
                    extra={
                        'metric_name': row['name'],
                        'has_bucket_counts': bool(row['bucket_counts']),
                        'has_bounds': bool(row['explicit_bounds']),
                    },
                )
                continue

            result.append(
                (
                    row['name'],
                    self._parse_attributes(row['attributes']),
                    list(row['bucket_counts']),
                    float(row['sum']),
                    int(row['count']),
                    list(row['explicit_bounds']),
                    row['time'],
                )
            )

        logger.debug(
            f'Returned {len(result)} histogram data entries',
            extra={'metric_name': metric_name},
        )

        return result

    async def fetch_histogram_series_for_range(
        self,
        metric_name: str,
        component: str | None,
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
                    i.name AS base_name,
                    i.attributes,
                    i.explicit_bounds,
                    h.bucket_counts,
                    h.sum,
                    h.count,
                    time_bucket($5::interval, h.time) AS bucket_time
                FROM metrics_info i
                JOIN metrics_histograms h ON i.id = h.metric_id
                WHERE i.name = $1
                  AND i.attributes @> $2::jsonb
                  AND i.type = 'histogram'
                  AND h.time >= to_timestamp($3)
                  AND h.time <= to_timestamp($4)
                ORDER BY h.time ASC
                """,
                metric_name,
                labels_json,
                start_ts,
                end_ts,
                step_interval,
            )

        result = []
        for row in rows:
            if not row['bucket_counts'] or not row['explicit_bounds']:
                continue

            attrs = self._parse_attributes(row['attributes'])
            bounds = list(row['explicit_bounds'])
            bucket_counts = list(row['bucket_counts'])

            if component is None or component == 'bucket':
                cumulative = 0
                for i, bound in enumerate(bounds):
                    if i < len(bucket_counts):
                        cumulative += bucket_counts[i]
                        bucket_attrs = attrs.copy()
                        bucket_attrs['le'] = str(bound)
                        result.append(
                            (
                                f'{row["base_name"]}_bucket',
                                bucket_attrs,
                                float(cumulative),
                                row['bucket_time'],
                            )
                        )

                bucket_attrs = attrs.copy()
                bucket_attrs['le'] = '+Inf'
                result.append(
                    (
                        f'{row["base_name"]}_bucket',
                        bucket_attrs,
                        float(row['count']),
                        row['bucket_time'],
                    )
                )

            if component is None or component == 'sum':
                result.append(
                    (
                        f'{row["base_name"]}_sum',
                        attrs,
                        float(row['sum']),
                        row['bucket_time'],
                    )
                )

            if component is None or component == 'count':
                result.append(
                    (
                        f'{row["base_name"]}_count',
                        attrs,
                        float(row['count']),
                        row['bucket_time'],
                    )
                )

        return result

    async def is_histogram_metric(self, metric_name: str) -> bool:
        try:
            async with self._get_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT 1 FROM metrics_info
                    WHERE name = $1 AND type = 'histogram'
                    LIMIT 1
                    """,
                    metric_name,
                )
                return row is not None
        except Exception as e:
            logger.warning(
                f'Error checking if metric is histogram: {e}',
                extra={'metric_name': metric_name},
            )
            return False


timescale_db = TimescaleDB()

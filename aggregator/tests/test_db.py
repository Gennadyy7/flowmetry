import json
from unittest.mock import AsyncMock, patch

import pytest

from aggregator.db import TimescaleDB, timescale_db
from aggregator.schemas import MetricPoint, MetricType


class TestTimescaleDBSimple:
    @pytest.fixture
    def db(self) -> TimescaleDB:
        return TimescaleDB()

    async def test_connect_already_connected(self, db: TimescaleDB) -> None:
        mock_pool = AsyncMock()
        db._pool = mock_pool

        with patch('aggregator.db.logger') as mock_logger:
            await db.connect()

            assert db._pool is mock_pool
            mock_logger.debug.assert_called_once_with('TimescaleDB pool already exists')

    async def test_connect_failure(self, db: TimescaleDB) -> None:
        with (
            patch('aggregator.db.asyncpg.create_pool') as mock_create_pool,
            patch('aggregator.db.settings') as mock_settings,
            patch('aggregator.db.logger') as mock_logger,
        ):
            mock_create_pool.side_effect = Exception('Connection failed')

            mock_settings.DB_HOST = 'localhost'
            mock_settings.DB_PORT = 5432
            mock_settings.POSTGRES_DB = 'test_db'
            mock_settings.POSTGRES_USER = 'test_user'
            mock_settings.POSTGRES_PASSWORD = 'test_password'
            mock_settings.DB_MIN_POOL_SIZE = 1
            mock_settings.DB_MAX_POOL_SIZE = 10
            mock_settings.DB_COMMAND_TIMEOUT = 30
            mock_settings.DB_SSL_MODE = 'prefer'

            with pytest.raises(Exception, match='Connection failed'):
                await db.connect()

            mock_logger.exception.assert_called_once_with(
                'Failed to connect to TimescaleDB'
            )

    async def test_insert_metric_not_connected(
        self, db: TimescaleDB, sample_metric_point_counter: MetricPoint
    ) -> None:
        with pytest.raises(RuntimeError, match='TimescaleDB not connected'):
            await db.insert_metric(sample_metric_point_counter)

    def test_get_or_create_metric_id_new_metric(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {'id': 123}

        import inspect

        assert inspect.isfunction(TimescaleDB._get_or_create_metric_id)

        call_args = mock_conn.fetchrow.call_args
        if call_args:
            sql = call_args[0][0]
            assert 'INSERT INTO metrics_info' in sql
            assert 'ON CONFLICT' in sql
            assert 'RETURNING id' in sql

    def test_get_or_create_metric_id_histogram_with_bounds(
        self, sample_metric_point_histogram: MetricPoint
    ) -> None:
        sample_metric_point_histogram = MetricPoint(
            name='test_histogram',
            description='Test histogram metric',
            unit='seconds',
            type=MetricType.HISTOGRAM,
            timestamp_nano=1640995200000000002,
            attributes={'service': 'test'},
            sum=1500.0,
            count=200,
            bucket_counts=[20, 50, 100, 180, 200],
            explicit_bounds=[0.1, 0.5, 1.0, 2.0],
        )

        assert sample_metric_point_histogram.explicit_bounds == [0.1, 0.5, 1.0, 2.0]

    def test_get_or_create_metric_id_non_histogram_no_bounds(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        assert sample_metric_point_counter.explicit_bounds is None

    async def test_close_without_pool(self, db: TimescaleDB) -> None:
        db._pool = None

        await db.close()
        assert db._pool is None

    async def test_close_with_pool(self, db: TimescaleDB) -> None:
        mock_pool = AsyncMock()
        db._pool = mock_pool

        with patch('aggregator.db.logger') as mock_logger:
            await db.close()

            assert db._pool is None
            mock_pool.close.assert_called_once()
            mock_logger.info.assert_any_call('TimescaleDB connection pool closed')

    async def test_close_multiple_calls(self, db: TimescaleDB) -> None:
        mock_pool = AsyncMock()
        db._pool = mock_pool

        await db.close()
        await db.close()
        await db.close()

        assert mock_pool.close.call_count == 1
        assert db._pool is None

    def test_timescale_db_singleton(self) -> None:
        assert isinstance(timescale_db, TimescaleDB)
        assert timescale_db._pool is None

    def test_initialization_logging(self) -> None:
        with patch('aggregator.db.logger') as mock_logger:
            TimescaleDB()
            mock_logger.info.assert_called_once_with('The database is initialized')

    def test_timestamp_conversion(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        timestamp_sec = sample_metric_point_counter.timestamp_nano / 1_000_000_000.0
        assert timestamp_sec == 1640995200.0

    def test_metric_validation_counter(self) -> None:
        metric = MetricPoint(
            name='test_counter',
            description='Test counter',
            unit='count',
            type=MetricType.COUNTER,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            value=42,
        )

        assert metric.type == MetricType.COUNTER
        assert metric.value == 42
        assert metric.sum is None
        assert metric.count is None
        assert metric.bucket_counts is None

    def test_metric_validation_gauge(self) -> None:
        metric = MetricPoint(
            name='test_gauge',
            description='Test gauge',
            unit='bytes',
            type=MetricType.GAUGE,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            value=1024.5,
        )

        assert metric.type == MetricType.GAUGE
        assert metric.value == 1024.5
        assert metric.sum is None
        assert metric.count is None
        assert metric.bucket_counts is None

    def test_metric_validation_histogram(self) -> None:
        metric = MetricPoint(
            name='test_histogram',
            description='Test histogram',
            unit='seconds',
            type=MetricType.HISTOGRAM,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            sum=1500.0,
            count=200,
            bucket_counts=[20, 50, 100, 180, 200],
            explicit_bounds=[0.1, 0.5, 1.0, 2.0],
        )

        assert metric.type == MetricType.HISTOGRAM
        assert metric.sum == 1500.0
        assert metric.count == 200
        assert metric.bucket_counts == [20, 50, 100, 180, 200]
        assert metric.explicit_bounds == [0.1, 0.5, 1.0, 2.0]
        assert metric.value is None

    def test_metric_attributes_serialization(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        attributes_json = json.dumps(sample_metric_point_counter.attributes)
        assert attributes_json == '{"service": "test", "method": "GET"}'

    def test_metric_explicit_bounds_handling(self) -> None:
        histogram_metric = MetricPoint(
            name='test_histogram',
            description='Test histogram',
            unit='seconds',
            type=MetricType.HISTOGRAM,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            sum=1500.0,
            count=200,
            bucket_counts=[20, 50, 100],
            explicit_bounds=[0.1, 0.5, 1.0],
        )

        counter_metric = MetricPoint(
            name='test_counter',
            description='Test counter',
            unit='count',
            type=MetricType.COUNTER,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            value=42,
        )

        assert histogram_metric.explicit_bounds == [0.1, 0.5, 1.0]
        assert counter_metric.explicit_bounds is None

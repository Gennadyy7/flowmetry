import asyncpg
import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from aggregator.db import TimescaleDB
from aggregator.redis_stream_client import RedisStreamClient
from aggregator.schemas import MetricPoint, MetricType
from aggregator.worker import AggregationWorker


class TestIntegrationSimple:
    """Упрощенные интеграционные тесты с реальными Docker сервисами."""

    async def test_redis_and_postgresql_connection(self) -> None:
        """Тест подключения к обоим сервисам."""
        # Тест Redis
        redis_client = Redis(
            host='localhost',
            port=6380,
            db=0,
            decode_responses=False,
        )

        await redis_client.ping()
        await redis_client.aclose()

        # Тест PostgreSQL
        conn = await asyncpg.connect(
            host='localhost',
            port=5433,
            user='test_user',
            password='test_password',
            database='flowmetry_test_db',
        )

        result = await conn.fetchval('SELECT 1')
        assert result == 1

        await conn.close()

    async def test_timescaledb_with_real_data(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        """Тест TimescaleDB с реальными данными."""
        # Создаем pool подключений
        pool = await asyncpg.create_pool(
            host='localhost',
            port=5433,
            user='test_user',
            password='test_password',
            database='flowmetry_test_db',
            min_size=1,
            max_size=10,
        )

        try:
            db = TimescaleDB()
            db._pool = pool

            # Вставляем метрику
            await db.insert_metric(sample_metric_point_counter)

            # Проверяем, что метрика была вставлена
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT mv.time, mi.name, mv.value
                    FROM metrics_values mv
                    JOIN metrics_info mi ON mv.metric_id = mi.id
                    WHERE mi.name = $1
                    ORDER BY mv.time DESC
                    LIMIT 1
                    """,
                    sample_metric_point_counter.name,
                )

                assert result is not None
                assert result['name'] == sample_metric_point_counter.name
                assert result['value'] == sample_metric_point_counter.value

        finally:
            await pool.close()

    async def test_redis_stream_client_basic(self) -> None:
        """Базовый тест Redis Stream Client."""
        client = RedisStreamClient(
            stream_name='test_basic_stream',
            host='localhost',
            port=6380,
            db=0,
            group='test_basic_group',
            consumer='test_basic_consumer',
            password=None,
        )

        redis_client = Redis(
            host='localhost',
            port=6380,
            db=0,
            decode_responses=False,
        )

        try:
            # Запускаем клиент (создаст consumer group)
            await client.start()

            # Добавляем сообщение в stream
            test_data = {
                'name': 'test_basic_counter',
                'description': 'Test basic counter',
                'unit': 'count',
                'type': 'counter',
                'timestamp_nano': 1640995200000000000,
                'attributes': {'service': 'test'},
                'value': 100,
            }

            import json

            await redis_client.xadd(
                'test_basic_stream', {'data': json.dumps(test_data)}
            )

            # Проверяем, что stream существует
            streams = await redis_client.xinfo_stream('test_basic_stream')
            assert streams['length'] >= 1

            await redis_client.aclose()

        finally:
            await client.stop()

    async def test_worker_components_integration(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        """Тест интеграции компонентов worker без бесконечных циклов."""
        # Настраиваем DB
        db_pool = await asyncpg.create_pool(
            host='localhost',
            port=5433,
            user='test_user',
            password='test_password',
            database='flowmetry_test_db',
            min_size=1,
            max_size=10,
        )

        try:
            # Настраиваем Redis Stream Client
            redis_stream_client = RedisStreamClient(
                stream_name='test_worker_stream',
                host='localhost',
                port=6380,
                db=0,
                group='test_worker_group',
                consumer='test_worker_consumer',
                password=None,
            )

            await redis_stream_client.start()

            # Настраиваем DB
            db = TimescaleDB()
            db._pool = db_pool

            # Создаем Worker
            worker = AggregationWorker(redis_stream_client, db)

            # Проверяем, что worker инициализирован правильно
            assert worker.consumer is redis_stream_client
            assert worker.db is db

            # Тестируем сохранение метрики напрямую
            await db.insert_metric(sample_metric_point_counter)

            # Проверяем, что метрика сохранена
            async with db_pool.acquire() as conn:
                result = await conn.fetchrow(
                    """
                    SELECT mi.name, mv.value
                    FROM metrics_values mv
                    JOIN metrics_info mi ON mv.metric_id = mi.id
                    WHERE mi.name = $1
                    """,
                    sample_metric_point_counter.name,
                )

                assert result is not None
                assert result['value'] == sample_metric_point_counter.value

            await redis_stream_client.stop()

        finally:
            await db_pool.close()

    async def test_error_handling_with_real_services(self) -> None:
        """Тест обработки ошибок с реальными сервисами."""
        # Тест с неверными данными подключения
        with pytest.raises((RedisConnectionError, OSError)):
            bad_redis_client = Redis(
                host='localhost',
                port=9999,  # Неверный порт
                db=0,
                decode_responses=False,
            )
            await bad_redis_client.ping()

        # Тест с неверными данными PostgreSQL
        with pytest.raises((ConnectionRefusedError, OSError)):
            bad_conn = await asyncpg.connect(
                host='localhost',
                port=9999,  # Неверный порт
                user='test_user',
                password='test_password',
                database='flowmetry_test_db',
            )
            await bad_conn.fetchval('SELECT 1')

    async def test_multiple_metrics_in_database(
        self, sample_metric_point_counter: MetricPoint
    ) -> None:
        """Тест сохранения множества метрик в базе данных."""
        # Создаем pool подключений
        pool = await asyncpg.create_pool(
            host='localhost',
            port=5433,
            user='test_user',
            password='test_password',
            database='flowmetry_test_db',
            min_size=1,
            max_size=10,
        )

        try:
            db = TimescaleDB()
            db._pool = pool

            # Создаем несколько метрик с уникальными именами
            import time

            timestamp = int(time.time() * 1000000000)  # Уникальный timestamp

            metrics = [
                MetricPoint(
                    name=f'test_counter_{timestamp}',
                    description='Test counter metric',
                    unit='count',
                    type=MetricType.COUNTER,
                    timestamp_nano=timestamp,
                    attributes={'service': 'test'},
                    value=42,
                ),
                MetricPoint(
                    name=f'test_gauge_{timestamp}',
                    description='Test gauge metric',
                    unit='bytes',
                    type=MetricType.GAUGE,
                    timestamp_nano=timestamp + 1,
                    attributes={'service': 'test'},
                    value=1024.5,
                ),
                MetricPoint(
                    name=f'test_histogram_{timestamp}',
                    description='Test histogram metric',
                    unit='seconds',
                    type=MetricType.HISTOGRAM,
                    timestamp_nano=timestamp + 2,
                    attributes={'service': 'test'},
                    sum=1500.0,
                    count=200,
                    bucket_counts=[20, 50, 100, 180, 200],
                    explicit_bounds=[0.1, 0.5, 1.0, 2.0],
                ),
            ]

            # Вставляем все метрики
            for metric in metrics:
                await db.insert_metric(metric)

            # Проверяем, что все метрики сохранены
            async with pool.acquire() as conn:
                # Проверяем counter и gauge
                values_result = await conn.fetch(
                    """
                    SELECT mi.name, mv.value
                    FROM metrics_values mv
                    JOIN metrics_info mi ON mv.metric_id = mi.id
                    WHERE mi.name IN ($1, $2)
                    ORDER BY mi.name
                    """,
                    f'test_counter_{timestamp}',
                    f'test_gauge_{timestamp}',
                )

                assert len(values_result) == 2

                # Проверяем histogram
                histogram_result = await conn.fetchrow(
                    """
                    SELECT mh.sum, mh.count, mh.bucket_counts
                    FROM metrics_histograms mh
                    JOIN metrics_info mi ON mh.metric_id = mi.id
                    WHERE mi.name = $1
                    """,
                    f'test_histogram_{timestamp}',
                )

                assert histogram_result is not None
                assert histogram_result['sum'] == 1500.0
                assert histogram_result['count'] == 200

        finally:
            await pool.close()

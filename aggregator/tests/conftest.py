from collections.abc import Generator
import json
from unittest.mock import AsyncMock

import pytest

from aggregator.db import TimescaleDB
from aggregator.redis_stream_client import RedisStreamClient
from aggregator.schemas import MetricPoint, MetricType
from aggregator.worker import AggregationWorker

# ==================== METRIC FIXTURES ====================


@pytest.fixture
def sample_metric_point_counter() -> MetricPoint:
    """Стандартная метрика типа COUNTER для тестов."""
    return MetricPoint(
        name='test_counter',
        description='Test counter metric',
        unit='count',
        type=MetricType.COUNTER,
        timestamp_nano=1640995200000000000,  # 2022-01-01 00:00:00 UTC
        attributes={'service': 'test', 'method': 'GET'},
        value=42,
    )


@pytest.fixture
def sample_metric_point_gauge() -> MetricPoint:
    """Стандартная метрика типа GAUGE для тестов."""
    return MetricPoint(
        name='test_gauge',
        description='Test gauge metric',
        unit='bytes',
        type=MetricType.GAUGE,
        timestamp_nano=1640995200000000000,
        attributes={'service': 'test', 'endpoint': '/api'},
        value=1024,
    )


@pytest.fixture
def sample_metric_point_histogram() -> MetricPoint:
    """Стандартная метрика типа HISTOGRAM для тестов."""
    return MetricPoint(
        name='test_histogram',
        description='Test histogram metric',
        unit='seconds',
        type=MetricType.HISTOGRAM,
        timestamp_nano=1640995200000000000,
        attributes={'service': 'test', 'endpoint': '/api'},
        sum=125.5,
        count=10,
        bucket_counts=[1, 3, 6, 8, 10],
        explicit_bounds=[0.1, 0.5, 1.0, 2.0, 5.0],
    )


# ==================== MOCK FIXTURES ====================


@pytest.fixture
def mock_consumer() -> AsyncMock:
    """Мок RedisStreamClient для worker тестов."""
    consumer = AsyncMock(spec=RedisStreamClient)
    consumer.ensure_consumer_group.return_value = None
    return consumer


@pytest.fixture
def mock_db() -> AsyncMock:
    """Мок TimescaleDB для worker тестов."""
    db = AsyncMock(spec=TimescaleDB)
    db.insert_metric.return_value = None
    return db


@pytest.fixture
def worker(mock_consumer: AsyncMock, mock_db: AsyncMock) -> AggregationWorker:
    """Worker с замоканными зависимостями."""
    return AggregationWorker(mock_consumer, mock_db)


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Мок Redis клиента с базовыми методами."""
    redis_mock = AsyncMock()
    redis_mock.ping.return_value = True
    redis_mock.xgroup_create.return_value = None
    redis_mock.xreadgroup.return_value = []
    redis_mock.xack.return_value = 1
    redis_mock.xpending_range.return_value = []
    redis_mock.xclaim.return_value = []
    redis_mock.aclose.return_value = None
    return redis_mock


@pytest.fixture
def sample_redis_messages() -> list[tuple[str, dict[bytes, bytes]]]:
    """Пример Redis сообщений для тестов."""
    messages = [
        (
            '1640995200000-0',
            {
                b'data': json.dumps(
                    {
                        'name': 'test_counter',
                        'description': 'Test counter',
                        'unit': 'count',
                        'type': 'counter',
                        'timestamp_nano': 1640995200000000000,
                        'attributes': {'service': 'test'},
                        'value': 42,
                    }
                ).encode('utf-8')
            },
        ),
        (
            '1640995200001-0',
            {
                b'data': json.dumps(
                    {
                        'name': 'test_gauge',
                        'description': 'Test gauge',
                        'unit': 'bytes',
                        'type': 'gauge',
                        'timestamp_nano': 1640995201000000000,
                        'attributes': {'service': 'test'},
                        'value': 1024,
                    }
                ).encode('utf-8')
            },
        ),
    ]
    return messages


# ==================== DOCKER FIXTURES ====================


@pytest.fixture(scope='session')
def docker_test_services() -> Generator[None, None, None]:
    """Запускает тестовые Docker сервисы."""
    import subprocess
    import time

    # Запускаем тестовые сервисы
    subprocess.run(
        ['docker-compose', '-f', 'docker-compose.test.yml', 'up', '-d'], check=True
    )

    # Ждем запуска сервисов
    time.sleep(10)

    yield

    # Останавливаем сервисы
    subprocess.run(
        ['docker-compose', '-f', 'docker-compose.test.yml', 'down'], check=True
    )


# ==================== DATABASE FIXTURES ====================


@pytest.fixture
def timescale_db() -> TimescaleDB:
    """Экземпляр TimescaleDB для тестов."""
    return TimescaleDB()


@pytest.fixture
def mock_pool() -> AsyncMock:
    """Мок connection pool."""
    pool = AsyncMock()
    return pool


@pytest.fixture
def mock_connection() -> AsyncMock:
    """Мок connection."""
    conn = AsyncMock()
    return conn


# ==================== REDIS CLIENT FIXTURES ====================


@pytest.fixture
def redis_client() -> RedisStreamClient:
    """RedisStreamClient для тестов."""
    return RedisStreamClient(
        stream_name='test_stream',
        host='localhost',
        port=6379,
        db=0,
        group='test_group',
        consumer='test_consumer',
        password=None,
    )

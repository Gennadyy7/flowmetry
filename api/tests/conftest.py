from collections.abc import AsyncGenerator
from typing import cast

import asyncpg
import pytest

from api.db import TimescaleDB
from api.schemas import MetricType


@pytest.fixture
async def test_db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
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
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def clean_test_db(test_db_pool: asyncpg.Pool) -> AsyncGenerator[None, None]:
    async with test_db_pool.acquire() as conn:
        await conn.execute(
            'TRUNCATE TABLE metrics_values, metrics_histograms, metrics_info RESTART IDENTITY CASCADE'
        )
    yield
    async with test_db_pool.acquire() as conn:
        await conn.execute(
            'TRUNCATE TABLE metrics_values, metrics_histograms, metrics_info RESTART IDENTITY CASCADE'
        )


@pytest.fixture
async def sample_metric_id(test_db_pool: asyncpg.Pool) -> int:
    async with test_db_pool.acquire() as conn:
        metric_id = await conn.fetchval(
            """
            INSERT INTO metrics_info (name, description, unit, type, attributes)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            """,
            'test_metric',
            'Test metric',
            'count',
            MetricType.COUNTER.value,
            '{"service": "test"}',
        )
    return cast(int, metric_id)


@pytest.fixture
async def sample_histogram_metric_id(test_db_pool: asyncpg.Pool) -> int:
    async with test_db_pool.acquire() as conn:
        metric_id = await conn.fetchval(
            """
            INSERT INTO metrics_info (name, description, unit, type, attributes, explicit_bounds)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            """,
            'test_histogram',
            'Test histogram',
            'seconds',
            MetricType.HISTOGRAM.value,
            '{"service": "test"}',
            '{0.1, 0.5, 1.0, 5.0}',
        )
    return cast(int, metric_id)


@pytest.fixture
async def timescale_db() -> AsyncGenerator[TimescaleDB, None]:
    db = TimescaleDB()
    await db.connect()
    try:
        yield db
    finally:
        await db.close()

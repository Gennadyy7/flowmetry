import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ['API_HOST'] = 'localhost'
os.environ['API_PORT'] = '8000'
os.environ['API_RELOAD'] = 'False'
os.environ['REDIS_HOST'] = 'localhost'
os.environ['REDIS_PORT'] = '6379'
os.environ['REDIS_STREAM_NAME'] = 'test_metrics'
os.environ['SERVICE_NAME'] = 'flowmetry-collector-test'
os.environ['SERVICE_VERSION'] = '0.1.0'
os.environ['LOG_LEVEL'] = 'DEBUG'
os.environ['LOG_FORMAT'] = 'TEXT'

from fastapi.testclient import TestClient

from collector.main import app
from collector.otlp.schemas import (
    AnyValue,
    KeyValue,
    Metric,
    NumberDataPoint,
    OTLPMetricsRequest,
    Resource,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)


class TestRouter:
    @pytest.fixture
    def client(self) -> TestClient:
        return TestClient(app)

    @pytest.fixture
    def sample_otlp_request(self) -> OTLPMetricsRequest:
        return OTLPMetricsRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=Resource(
                        attributes=[
                            KeyValue(
                                key='service.name',
                                value=AnyValue(string_value='test-service'),
                            )
                        ]
                    ),
                    scope_metrics=[
                        ScopeMetrics(
                            metrics=[
                                Metric(
                                    name='test_counter',
                                    description='Test counter metric',
                                    unit='count',
                                    sum=Sum(
                                        data_points=[
                                            NumberDataPoint(
                                                attributes=[
                                                    KeyValue(
                                                        key='method',
                                                        value=AnyValue(
                                                            string_value='GET'
                                                        ),
                                                    )
                                                ],
                                                time_unix_nano='1234567890',
                                                as_int='42',
                                            )
                                        ],
                                        aggregation_temporality='cumulative',
                                        is_monotonic=True,
                                    ),
                                )
                            ]
                        )
                    ],
                )
            ]
        )

    def test_health_endpoint(self, client: TestClient) -> None:
        response = client.get('/health')

        assert response.status_code == 200
        assert response.json() == {'status': 'ok'}

    @pytest.mark.asyncio
    async def test_ingest_metrics_success(
        self, sample_otlp_request: OTLPMetricsRequest
    ) -> None:
        from collector.router import ingest_metrics

        with patch('collector.router.convert_otlp_to_internal') as mock_convert:
            with patch('collector.router.redis_stream_client') as mock_redis:
                mock_point = MagicMock()
                mock_point.model_dump.return_value = {
                    'name': 'test_counter',
                    'value': 42,
                }
                mock_convert.return_value = [mock_point]
                mock_redis.send_message = AsyncMock()

                result = await ingest_metrics(sample_otlp_request)

                assert result == {'received': 1}
                mock_convert.assert_called_once_with(sample_otlp_request)
                assert mock_redis.send_message.call_count == 1

    @pytest.mark.asyncio
    async def test_ingest_metrics_multiple_points(self) -> None:
        from collector.router import ingest_metrics

        with patch('collector.router.convert_otlp_to_internal') as mock_convert:
            with patch('collector.router.redis_stream_client') as mock_redis:
                mock_point1 = MagicMock()
                mock_point1.model_dump.return_value = {'name': 'metric1', 'value': 1}
                mock_point2 = MagicMock()
                mock_point2.model_dump.return_value = {'name': 'metric2', 'value': 2}
                mock_point3 = MagicMock()
                mock_point3.model_dump.return_value = {'name': 'metric3', 'value': 3}
                mock_convert.return_value = [mock_point1, mock_point2, mock_point3]
                mock_redis.send_message = AsyncMock()

                request = OTLPMetricsRequest(resource_metrics=[])
                result = await ingest_metrics(request)

                assert result == {'received': 3}
                assert mock_redis.send_message.call_count == 3

    @pytest.mark.asyncio
    async def test_ingest_metrics_conversion_error(self) -> None:
        from fastapi import HTTPException

        from collector.router import ingest_metrics

        with patch('collector.router.convert_otlp_to_internal') as mock_convert:
            mock_convert.side_effect = ValueError('Invalid OTLP format')

            request = OTLPMetricsRequest(resource_metrics=[])

            with pytest.raises(HTTPException) as exc_info:
                await ingest_metrics(request)

            assert exc_info.value.status_code == 400
            assert 'Invalid OTLP data: Invalid OTLP format' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_ingest_metrics_redis_error(
        self, sample_otlp_request: OTLPMetricsRequest
    ) -> None:
        from fastapi import HTTPException

        from collector.router import ingest_metrics

        with patch('collector.router.convert_otlp_to_internal') as mock_convert:
            with patch('collector.router.redis_stream_client') as mock_redis:
                mock_point = MagicMock()
                mock_point.model_dump.return_value = {'name': 'test', 'value': 1}
                mock_convert.return_value = [mock_point]
                mock_redis.send_message = AsyncMock(
                    side_effect=Exception('Redis connection failed')
                )

                with pytest.raises(HTTPException) as exc_info:
                    await ingest_metrics(sample_otlp_request)

                assert exc_info.value.status_code == 400
                assert (
                    'Invalid OTLP data: Redis connection failed'
                    in exc_info.value.detail
                )

    @pytest.mark.asyncio
    async def test_ingest_metrics_empty_request(self) -> None:
        from collector.router import ingest_metrics

        with patch('collector.router.convert_otlp_to_internal') as mock_convert:
            with patch('collector.router.redis_stream_client') as mock_redis:
                mock_convert.return_value = []
                mock_redis.send_message = AsyncMock()

                request = OTLPMetricsRequest(resource_metrics=[])
                result = await ingest_metrics(request)

                assert result == {'received': 0}
                mock_redis.send_message.assert_not_called()

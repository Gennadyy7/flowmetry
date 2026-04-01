from unittest.mock import AsyncMock, patch

import pytest

from api.promql_parser import ParsedQuery
from api.schemas import (
    LabelNamesResponse,
    LabelValuesResponse,
    SeriesResponse,
)
from api.services.prometheus import PrometheusService


class TestPrometheusService:
    async def test_get_build_info(self) -> None:
        result = PrometheusService.get_build_info()

        assert result.status == 'success'
        assert result.data.version == '0.1.0'
        assert result.data.revision == 'custom'
        assert result.data.branch == 'master'
        assert result.data.buildUser == 'flowmetry'
        assert result.data.goVersion == 'go1.21'
        assert result.data.platform == 'linux/amd64'

    @patch('api.services.prometheus.parser')
    async def test_handle_instant_query_parse_error(
        self, mock_parser: AsyncMock
    ) -> None:
        mock_parser.parse.side_effect = ValueError('Invalid query')

        with pytest.raises(ValueError, match='Invalid query'):
            await PrometheusService.handle_instant_query('invalid_query', 1234567890)

    @patch('api.services.prometheus.timescale_db')
    @patch('api.services.prometheus.parser')
    async def test_handle_instant_query_success(
        self, mock_parser: AsyncMock, mock_db: AsyncMock
    ) -> None:
        mock_parsed = ParsedQuery(raw='test_query', metric_name='test_metric')
        mock_parser.parse.return_value = mock_parsed
        mock_db.query_instant = AsyncMock(return_value=[])
        mock_db.is_histogram_metric = AsyncMock(return_value=False)
        mock_db.fetch_metric_instant = AsyncMock(return_value=[])

        result = await PrometheusService.handle_instant_query('test_query', 1234567890)

        assert result.data.result == []

    @patch('api.services.prometheus.timescale_db')
    @patch('api.services.prometheus.parser')
    async def test_handle_range_query_success(
        self, mock_parser: AsyncMock, mock_db: AsyncMock
    ) -> None:
        mock_parsed = ParsedQuery(raw='test_query', metric_name='test_metric')
        mock_parser.parse.return_value = mock_parsed
        mock_db.query_range = AsyncMock(return_value=[])
        mock_db.is_histogram_metric = AsyncMock(return_value=False)
        mock_db.fetch_timeseries_for_range = AsyncMock(return_value=[])

        result = await PrometheusService.handle_range_query(
            'test_query', 1234567890, 1234567990, 60
        )

        assert result.data.result == []

    @patch('api.services.prometheus.parser')
    async def test_handle_range_query_parse_error(self, mock_parser: AsyncMock) -> None:
        mock_parser.parse.side_effect = ValueError('Invalid query')

        with pytest.raises(ValueError, match='Invalid query'):
            await PrometheusService.handle_range_query(
                'invalid_query', 1234567890, 1234567990, 60
            )

    @patch('api.services.prometheus.timescale_db')
    async def test_get_series_success(self, mock_db: AsyncMock) -> None:
        mock_db.fetch_series = AsyncMock(
            return_value=[{'__name__': 'test_metric', 'label': 'value'}]
        )

        result = await PrometheusService.get_series(['test_metric'])

        assert isinstance(result, SeriesResponse)
        assert len(result.data) == 1
        assert result.data[0] == {'__name__': 'test_metric', 'label': 'value'}
        mock_db.fetch_series.assert_called_once_with(matchers=['test_metric'])

    @patch('api.services.prometheus.timescale_db')
    async def test_get_series_empty(self, mock_db: AsyncMock) -> None:
        mock_db.fetch_series = AsyncMock(return_value=[])

        result = await PrometheusService.get_series([])

        assert isinstance(result, SeriesResponse)
        assert len(result.data) == 0
        mock_db.fetch_series.assert_called_once_with(matchers=[])

    @patch('api.services.prometheus.timescale_db')
    async def test_get_label_names_success(self, mock_db: AsyncMock) -> None:
        mock_db.fetch_all_label_names = AsyncMock(
            return_value=['__name__', 'job', 'instance']
        )

        result = await PrometheusService.get_label_names()

        assert isinstance(result, LabelNamesResponse)
        assert result.data == ['__name__', 'job', 'instance']
        mock_db.fetch_all_label_names.assert_called_once()

    @patch('api.services.prometheus.timescale_db')
    async def test_get_label_values_success(self, mock_db: AsyncMock) -> None:
        mock_db.fetch_label_values = AsyncMock(return_value=['value1', 'value2'])

        result = await PrometheusService.get_label_values('job')

        assert isinstance(result, LabelValuesResponse)
        assert result.data == ['value1', 'value2']
        mock_db.fetch_label_values.assert_called_once_with('job')

    async def test_handle_instant_query_simple_metric(self) -> None:
        from api.db import timescale_db
        from api.services.prometheus import PrometheusService

        with patch.object(
            timescale_db, 'fetch_metric_instant', new_callable=AsyncMock
        ) as mock_get_data:
            mock_get_data.return_value = []

            result = await PrometheusService.handle_instant_query(
                'test_metric', 1640995200
            )

            assert result.status == 'success'
            assert result.data.resultType == 'vector'

    async def test_handle_range_query_simple_metric(self) -> None:
        from api.db import timescale_db
        from api.services.prometheus import PrometheusService

        with patch.object(
            timescale_db, 'fetch_timeseries_for_range', new_callable=AsyncMock
        ) as mock_get_data:
            mock_get_data.return_value = []

            result = await PrometheusService.handle_range_query(
                'test_metric', 1640995200, 1640995500, 60
            )

            assert result.status == 'success'
            assert result.data.resultType == 'matrix'

    async def test_get_series(self) -> None:
        from api.db import timescale_db
        from api.services.prometheus import PrometheusService

        with patch.object(
            timescale_db, 'fetch_series', new_callable=AsyncMock
        ) as mock_fetch:
            # Return data in the format expected by SeriesResponse
            mock_fetch.return_value = [
                {'__name__': 'test_metric', 'service': 'test', 'version': '1.0'}
            ]

            result = await PrometheusService.get_series(['test_metric'])

            assert result.status == 'success'
            assert len(result.data) == 1
            assert result.data[0]['__name__'] == 'test_metric'
            assert result.data[0]['service'] == 'test'
            assert result.data[0]['version'] == '1.0'

    async def test_get_label_names(self) -> None:
        from api.db import timescale_db
        from api.services.prometheus import PrometheusService

        with patch.object(
            timescale_db, 'fetch_all_label_names', new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = ['service', 'version', 'job']

            result = await PrometheusService.get_label_names()

            assert result.status == 'success'
            assert set(result.data) == {'service', 'version', 'job'}

    async def test_get_label_values(self) -> None:
        from api.db import timescale_db
        from api.services.prometheus import PrometheusService

        with patch.object(
            timescale_db, 'fetch_label_values', new_callable=AsyncMock
        ) as mock_fetch:
            mock_fetch.return_value = ['api', 'worker', 'collector']

            result = await PrometheusService.get_label_values('service')

            assert result.status == 'success'
            assert set(result.data) == {'api', 'worker', 'collector'}

    async def test_handle_instant_query_invalid_promql(self) -> None:
        with pytest.raises(ValueError):
            await PrometheusService.handle_instant_query('rate(', 1640995200)

    async def test_handle_range_query_invalid_promql(self) -> None:
        with pytest.raises(ValueError):
            await PrometheusService.handle_range_query(
                'rate(', 1640995200, 1640995500, 60
            )

    async def test_handle_instant_query_scalar_value(self) -> None:
        result = await PrometheusService.handle_instant_query('42', 1640995200)

        assert result.status == 'success'
        assert result.data.resultType == 'vector'
        assert len(result.data.result) == 1
        assert result.data.result[0].metric.__name__ == '42'
        assert result.data.result[0].value == (1640995200.0, '42')

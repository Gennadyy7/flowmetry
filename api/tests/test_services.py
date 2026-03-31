from unittest.mock import AsyncMock, patch

import pytest

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

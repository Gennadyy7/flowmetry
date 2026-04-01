from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
import pytest

from api.main import app
from api.schemas import (
    InstantQueryData,
    InstantQueryResponse,
    LabelNamesResponse,
    LabelValuesResponse,
    QueryRangeData,
    QueryRangeResponse,
    SeriesResponse,
)


class TestRouter:
    @pytest.fixture
    def client(self) -> TestClient:
        return TestClient(app)

    def test_health_endpoint(self, client: TestClient) -> None:
        response = client.get('/health')

        assert response.status_code == 200
        assert response.json() == {'status': 'ok'}

    def test_build_info_endpoint(self, client: TestClient) -> None:
        response = client.get('/api/v1/status/buildinfo')

        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'success'
        assert 'version' in data['data']
        assert 'revision' in data['data']
        assert data['data']['version'] == '0.1.0'

    @patch('api.router.PrometheusService.handle_instant_query')
    def test_instant_query_get_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = InstantQueryResponse(
            status='success', data=InstantQueryData(resultType='vector', result=[])
        )
        mock_handler.return_value = mock_response

        response = client.get('/api/v1/query?query=up&time=1234567890')

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once_with('up', 1234567890.0)

    @patch('api.router.PrometheusService.handle_instant_query')
    def test_instant_query_post_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = InstantQueryResponse(
            status='success', data=InstantQueryData(resultType='vector', result=[])
        )
        mock_handler.return_value = mock_response

        response = client.post(
            '/api/v1/query', data={'query': 'up', 'time': '1234567890'}
        )

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once_with('up', 1234567890.0)

    @patch('api.router.PrometheusService.handle_instant_query')
    def test_instant_query_with_current_time(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = InstantQueryResponse(
            status='success', data=InstantQueryData(resultType='vector', result=[])
        )
        mock_handler.return_value = mock_response

        with patch('api.router.datetime') as mock_datetime:
            mock_datetime.now.return_value.timestamp.return_value = 1234567890.5

            response = client.get('/api/v1/query?query=up')

            assert response.status_code == 200
            mock_handler.assert_called_once_with('up', 1234567890.5)

    def test_instant_query_missing_query(self, client: TestClient) -> None:
        response = client.get('/api/v1/query')

        assert response.status_code == 400
        assert 'Missing query parameter' in response.json()['detail']

    @patch('api.router.PrometheusService.handle_instant_query')
    def test_instant_query_invalid_promql(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        from api.promql_parser import ParseError

        mock_handler.side_effect = ParseError('Invalid query', 'rate(')

        response = client.get('/api/v1/query?query=rate(')

        assert response.status_code == 400
        assert 'Invalid query' in response.json()['detail']

    @patch('api.router.PrometheusService.handle_instant_query')
    def test_instant_query_database_error(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_handler.side_effect = Exception('Database connection failed')

        response = client.get('/api/v1/query?query=up')

        assert response.status_code == 500
        assert 'Database error' in response.json()['detail']

    @patch('api.router.PrometheusService.handle_range_query')
    def test_query_range_get_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = QueryRangeResponse(
            status='success', data=QueryRangeData(resultType='matrix', result=[])
        )
        mock_handler.return_value = mock_response

        response = client.get(
            '/api/v1/query_range?query=up&start=1234567890&end=1234567950&step=60'
        )

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once_with('up', 1234567890.0, 1234567950.0, 60)

    @patch('api.router.PrometheusService.handle_range_query')
    def test_query_range_post_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = QueryRangeResponse(
            status='success', data=QueryRangeData(resultType='matrix', result=[])
        )
        mock_handler.return_value = mock_response

        response = client.post(
            '/api/v1/query_range',
            data={
                'query': 'up',
                'start': '1234567890',
                'end': '1234567950',
                'step': '60',
            },
        )

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once_with('up', 1234567890.0, 1234567950.0, 60)

    def test_query_range_missing_parameters(self, client: TestClient) -> None:
        response = client.get('/api/v1/query_range?query=up')

        assert response.status_code == 400
        assert 'Missing start parameter' in response.json()['detail']

    def test_query_range_missing_end(self, client: TestClient) -> None:
        response = client.get('/api/v1/query_range?query=up&start=1234567890')

        assert response.status_code == 400
        assert 'Missing end parameter' in response.json()['detail']

    def test_query_range_missing_step(self, client: TestClient) -> None:
        response = client.get(
            '/api/v1/query_range?query=up&start=1234567890&end=1234567950'
        )

        assert response.status_code == 400
        assert 'Missing step parameter' in response.json()['detail']

    def test_query_range_invalid_step(self, client: TestClient) -> None:
        response = client.get(
            '/api/v1/query_range?query=up&start=1234567890&end=1234567950&step=0'
        )

        assert response.status_code == 422

    @patch('api.router.PrometheusService.handle_range_query')
    def test_query_range_invalid_promql(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        from api.promql_parser import ParseError

        mock_handler.side_effect = ParseError('Invalid query', 'rate(')

        response = client.get(
            '/api/v1/query_range?query=rate(&start=1234567890&end=1234567950&step=60'
        )

        assert response.status_code == 400
        assert 'Invalid query' in response.json()['detail']

    @patch('api.router.PrometheusService.get_series')
    def test_get_series_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = SeriesResponse(
            status='success', data=[{'__name__': 'up', 'job': 'api'}]
        )
        mock_handler.return_value = mock_response

        response = client.get('/api/v1/series?match[]=up&match[]=http_requests_total')

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once_with(['up', 'http_requests_total'])

    @patch('api.router.PrometheusService.get_series')
    def test_get_series_no_match(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = SeriesResponse(status='success', data=[])
        mock_handler.return_value = mock_response

        response = client.get('/api/v1/series')

        assert response.status_code == 200
        mock_handler.assert_called_once_with([])

    @patch('api.router.PrometheusService.get_series')
    def test_get_series_database_error(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_handler.side_effect = Exception('Database error')

        response = client.get('/api/v1/series?match[]=up')

        assert response.status_code == 500
        assert 'Database error' in response.json()['detail']

    @patch('api.router.PrometheusService.get_label_names')
    def test_get_label_names_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = LabelNamesResponse(
            status='success', data=['job', 'instance', 'service']
        )
        mock_handler.return_value = mock_response

        response = client.get('/api/v1/labels')

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once()

    @patch('api.router.PrometheusService.get_label_names')
    def test_get_label_names_database_error(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_handler.side_effect = Exception('Database error')

        response = client.get('/api/v1/labels')

        assert response.status_code == 500
        assert 'Database error' in response.json()['detail']

    @patch('api.router.PrometheusService.get_label_values')
    def test_get_label_values_success(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_response = LabelValuesResponse(
            status='success', data=['api', 'worker', 'collector']
        )
        mock_handler.return_value = mock_response

        response = client.get('/api/v1/label/job/values')

        assert response.status_code == 200
        assert response.json() == mock_response.model_dump()
        mock_handler.assert_called_once_with('job')

    @patch('api.router.PrometheusService.get_label_values')
    def test_get_label_values_database_error(
        self, mock_handler: AsyncMock, client: TestClient
    ) -> None:
        mock_handler.side_effect = Exception('Database error')

        response = client.get('/api/v1/label/job/values')

        assert response.status_code == 500
        assert 'Database error' in response.json()['detail']

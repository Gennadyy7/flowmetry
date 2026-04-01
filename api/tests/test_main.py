from unittest.mock import AsyncMock, patch

import pytest

from api.main import app


class TestMain:
    def test_app_creation(self) -> None:
        assert app is not None
        assert app.title == 'FastAPI'
        assert len(app.routes) > 0

    @pytest.mark.asyncio
    async def test_lifespan_context(self) -> None:
        from api.db import timescale_db
        from api.main import lifespan

        with patch.object(
            timescale_db, 'connect', new_callable=AsyncMock
        ) as mock_connect:
            with patch.object(
                timescale_db, 'close', new_callable=AsyncMock
            ) as mock_close:
                async with lifespan(app):
                    pass

                mock_connect.assert_called_once()
                mock_close.assert_called_once()

    def test_health_endpoint_direct(self) -> None:
        from fastapi.testclient import TestClient

        client = TestClient(app)
        response = client.get('/health')

        assert response.status_code == 200
        assert response.json() == {'status': 'ok'}

    def test_router_inclusion(self) -> None:
        route_paths = []
        for route in app.routes:
            if hasattr(route, 'path'):
                route_paths.append(route.path)

        assert '/api/v1/query' in route_paths
        assert '/api/v1/query_range' in route_paths
        assert '/api/v1/series' in route_paths
        assert '/api/v1/labels' in route_paths
        assert '/api/v1/status/buildinfo' in route_paths

from unittest.mock import AsyncMock

from aiohttp import web

from aggregator.health_server import HealthServer


class TestHealthServer:
    def test_init(self) -> None:
        server = HealthServer('localhost', 8080)
        assert server.host == 'localhost'
        assert server.port == 8080
        assert server._runner is None

    async def test_start_and_stop(self) -> None:
        server = HealthServer('localhost', 8081)

        await server.start()
        assert server._runner is not None

        await server.stop()
        assert server._runner is None

    async def test_start_idempotent(self) -> None:
        server = HealthServer('localhost', 8082)

        await server.start()
        runner = server._runner

        await server.start()
        assert server._runner is runner

        await server.stop()

    async def test_stop_idempotent(self) -> None:
        server = HealthServer('localhost', 8083)

        await server.stop()
        assert server._runner is None

        await server.start()
        await server.stop()
        await server.stop()
        assert server._runner is None

    async def test_health_handler(self) -> None:
        request = AsyncMock()

        response = await HealthServer._health_handler(request)

        assert isinstance(response, web.Response)
        assert response.status == 200

    async def test_start_and_stop_integration(self) -> None:
        server = HealthServer('127.0.0.1', 8086)

        await server.start()
        assert server._runner is not None

        await server.stop()
        assert server._runner is None

import logging

from aiohttp import web

logger = logging.getLogger(__name__)


class HealthServer:
    """Minimal async HTTP server for health checks."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        if self._runner is not None:
            return
        app = web.Application()
        app.router.add_get('/health', self._health_handler)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info(f'Health server started on {self.host}:{self.port}/health')

    async def stop(self) -> None:
        if self._runner is None:
            return
        await self._runner.cleanup()
        self._runner = None
        logger.info('Health server stopped')

    @staticmethod
    async def _health_handler(_request: web.Request) -> web.Response:
        return web.json_response({'status': 'ok'})

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
import uvicorn

from collector.config import settings
from collector.logging import setup_logging
from collector.redis_stream_client import redis_stream_client
from collector.router import router as metrics_router

setup_logging(
    service_name=settings.SERVICE_NAME,
    level=settings.LOG_LEVEL,
    log_format=settings.LOG_FORMAT,
    version=settings.SERVICE_VERSION,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await redis_stream_client.start()
    try:
        yield
    finally:
        await asyncio.gather(
            # worker_task.cancel(),
            redis_stream_client.stop(),
            return_exceptions=True,
        )


app = FastAPI(lifespan=lifespan)


@app.get('/health')
async def health_check() -> dict[str, str]:
    logger.debug('Health check...')
    return {'status': 'ok'}


app.include_router(metrics_router)


def main() -> None:
    uvicorn.run(
        'collector.main:app',
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_RELOAD,
    )


if __name__ == '__main__':
    main()

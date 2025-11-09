import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
import uvicorn

from api.config import settings
from api.db import timescale_db
from api.log_config_loader import setup_logging
from api.router import router as metrics_router

setup_logging(
    service_name=settings.SERVICE_NAME,
    level=settings.LOG_LEVEL,
    log_format=settings.LOG_FORMAT,
    version=settings.SERVICE_VERSION,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await timescale_db.connect()
    logger.info('API started')
    try:
        yield
    finally:
        logger.info('Shutting down...')
        await asyncio.gather(
            timescale_db.close(),
            return_exceptions=True,
        )
        logger.info('Shutdown complete')


app = FastAPI(lifespan=lifespan)


@app.get('/health')
async def health_check() -> dict[str, str]:
    logger.debug('Health check...')
    return {'status': 'ok'}


app.include_router(metrics_router)


def main() -> None:
    uvicorn.run(
        'api.main:app',
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_RELOAD,
    )


if __name__ == '__main__':
    main()

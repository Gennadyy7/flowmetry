import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from collector.config import settings
from collector.redis_stream_client import RedisStreamClient

redis_stream_client = RedisStreamClient(
    stream_name=settings.REDIS_STREAM_NAME,
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD,
)


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


def main() -> None:
    uvicorn.run(
        'collector.main:app',
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_RELOAD,
    )


if __name__ == '__main__':
    main()

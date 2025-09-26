import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from collector.config import settings
from collector.redis_stream_client import redis_stream_client
from collector.router import router as metrics_router


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

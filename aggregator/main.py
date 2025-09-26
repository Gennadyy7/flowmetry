import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import signal

from redis.asyncio import Redis

from aggregator.config import settings


@asynccontextmanager
async def lifespan() -> AsyncGenerator[None, None]:
    redis_client = Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=True,
    )
    # worker_task = asyncio.create_task(background_worker(redis_client))
    try:
        yield
    finally:
        await asyncio.gather(
            # worker_task.cancel(),
            redis_client.aclose(),
            return_exceptions=True,
        )


async def main() -> None:
    shutdown_event = asyncio.Event()

    for sig in [signal.SIGTERM, signal.SIGINT]:
        asyncio.get_running_loop().add_signal_handler(sig, shutdown_event.set)

    async with lifespan():
        await shutdown_event.wait()


if __name__ == '__main__':
    asyncio.run(main())

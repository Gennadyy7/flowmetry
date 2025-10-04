import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging
import signal

from aggregator.config import settings
from aggregator.db import timescale_db
from aggregator.logging import setup_logging
from aggregator.redis_stream_client import redis_stream_client
from aggregator.worker import AggregationWorker

setup_logging(
    service_name=settings.SERVICE_NAME,
    level=settings.LOG_LEVEL,
    log_format=settings.LOG_FORMAT,
    version=settings.SERVICE_VERSION,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan() -> AsyncGenerator[None, None]:
    await redis_stream_client.start()
    await timescale_db.connect()
    aggregation_worker = AggregationWorker(redis_stream_client, timescale_db)
    worker_task = asyncio.create_task(aggregation_worker.start())
    logger.info('Aggregation worker task started')
    try:
        yield
    finally:
        logger.info('Shutting down...')
        aggregation_worker.stop()
        try:
            await asyncio.wait_for(
                worker_task, timeout=settings.WORKER_SHUTDOWN_TIMEOUT
            )
            logger.info('Aggregation worker stopped gracefully')
        except TimeoutError:
            logger.warning('Worker did not stop in time, cancelling...')
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)
        await asyncio.gather(
            timescale_db.close(),
            redis_stream_client.stop(),
            return_exceptions=True,
        )
        logger.info('Shutdown complete')


async def main() -> None:
    shutdown_event = asyncio.Event()

    for sig in [signal.SIGTERM, signal.SIGINT]:
        asyncio.get_running_loop().add_signal_handler(sig, shutdown_event.set)

    async with lifespan():
        await shutdown_event.wait()


if __name__ == '__main__':
    asyncio.run(main())

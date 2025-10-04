import asyncio
import logging

from aggregator.config import settings
from aggregator.db import TimescaleDB
from aggregator.redis_stream_client import RedisStreamClient

logger = logging.getLogger(__name__)


class AggregationWorker:
    def __init__(self, consumer: RedisStreamClient, db: TimescaleDB) -> None:
        self.consumer = consumer
        self.db = db
        self._running = True

    async def start(self) -> None:
        await self.consumer.ensure_consumer_group()
        logger.info('Aggregation worker started')

        while self._running:
            try:
                processed_new = False

                async for msg_id, point in self.consumer.read_messages(
                    count=settings.REDIS_BATCH_SIZE,
                    block_ms=settings.REDIS_BLOCK_MS,
                ):
                    processed_new = True
                    try:
                        await self.db.insert_metric(point)
                        await self.consumer.ack(msg_id)
                        logger.debug(
                            'Metric processed',
                            extra={'msg_id': msg_id, 'name': point.name},
                        )
                    except Exception as e:
                        logger.error(
                            'Failed to save metric to DB',
                            extra={
                                'msg_id': msg_id,
                                'name': point.name,
                                'error': str(e),
                            },
                        )

                if not processed_new:
                    await self._process_pending_messages()
            except asyncio.CancelledError:
                logger.info('Worker task cancelled')
                break
            except Exception as e:
                logger.exception(
                    'Unexpected error in worker loop', extra={'error': str(e)}
                )
                await asyncio.sleep(1)

    async def _process_pending_messages(self) -> None:
        async for msg_id, point in self.consumer.claim_pending_messages(
            min_idle_time_ms=settings.REDIS_PENDING_IDLE_MS,
            count=settings.REDIS_BATCH_SIZE,
        ):
            try:
                await self.db.insert_metric(point)
                await self.consumer.ack(msg_id)
                logger.debug(
                    'Recovered pending metric',
                    extra={'msg_id': msg_id, 'name': point.name},
                )
            except Exception as e:
                logger.error(
                    'Failed to save recovered metric to DB',
                    extra={'msg_id': msg_id, 'name': point.name, 'error': str(e)},
                )

    def stop(self) -> None:
        self._running = False

from collections.abc import AsyncGenerator
import json
import logging

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from aggregator.config import settings
from aggregator.schemas import MetricPoint

logger = logging.getLogger(__name__)


class MetricsConsumer:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.stream = settings.REDIS_STREAM_NAME
        self.group = settings.REDIS_CONSUMER_GROUP
        self.consumer = settings.REDIS_CONSUMER_NAME

    async def ensure_consumer_group(self) -> None:
        try:
            await self.redis.xgroup_create(
                self.stream, self.group, id='0', mkstream=True
            )
            logger.info('Created consumer group', extra={'group': self.group})
        except ResponseError as e:
            if 'BUSYGROUP' in str(e):
                logger.debug('Consumer group already exists')
            else:
                raise

    @staticmethod
    def _parse_message(raw_data: bytes | str) -> MetricPoint:
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode('utf-8')
        data_dict = json.loads(raw_data)
        return MetricPoint.model_validate(data_dict)

    async def read_messages(
        self,
        count: int,
        block_ms: int,
    ) -> AsyncGenerator[tuple[str, MetricPoint], None]:
        messages = await self.redis.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream: '>'},
            count=count,
            block=block_ms,
        )

        if not messages:
            return

        for _, msg_list in messages:
            for msg_id, fields in msg_list:
                try:
                    data = fields.get(b'data') or fields.get('data')
                    if not data:
                        logger.warning(
                            "Empty 'data' field in message", extra={'msg_id': msg_id}
                        )
                        continue
                    point = self._parse_message(data)
                    yield msg_id, point
                except Exception as e:
                    logger.error(
                        'Failed to parse message',
                        extra={'msg_id': msg_id, 'error': str(e)},
                    )

    async def ack(self, msg_id: str) -> None:
        await self.redis.xack(self.stream, self.group, msg_id)

    async def claim_pending_messages(
        self,
        min_idle_time_ms: int,
        count: int,
    ) -> AsyncGenerator[tuple[str, MetricPoint], None]:
        try:
            pending_entries = await self.redis.xpending_range(
                name=self.stream,
                groupname=self.group,
                min='-',
                max='+',
                count=count,
                idle=min_idle_time_ms,
            )

            if not pending_entries:
                return

            message_ids = [entry['message_id'] for entry in pending_entries]

            claimed_messages = await self.redis.xclaim(
                name=self.stream,
                groupname=self.group,
                consumername=self.consumer,
                min_idle_time=min_idle_time_ms,
                message_ids=message_ids,
            )

            if not claimed_messages:
                return

            for msg_id, fields in claimed_messages:
                try:
                    data = fields.get(b'data') or fields.get('data')
                    if not data:
                        logger.warning(
                            "Empty 'data' in claimed message", extra={'msg_id': msg_id}
                        )
                        await self.ack(msg_id)
                        continue
                    point = self._parse_message(data)
                    yield msg_id, point
                except Exception as e:
                    logger.error(
                        'Failed to parse claimed message',
                        extra={'msg_id': msg_id, 'error': str(e)},
                    )
        except Exception as e:
            logger.exception(
                'Error during pending message recovery', extra={'error': str(e)}
            )

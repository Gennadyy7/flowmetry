from collections.abc import AsyncGenerator
import json
import logging

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from aggregator.config import settings
from aggregator.schemas import MetricPoint

logger = logging.getLogger(__name__)


class RedisStreamClient:
    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        stream_name: str,
        group: str,
        consumer: str,
        password: str | None,
        ssl: bool = False,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self.stream_name = stream_name
        self.group = group
        self.consumer = consumer

        self._redis: Redis | None = None
        self._running = False

    async def start(self) -> None:
        if self._redis is not None:
            logger.debug('Redis stream client already initialized')
            return

        logger.info(
            'Initializing Redis stream client',
            extra={
                'stream_name': self.stream_name,
                'host': self.host,
                'port': self.port,
                'db': self.db,
            },
        )
        try:
            self._redis = Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                ssl=self.ssl,
                decode_responses=False,
            )
            await self._redis.ping()
            self._running = True
        except Exception:
            logger.exception('Failed to start Redis stream client')
            raise

    async def stop(self) -> None:
        if self._redis:
            logger.info('Stopping Redis stream client')
            await self._redis.aclose()
            self._redis = None
        self._running = False
        logger.info('Redis stream client stopped')

    async def ensure_consumer_group(self) -> None:
        if not self._redis:
            raise RuntimeError('Redis stream client not initialized')
        try:
            await self._redis.xgroup_create(
                self.stream_name, self.group, id='0', mkstream=True
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
        if not self._redis or not self._running:
            raise RuntimeError('Redis client not started')
        messages = await self._redis.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream_name: '>'},
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
        if not self._redis or not self._running:
            raise RuntimeError('Redis client not started')
        await self._redis.xack(self.stream_name, self.group, msg_id)

    async def claim_pending_messages(
        self,
        min_idle_time_ms: int,
        count: int,
    ) -> AsyncGenerator[tuple[str, MetricPoint], None]:
        if not self._redis or not self._running:
            raise RuntimeError('Redis client not started')
        try:
            pending_entries = await self._redis.xpending_range(
                name=self.stream_name,
                groupname=self.group,
                min='-',
                max='+',
                count=count,
                idle=min_idle_time_ms,
            )

            if not pending_entries:
                return

            message_ids = [entry['message_id'] for entry in pending_entries]

            claimed_messages = await self._redis.xclaim(
                name=self.stream_name,
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


redis_stream_client = RedisStreamClient(
    stream_name=settings.REDIS_STREAM_NAME,
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD,
    group=settings.REDIS_CONSUMER_GROUP,
    consumer=settings.REDIS_CONSUMER_NAME,
)

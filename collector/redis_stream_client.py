import asyncio
from contextvars import ContextVar
import json
import logging
from typing import Any
import uuid

from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError

from collector.config import settings

logger = logging.getLogger(__name__)


class RedisStreamClient:
    def __init__(
        self,
        stream_name: str,
        host: str,
        port: int,
        db: int,
        password: str | None,
        ssl: bool = False,
        buffer_size: int = 1000,
    ):
        self.stream_name = stream_name
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self.buffer_size = buffer_size

        self._redis: Redis | None = None
        self._buffer: list[bytes] = []
        self._buffer_lock = asyncio.Lock()
        self._running = False

        self._trace_id_context: ContextVar[str | None] = ContextVar(
            'current_trace_id', default=None
        )

    async def start(self) -> None:
        if self._redis is not None:
            logger.debug('Redis client already initialized')
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

    async def _send_to_redis(self, data: bytes) -> None:
        if not self._redis:
            raise RuntimeError('Redis client not initialized')
        await self._redis.xadd(self.stream_name, {'data': data})

    async def send_message(self, message: dict[str, Any]) -> None:
        if not self._running:
            raise RuntimeError('Redis client not started')

        trace_id = self._trace_id_context.get()
        if not trace_id:
            trace_id = str(uuid.uuid4())

        message_with_id = {**message, 'trace_id': trace_id}
        data = json.dumps(message_with_id, ensure_ascii=False).encode('utf-8')

        try:
            """
            This code guarantees FIFO, but this section is extremely redundant for metrics.
            This section should be changed in the future as part of testing and comparison,
            possibly without guaranteeing any order.
            """
            async with self._buffer_lock:
                while self._buffer:
                    buffered_data = self._buffer[0]
                    await self._send_to_redis(buffered_data)
                    self._buffer.pop(0)
                await self._send_to_redis(data)
                logger.debug(
                    'Message sent to Redis stream',
                    extra={'trace_id': trace_id, 'stream': self.stream_name},
                )
        except (ConnectionError, TimeoutError) as e:
            logger.warning(
                'Redis connection error – buffering message',
                extra={
                    'trace_id': trace_id,
                    'error': str(e),
                    'buffer_len': len(self._buffer),
                    'buffer_size_limit': self.buffer_size,
                },
            )
            async with self._buffer_lock:
                if len(self._buffer) < self.buffer_size:
                    self._buffer.append(data)
                    logger.debug(
                        'Message added to buffer',
                        extra={'trace_id': trace_id, 'buffer_len': len(self._buffer)},
                    )
                else:
                    logger.warning(
                        'Buffer overflow – dropping message',
                        extra={'trace_id': trace_id},
                    )


redis_stream_client = RedisStreamClient(
    stream_name=settings.REDIS_STREAM_NAME,
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    password=settings.REDIS_PASSWORD,
)

import asyncio
from contextvars import ContextVar
import json
from typing import Any
import uuid

from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError


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
            return

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

    async def stop(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None
        self._running = False

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
            await self._send_to_redis(data)

            async with self._buffer_lock:
                while self._buffer:
                    buffered_data = self._buffer.pop(0)
                    await self._send_to_redis(buffered_data)

        except (ConnectionError, TimeoutError):
            async with self._buffer_lock:
                if len(self._buffer) < self.buffer_size:
                    self._buffer.append(data)
                else:
                    # logger.warning("Buffer overflow, dropping metric", trace_id=trace_id)
                    pass

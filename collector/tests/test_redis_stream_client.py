import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError, TimeoutError

from collector.redis_stream_client import RedisStreamClient


class TestRedisStreamClient:
    @pytest.fixture
    def client(self) -> RedisStreamClient:
        return RedisStreamClient(
            stream_name='test_stream',
            host='localhost',
            port=6379,
            db=0,
            password=None,
            buffer_size=10,
        )

    @pytest.fixture
    def mock_redis(self) -> AsyncMock:
        return AsyncMock()

    async def test_start_initializes_redis_connection(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        with patch('collector.redis_stream_client.Redis', return_value=mock_redis):
            await client.start()

            mock_redis.ping.assert_called_once()
            assert client._redis is mock_redis
            assert client._running is True

    async def test_start_already_initialized(self, client: RedisStreamClient) -> None:
        client._redis = AsyncMock()
        client._running = True

        with patch('collector.redis_stream_client.Redis') as mock_redis_class:
            await client.start()

            mock_redis_class.assert_not_called()

    async def test_start_connection_error(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        mock_redis.ping.side_effect = Exception('Connection failed')

        with patch('collector.redis_stream_client.Redis', return_value=mock_redis):
            with pytest.raises(Exception, match='Connection failed'):
                await client.start()

    async def test_stop_closes_redis_connection(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        await client.stop()

        mock_redis.aclose.assert_called_once()
        assert client._redis is None
        assert client._running is False

    async def test_stop_without_redis(self, client: RedisStreamClient) -> None:
        await client.stop()

        assert client._redis is None
        assert client._running is False

    async def test_send_to_redis_not_initialized(
        self, client: RedisStreamClient
    ) -> None:
        with pytest.raises(RuntimeError, match='Redis client not initialized'):
            await client._send_to_redis(b'test data')

    async def test_send_to_redis_success(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis

        await client._send_to_redis(b'test data')

        mock_redis.xadd.assert_called_once_with('test_stream', {'data': b'test data'})

    async def test_send_message_not_started(self, client: RedisStreamClient) -> None:
        with pytest.raises(RuntimeError, match='Redis client not started'):
            await client.send_message({'test': 'data'})

    async def test_send_message_success_with_trace_id(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        client._trace_id_context.set('existing-trace-id')

        with patch.object(client, '_send_to_redis') as mock_send:
            await client.send_message({'metric': 'value'})

            expected_data = json.dumps(
                {'metric': 'value', 'trace_id': 'existing-trace-id'}, ensure_ascii=False
            ).encode('utf-8')
            mock_send.assert_called_once_with(expected_data)

    async def test_send_message_success_without_trace_id(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        with patch.object(client, '_send_to_redis') as mock_send:
            await client.send_message({'metric': 'value'})

            mock_send.assert_called_once()
            call_args = mock_send.call_args[0][0]
            sent_data = json.loads(call_args.decode('utf-8'))
            assert sent_data['metric'] == 'value'
            assert 'trace_id' in sent_data

    async def test_send_message_with_buffered_messages(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        client._buffer = [b'buffered1', b'buffered2']

        with patch.object(client, '_send_to_redis') as mock_send:
            await client.send_message({'new': 'message'})

            assert mock_send.call_count == 3
            assert client._buffer == []

    async def test_send_message_connection_error_buffers_message(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xadd.side_effect = ConnectionError('Redis connection failed')

        with patch.object(client, '_send_to_redis') as mock_send:
            mock_send.side_effect = ConnectionError('Redis connection failed')

            await client.send_message({'test': 'data'})

            assert len(client._buffer) == 1
            buffered_data = client._buffer[0]
            buffered_json = json.loads(buffered_data.decode('utf-8'))
            assert buffered_json['test'] == 'data'
            assert 'trace_id' in buffered_json

    async def test_send_message_timeout_error_buffers_message(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        with patch.object(client, '_send_to_redis') as mock_send:
            mock_send.side_effect = TimeoutError('Redis timeout')

            await client.send_message({'test': 'data'})

            assert len(client._buffer) == 1

    async def test_send_message_buffer_overflow_drops_message(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        client.buffer_size = 2
        client._buffer = [b'message1', b'message2']

        with patch.object(client, '_send_to_redis') as mock_send:
            mock_send.side_effect = ConnectionError('Redis connection failed')

            await client.send_message({'new': 'message'})

            assert len(client._buffer) == 2
            assert client._buffer == [b'message1', b'message2']

    async def test_send_message_buffer_not_full_adds_message(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        client.buffer_size = 3
        client._buffer = [b'message1']

        with patch.object(client, '_send_to_redis') as mock_send:
            mock_send.side_effect = ConnectionError('Redis connection failed')

            await client.send_message({'new': 'message'})

            assert len(client._buffer) == 2
            buffered_data = client._buffer[1]
            buffered_json = json.loads(buffered_data.decode('utf-8'))
            assert buffered_json['new'] == 'message'
            assert 'trace_id' in buffered_json

    async def test_send_message_general_exception(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        with patch.object(client, '_send_to_redis') as mock_send:
            mock_send.side_effect = ValueError('Unexpected error')

            with pytest.raises(ValueError, match='Unexpected error'):
                await client.send_message({'test': 'data'})

    async def test_concurrent_send_message_buffer_safety(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xadd.side_effect = ConnectionError('Redis connection failed')

        async def send_message_task(index: int) -> None:
            await client.send_message({'index': index})

        tasks = [send_message_task(i) for i in range(5)]
        await asyncio.gather(*tasks)

        assert len(client._buffer) == 5

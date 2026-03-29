import json
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ResponseError

from aggregator.redis_stream_client import RedisStreamClient
from aggregator.schemas import MetricPoint, MetricType


class TestRedisStreamClient:
    @pytest.fixture
    def client(self) -> RedisStreamClient:
        return RedisStreamClient(
            stream_name='test_stream',
            host='localhost',
            port=6379,
            db=0,
            group='test_group',
            consumer='test_consumer',
            password=None,
        )

    async def test_start_initializes_redis_connection(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        with patch('aggregator.redis_stream_client.Redis', return_value=mock_redis):
            await client.start()

            mock_redis.ping.assert_called_once()
            assert client._redis is mock_redis
            assert client._running is True

    async def test_start_already_initialized(self, client: RedisStreamClient) -> None:
        client._redis = AsyncMock()
        client._running = True

        with patch('aggregator.redis_stream_client.Redis') as mock_redis_class:
            await client.start()
            mock_redis_class.assert_not_called()

    async def test_start_connection_error(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        mock_redis.ping.side_effect = Exception('Connection failed')

        with patch('aggregator.redis_stream_client.Redis', return_value=mock_redis):
            with pytest.raises(Exception, match='Connection failed'):
                await client.start()

        assert client._redis is None

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

    async def test_ensure_consumer_group_creates_group(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis

        await client.ensure_consumer_group()

        mock_redis.xgroup_create.assert_called_once_with(
            'test_stream', 'test_group', id='0', mkstream=True
        )

    async def test_ensure_consumer_group_already_exists(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        mock_redis.xgroup_create.side_effect = ResponseError(
            'BUSYGROUP Consumer Group name already exists'
        )

        await client.ensure_consumer_group()

        mock_redis.xgroup_create.assert_called_once()

    async def test_ensure_consumer_group_other_error(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        mock_redis.xgroup_create.side_effect = ResponseError('Other error')

        with pytest.raises(ResponseError, match='Other error'):
            await client.ensure_consumer_group()

    async def test_ensure_consumer_group_not_initialized(
        self, client: RedisStreamClient
    ) -> None:
        with pytest.raises(RuntimeError, match='Redis stream client not initialized'):
            await client.ensure_consumer_group()

    def test_parse_message_valid_json(self, client: RedisStreamClient) -> None:
        data = {
            'name': 'test_metric',
            'description': 'Test metric',
            'unit': 'count',
            'type': 'counter',
            'timestamp_nano': 1640995200000000000,
            'attributes': {'service': 'test'},
            'value': 42,
        }
        json_data = json.dumps(data)

        result = client._parse_message(json_data)

        assert isinstance(result, MetricPoint)
        assert result.name == 'test_metric'
        assert result.type == MetricType.COUNTER
        assert result.value == 42

    def test_parse_message_bytes_input(self, client: RedisStreamClient) -> None:
        data = {
            'name': 'test_metric',
            'description': 'Test metric',
            'unit': 'count',
            'type': 'counter',
            'timestamp_nano': 1640995200000000000,
            'attributes': {'service': 'test'},
            'value': 42,
        }
        json_data = json.dumps(data).encode('utf-8')

        result = client._parse_message(json_data)

        assert isinstance(result, MetricPoint)
        assert result.name == 'test_metric'

    def test_parse_message_invalid_json(self, client: RedisStreamClient) -> None:
        invalid_json = '{ invalid json }'

        with pytest.raises(json.JSONDecodeError):
            client._parse_message(invalid_json)

    async def test_read_messages_success(
        self,
        client: RedisStreamClient,
        mock_redis: AsyncMock,
        sample_redis_messages: list[tuple[str, dict[bytes, bytes]]],
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xreadgroup.return_value = [('test_stream', sample_redis_messages)]

        messages = []
        async for msg_id, point in client.read_messages(count=10, block_ms=1000):
            messages.append((msg_id, point))

        assert len(messages) == 2
        assert messages[0][0] == '1640995200000-0'
        assert isinstance(messages[0][1], MetricPoint)
        assert messages[0][1].name == 'test_counter'

        mock_redis.xreadgroup.assert_called_once_with(
            groupname='test_group',
            consumername='test_consumer',
            streams={'test_stream': '>'},
            count=10,
            block=1000,
        )

    async def test_read_messages_no_messages(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xreadgroup.return_value = []

        messages = []
        async for msg_id, point in client.read_messages(count=10, block_ms=1000):
            messages.append((msg_id, point))

        assert len(messages) == 0

    async def test_read_messages_not_started(self, client: RedisStreamClient) -> None:
        with pytest.raises(RuntimeError, match='Redis client not started'):
            async for _ in client.read_messages(count=10, block_ms=1000):
                pass

    async def test_read_messages_empty_data_field(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xreadgroup.return_value = [
            ('test_stream', [('1640995200000-0', {b'other_field': b'value'})])
        ]

        messages = []
        async for msg_id, point in client.read_messages(count=10, block_ms=1000):
            messages.append((msg_id, point))

        assert len(messages) == 0

    async def test_read_messages_parse_error(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xreadgroup.return_value = [
            ('test_stream', [('1640995200000-0', {b'data': b'invalid json'})])
        ]

        messages = []
        async for msg_id, point in client.read_messages(count=10, block_ms=1000):
            messages.append((msg_id, point))

        assert len(messages) == 0

    async def test_ack_success(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        await client.ack('1640995200000-0')

        mock_redis.xack.assert_called_once_with(
            'test_stream', 'test_group', '1640995200000-0'
        )

    async def test_ack_not_started(self, client: RedisStreamClient) -> None:
        with pytest.raises(RuntimeError, match='Redis client not started'):
            await client.ack('1640995200000-0')

    async def test_claim_pending_messages_success(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        pending_entries = [
            {
                'message_id': '1640995200000-0',
                'consumer': 'old_consumer',
                'idle': 35000,
                'delivered': 1,
            },
            {
                'message_id': '1640995200001-0',
                'consumer': 'old_consumer',
                'idle': 40000,
                'delivered': 1,
            },
        ]

        claimed_messages = [
            (
                '1640995200000-0',
                {
                    b'data': b'{"name": "test_metric", "type": "counter", "timestamp_nano": 1640995200000000000, "attributes": {}, "value": 42, "description": "", "unit": ""}'
                },
            ),
            (
                '1640995200001-0',
                {
                    b'data': b'{"name": "test_metric2", "type": "gauge", "timestamp_nano": 1640995200000000001, "attributes": {}, "value": 24.5, "description": "", "unit": ""}'
                },
            ),
        ]

        mock_redis.xpending_range.return_value = pending_entries
        mock_redis.xclaim.return_value = claimed_messages

        messages = []
        async for msg_id, point in client.claim_pending_messages(
            min_idle_time_ms=30000, count=10
        ):
            messages.append((msg_id, point))

        assert len(messages) == 2
        assert messages[0][0] == '1640995200000-0'
        assert isinstance(messages[0][1], MetricPoint)

        mock_redis.xpending_range.assert_called_once_with(
            name='test_stream',
            groupname='test_group',
            min='-',
            max='+',
            count=10,
            idle=30000,
        )

        message_ids = ['1640995200000-0', '1640995200001-0']
        mock_redis.xclaim.assert_called_once_with(
            name='test_stream',
            groupname='test_group',
            consumername='test_consumer',
            min_idle_time=30000,
            message_ids=message_ids,
        )

    async def test_claim_pending_messages_no_pending(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xpending_range.return_value = []

        messages = []
        async for msg_id, point in client.claim_pending_messages(
            min_idle_time_ms=30000, count=10
        ):
            messages.append((msg_id, point))

        assert len(messages) == 0
        mock_redis.xclaim.assert_not_called()

    async def test_claim_pending_messages_no_claimed(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xpending_range.return_value = [
            {
                'message_id': '1640995200000-0',
                'consumer': 'old_consumer',
                'idle': 35000,
                'delivered': 1,
            }
        ]
        mock_redis.xclaim.return_value = []

        messages = []
        async for msg_id, point in client.claim_pending_messages(
            min_idle_time_ms=30000, count=10
        ):
            messages.append((msg_id, point))

        assert len(messages) == 0

    async def test_claim_pending_messages_not_started(
        self, client: RedisStreamClient
    ) -> None:
        with pytest.raises(RuntimeError, match='Redis client not started'):
            async for _ in client.claim_pending_messages(
                min_idle_time_ms=30000, count=10
            ):
                pass

    async def test_claim_pending_messages_empty_data(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True

        pending_entries = [
            {
                'message_id': '1640995200000-0',
                'consumer': 'old_consumer',
                'idle': 35000,
                'delivered': 1,
            }
        ]

        claimed_messages = [('1640995200000-0', {b'other_field': b'value'})]

        mock_redis.xpending_range.return_value = pending_entries
        mock_redis.xclaim.return_value = claimed_messages

        messages = []
        async for msg_id, point in client.claim_pending_messages(
            min_idle_time_ms=30000, count=10
        ):
            messages.append((msg_id, point))

        assert len(messages) == 0
        mock_redis.xack.assert_called_once_with(
            'test_stream', 'test_group', '1640995200000-0'
        )

    async def test_claim_pending_messages_exception_handling(
        self, client: RedisStreamClient, mock_redis: AsyncMock
    ) -> None:
        client._redis = mock_redis
        client._running = True
        mock_redis.xpending_range.side_effect = Exception('Redis error')

        messages = []
        async for msg_id, point in client.claim_pending_messages(
            min_idle_time_ms=30000, count=10
        ):
            messages.append((msg_id, point))

        assert len(messages) == 0

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, patch

from aggregator.schemas import MetricPoint, MetricType
from aggregator.tests.async_helpers import AsyncIteratorMock, EmptyAsyncIterator
from aggregator.worker import AggregationWorker


class TestAggregationWorker:
    def test_worker_initialization(self, worker: AggregationWorker) -> None:
        assert worker.consumer is not None
        assert worker.db is not None
        assert worker._running is True

    def test_worker_stop_method(self, worker: AggregationWorker) -> None:
        assert worker._running is True
        worker.stop()
        assert worker._running is False

    def test_worker_stop_multiple_calls(self, worker: AggregationWorker) -> None:
        worker.stop()
        worker.stop()
        assert worker._running is False

    def test_start_method_structure(self, worker: AggregationWorker) -> None:
        assert hasattr(worker, 'start')
        assert callable(worker.start)
        assert worker._running is True

    async def test_process_single_message(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        processed_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        await mock_process_new_messages()

        assert len(processed_messages) == 1
        assert processed_messages[0] == ('msg1', sample_metric_point_counter)
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_called_once_with('msg1')

    async def test_process_multiple_messages(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        metric1 = MetricPoint(
            name='test_counter1',
            description='Test counter 1',
            unit='count',
            type=MetricType.COUNTER,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            value=10,
        )
        metric2 = MetricPoint(
            name='test_counter2',
            description='Test counter 2',
            unit='count',
            type=MetricType.COUNTER,
            timestamp_nano=1640995200000000001,
            attributes={'service': 'test'},
            value=20,
        )

        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [
                ('msg1', metric1),
                ('msg2', metric2),
            ]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        processed_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        await mock_process_new_messages()

        assert len(processed_messages) == 2
        assert processed_messages[0] == ('msg1', metric1)
        assert processed_messages[1] == ('msg2', metric2)
        assert mock_db.insert_metric.call_count == 2
        assert mock_consumer.ack.call_count == 2

    async def test_process_pending_messages(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('pending1', sample_metric_point_counter)]
        )

        processed_messages = []

        async def mock_process_pending_messages() -> None:
            async for msg_id, point in mock_consumer.claim_pending_messages(
                min_idle_time_ms=30000,
                count=10,
            ):
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        await mock_process_pending_messages()

        assert len(processed_messages) == 1
        assert processed_messages[0] == ('pending1', sample_metric_point_counter)
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_called_once_with('pending1')
        mock_consumer.claim_pending_messages.assert_called_once_with(
            min_idle_time_ms=30000,
            count=10,
        )

    async def test_process_pending_messages_with_messages(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )

        await worker._process_pending_messages()

        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_called_once_with('msg1')

    async def test_process_pending_messages_empty(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        async def mock_claim_messages(
            *args: Any, **kwargs: Any
        ) -> AsyncGenerator[Any, None]:
            return
            yield  # This yield will never be executed

        mock_consumer.claim_pending_messages = mock_claim_messages

        await worker._process_pending_messages()

        mock_db.insert_metric.assert_not_called()
        mock_consumer.ack.assert_not_called()

    async def test_error_handling_in_message_processing(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_db.insert_metric.side_effect = Exception('DB Error')
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        processed_messages = []
        errors = []

        async def mock_process_new_messages() -> None:
            try:
                async for msg_id, point in mock_consumer.read_messages():
                    processed_messages.append((msg_id, point))
                    await mock_db.insert_metric(point)
                    await mock_consumer.ack(msg_id)
            except Exception as e:
                errors.append(e)

        await mock_process_new_messages()

        assert len(processed_messages) == 1
        assert len(errors) == 1
        assert 'DB Error' in str(errors[0])
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_not_called()

    async def test_process_pending_messages_handles_exception(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_db.insert_metric.side_effect = Exception('DB Error')
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )

        await worker._process_pending_messages()

        mock_db.insert_metric.assert_called_once()
        mock_consumer.ack.assert_not_called()

    def test_process_pending_messages_logging(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_db.insert_metric.side_effect = Exception('DB Error')
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )

        with patch('aggregator.worker.logger') as mock_logger:
            assert mock_logger is not None

    async def test_worker_handles_empty_streams(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        mock_consumer.read_messages.return_value = EmptyAsyncIterator()
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        processed_new_messages = []
        processed_pending_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_new_messages.append((msg_id, point))

        async def mock_process_pending_messages() -> None:
            async for msg_id, point in mock_consumer.claim_pending_messages(
                min_idle_time_ms=30000,
                count=10,
            ):
                processed_pending_messages.append((msg_id, point))

        await mock_process_new_messages()
        await mock_process_pending_messages()

        assert len(processed_new_messages) == 0
        assert len(processed_pending_messages) == 0
        mock_db.insert_metric.assert_not_called()
        mock_consumer.ack.assert_not_called()

    async def test_worker_handles_database_errors(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        mock_db.insert_metric.side_effect = Exception('Database connection failed')
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        processed_messages = []
        errors = []

        async def mock_process_new_messages() -> None:
            try:
                async for msg_id, point in mock_consumer.read_messages():
                    processed_messages.append((msg_id, point))
                    await mock_db.insert_metric(point)
                    await mock_consumer.ack(msg_id)
            except Exception as e:
                errors.append(e)

        await mock_process_new_messages()

        assert len(processed_messages) == 1
        assert len(errors) == 1
        assert 'Database connection failed' in str(errors[0])
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_not_called()

    def test_worker_attributes_types(self, worker: AggregationWorker) -> None:
        assert isinstance(worker.consumer, AsyncMock)
        assert isinstance(worker.db, AsyncMock)
        assert isinstance(worker._running, bool)

    def test_worker_state_independence(self, worker: AggregationWorker) -> None:
        original_running = worker._running
        worker.stop()
        assert worker._running != original_running
        assert worker._running is False

    def test_worker_with_different_consumer_and_db(
        self, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        worker = AggregationWorker(mock_consumer, mock_db)
        assert worker.consumer is mock_consumer
        assert worker.db is mock_db

    async def test_worker_with_different_metric_types(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        counter_metric = MetricPoint(
            name='test_counter',
            description='Test counter',
            unit='count',
            type=MetricType.COUNTER,
            timestamp_nano=1640995200000000000,
            attributes={'service': 'test'},
            value=42,
        )
        gauge_metric = MetricPoint(
            name='test_gauge',
            description='Test gauge',
            unit='bytes',
            type=MetricType.GAUGE,
            timestamp_nano=1640995200000000001,
            attributes={'service': 'test'},
            value=1024,
        )
        histogram_metric = MetricPoint(
            name='test_histogram',
            description='Test histogram',
            unit='seconds',
            type=MetricType.HISTOGRAM,
            timestamp_nano=1640995200000000002,
            attributes={'service': 'test'},
            sum=125.5,
            count=10,
            bucket_counts=[1, 3, 6, 8, 10],
            explicit_bounds=[0.1, 0.5, 1.0, 2.0, 5.0],
        )

        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [
                ('msg1', counter_metric),
                ('msg2', gauge_metric),
                ('msg3', histogram_metric),
            ]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        processed_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        await mock_process_new_messages()

        assert len(processed_messages) == 3
        assert processed_messages[0] == ('msg1', counter_metric)
        assert processed_messages[1] == ('msg2', gauge_metric)
        assert processed_messages[2] == ('msg3', histogram_metric)
        assert mock_db.insert_metric.call_count == 3
        assert mock_consumer.ack.call_count == 3

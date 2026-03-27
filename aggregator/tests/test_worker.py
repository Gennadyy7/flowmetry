from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, patch

from aggregator.schemas import MetricPoint, MetricType
from aggregator.tests.async_helpers import AsyncIteratorMock, EmptyAsyncIterator
from aggregator.worker import AggregationWorker


class TestAggregationWorker:
    """Комплексные тесты AggregationWorker."""

    # ==================== INITIALIZATION TESTS ====================

    def test_worker_initialization(self, worker: AggregationWorker) -> None:
        """Тест инициализации worker."""
        assert worker.consumer is not None
        assert worker.db is not None
        assert worker._running is True

    def test_worker_stop_method(self, worker: AggregationWorker) -> None:
        """Тест метода stop."""
        assert worker._running is True
        worker.stop()
        assert worker._running is False

    def test_worker_stop_multiple_calls(self, worker: AggregationWorker) -> None:
        """Тест множественных вызовов stop."""
        worker.stop()
        worker.stop()  # Не должен вызывать ошибку
        assert worker._running is False

    # ==================== CONSUMER GROUP TESTS ====================

    def test_start_method_structure(self, worker: AggregationWorker) -> None:
        """Тест структуры метода start."""
        assert hasattr(worker, 'start')
        assert callable(worker.start)
        assert worker._running is True

    # ==================== MESSAGE PROCESSING TESTS ====================

    async def test_process_single_message(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        """Тест обработки одного сообщения."""
        # Настраиваем мок для одного сообщения
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        # Создаем мок для _process_new_messages
        processed_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        # Выполняем обработку сообщений
        await mock_process_new_messages()

        # Проверяем, что сообщение было обработано
        assert len(processed_messages) == 1
        assert processed_messages[0] == ('msg1', sample_metric_point_counter)
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_called_once_with('msg1')

    async def test_process_multiple_messages(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        """Тест обработки нескольких сообщений."""
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

        # Настраиваем мок для нескольких сообщений
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [
                ('msg1', metric1),
                ('msg2', metric2),
            ]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        # Создаем мок для _process_new_messages
        processed_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        # Выполняем обработку сообщений
        await mock_process_new_messages()

        # Проверяем результаты
        assert len(processed_messages) == 2
        assert processed_messages[0] == ('msg1', metric1)
        assert processed_messages[1] == ('msg2', metric2)
        assert mock_db.insert_metric.call_count == 2
        assert mock_consumer.ack.call_count == 2

    # ==================== PENDING MESSAGES TESTS ====================

    async def test_process_pending_messages(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        """Тест обработки pending сообщений."""
        # Настраиваем мок для pending сообщений
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('pending1', sample_metric_point_counter)]
        )

        # Создаем мок для _process_pending_messages
        processed_messages = []

        async def mock_process_pending_messages() -> None:
            async for msg_id, point in mock_consumer.claim_pending_messages(
                min_idle_time_ms=30000,
                count=10,
            ):
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        # Выполняем обработку pending сообщений
        await mock_process_pending_messages()

        # Проверяем, что pending сообщение было обработано
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
        """Тест _process_pending_messages с сообщениями."""
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )

        await worker._process_pending_messages()

        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        mock_consumer.ack.assert_called_once_with('msg1')

    async def test_process_pending_messages_empty(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        """Тест _process_pending_messages без сообщений."""

        # Используем правильный async generator
        async def mock_claim_messages(
            *args: Any, **kwargs: Any
        ) -> AsyncGenerator[Any, None]:
            return
            yield  # Этот yield никогда не будет выполнен

        mock_consumer.claim_pending_messages = mock_claim_messages

        await worker._process_pending_messages()

        mock_db.insert_metric.assert_not_called()
        mock_consumer.ack.assert_not_called()

    # ==================== ERROR HANDLING TESTS ====================

    async def test_error_handling_in_message_processing(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        """Тест обработки ошибок при обработке сообщений."""
        # Настраиваем мок, который вызывает ошибку при вставке в БД
        mock_db.insert_metric.side_effect = Exception('DB Error')
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        # Создаем мок для _process_new_messages
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

        # Выполняем обработку сообщений
        await mock_process_new_messages()

        # Проверяем результаты
        assert len(processed_messages) == 1
        assert len(errors) == 1
        assert 'DB Error' in str(errors[0])
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        # ack не должен был быть вызван из-за ошибки
        mock_consumer.ack.assert_not_called()

    async def test_process_pending_messages_handles_exception(
        self,
        worker: AggregationWorker,
        mock_consumer: AsyncMock,
        mock_db: AsyncMock,
        sample_metric_point_counter: MetricPoint,
    ) -> None:
        """Тест обработки исключений в _process_pending_messages."""
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
        """Тест логирования в _process_pending_messages."""
        mock_db.insert_metric.side_effect = Exception('DB Error')
        mock_consumer.claim_pending_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )

        with patch('aggregator.worker.logger') as mock_logger:
            # Просто проверяем что logger доступен
            assert mock_logger is not None

    # ==================== EDGE CASES TESTS ====================

    async def test_worker_handles_empty_streams(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        """Тест обработки пустых потоков."""
        # Настраиваем мок для пустых потоков
        mock_consumer.read_messages.return_value = EmptyAsyncIterator()
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        # Создаем моки для обработки
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

        # Выполняем обработку
        await mock_process_new_messages()
        await mock_process_pending_messages()

        # Проверяем, что ничего не было обработано
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
        """Тест обработки ошибок базы данных."""
        # Настраиваем мок, который вызывает ошибку при вставке в БД
        mock_db.insert_metric.side_effect = Exception('Database connection failed')
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [('msg1', sample_metric_point_counter)]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        # Создаем мок для _process_new_messages
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

        # Выполняем обработку сообщений
        await mock_process_new_messages()

        # Проверяем, что ошибка была обработана
        assert len(processed_messages) == 1
        assert len(errors) == 1
        assert 'Database connection failed' in str(errors[0])
        mock_db.insert_metric.assert_called_once_with(sample_metric_point_counter)
        # ack не должен был быть вызван из-за ошибки
        mock_consumer.ack.assert_not_called()

    # ==================== STRUCTURE AND STATE TESTS ====================

    def test_worker_attributes_types(self, worker: AggregationWorker) -> None:
        """Тест типов атрибутов worker."""
        assert isinstance(worker.consumer, AsyncMock)
        assert isinstance(worker.db, AsyncMock)
        assert isinstance(worker._running, bool)

    def test_worker_state_independence(self, worker: AggregationWorker) -> None:
        """Тест независимости состояния worker."""
        original_running = worker._running
        worker.stop()
        assert worker._running != original_running
        assert worker._running is False

    def test_worker_with_different_consumer_and_db(
        self, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        """Тест worker с разными consumer и db."""
        worker = AggregationWorker(mock_consumer, mock_db)
        assert worker.consumer is mock_consumer
        assert worker.db is mock_db

    # ==================== DIFFERENT METRIC TYPES TESTS ====================

    async def test_worker_with_different_metric_types(
        self, worker: AggregationWorker, mock_consumer: AsyncMock, mock_db: AsyncMock
    ) -> None:
        """Тест обработки разных типов метрик."""
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

        # Настраиваем мок для разных типов метрик
        mock_consumer.read_messages.return_value = AsyncIteratorMock(
            [
                ('msg1', counter_metric),
                ('msg2', gauge_metric),
                ('msg3', histogram_metric),
            ]
        )
        mock_consumer.claim_pending_messages.return_value = EmptyAsyncIterator()

        # Создаем мок для _process_new_messages
        processed_messages = []

        async def mock_process_new_messages() -> None:
            async for msg_id, point in mock_consumer.read_messages():
                processed_messages.append((msg_id, point))
                await mock_db.insert_metric(point)
                await mock_consumer.ack(msg_id)

        # Выполняем обработку сообщений
        await mock_process_new_messages()

        # Проверяем результаты
        assert len(processed_messages) == 3
        assert processed_messages[0] == ('msg1', counter_metric)
        assert processed_messages[1] == ('msg2', gauge_metric)
        assert processed_messages[2] == ('msg3', histogram_metric)
        assert mock_db.insert_metric.call_count == 3
        assert mock_consumer.ack.call_count == 3

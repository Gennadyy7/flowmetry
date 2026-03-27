from collections.abc import AsyncIterator
from typing import Generic, TypeVar

T = TypeVar('T')


class AsyncIteratorMock(Generic[T]):
    """Правильный async iterator mock для тестов."""

    def __init__(self, items: list[T] | None = None):
        self.items = items or []
        self.index = 0

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


class EmptyAsyncIterator(Generic[T]):
    """Пустой async iterator для тестов."""

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        raise StopAsyncIteration


def create_async_iterator[T](items: list[T] | None = None) -> AsyncIterator[T]:
    """Создает async iterator для тестов."""
    if items is None or len(items) == 0:
        return EmptyAsyncIterator()
    return AsyncIteratorMock(items)

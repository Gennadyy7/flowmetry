---
trigger: glob
description: 
globs: **/*.py
---

# Python Standards для Flowmetry

## Общие требования
- Python 3.12+
- Строгая типизация: type hints для всех функций и методов
- Следование PEP 8
- Single quotes для строк

## Асинхронный код
- Все I/O операции должны быть асинхронными
- Использовать async/await паттерн
- Для HTTP клиентов: aiohttp
- Для Redis: redis.asyncio
- Для PostgreSQL: asyncpg

## Структура кода
- Принципы ООП и DRY
- Каждый микросервис в отдельном пакете (collector, aggregator, api)
- Конфигурация через Pydantic Settings
- Логирование через logging с использованием log_config_loader.py

## Обработка ошибок
- Использовать конкретные исключения
- Логировать ошибки с контекстом (extra parameters)
- Не использовать bare except

## База данных
- No ORM, только Raw SQL
- Использовать connection pooling (asyncpg.Pool)
- Все SQL запросы должны быть параметризированы
- Использовать контекстные менеджеры для соединений

## Тестирование
- pytest с asyncio_mode = "auto"
- Покрытие кода обязательно
- Тесты в папке tests/ каждого микросервиса
- Использовать pytest-asyncio для асинхронных тестов
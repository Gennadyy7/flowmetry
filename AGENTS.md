# Flowmetry Project Context

## Описание проекта
Система сбора, агрегации и подготовки метрик производительности веб-приложений.

## Микросервисы

### Collector
- Назначение: Приём метрик по OTLP протоколу
- Технологии: FastAPI, Redis Streams
- Порт: 8000 (внутри контейнера), 8001 (внешний)
- Вход: OTLP/HTTP+Protobuf
- Выход: Redis Streams

### Aggregator
- Назначение: Чтение из Redis Streams, запись в TimescaleDB
- Технологии: asyncio worker, aiohttp (health), asyncpg
- Health порт: 8080
- Вход: Redis Streams
- Выход: TimescaleDB

### API
- Назначение: Prometheus-compatible HTTP API для Grafana
- Технологии: FastAPI, asyncpg
- Порт: 8000
- Вход: PromQL запросы от Grafana
- Выход: JSON ответы в формате Prometheus API

## Инфраструктура
- Redis Streams: буфер между collector и aggregator
- TimescaleDB: хранение временных рядов метрик
- Grafana: визуализация метрик
- pgAdmin: администрирование БД

## Развёртывание
- Poetry с in-project окружением
- Docker + docker-compose
- Pre-commit hooks (ruff, mypy)
- CI с автоматическими проверками

## Инструменты
- ruff: линтинг и форматирование
- mypy: строгая проверка типов (strict mode)
- pytest: тестирование с покрытием
- pre-commit: автоматические проверки перед коммитом

## Важные файлы
- pyproject.toml: зависимости и конфигурация инструментов
- .env.example: шаблон переменных окружения
- entrypoint.sh: точка входа каждого микросервиса
- timescaledb/init/*.sql: инициализация схемы БД

## Требования к разработке
- Код должен проходить все CI проверки
- Покрытие тестами обязательно
- Type hints обязательны
- Минимальные необходимые изменения
- Сохранять существующую архитектуру и стиль
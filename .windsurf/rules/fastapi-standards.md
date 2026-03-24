---
trigger: glob
description: 
globs: **/api/**/*.py, **/collector/**/*.py
---

# FastAPI Standards для Flowmetry

## Структура приложения
- Использовать lifespan контекстный менеджер для инициализации/завершения
- Health check endpoint: GET /health возвращает {'status': 'ok'}
- Роутеры выносить в отдельные файлы с префиксом

## Request/Response
- Использовать Pydantic модели для валидации
- Аннотировать типы всех параметров
- Использовать Annotated для зависимостей

## Логирование
- Логировать входящие запросы (debug level)
- Логировать ошибки с полным контекстом
- Использовать extra parameters для структурированного логирования

## Endpoints
- RESTful именование путей
- Prometheus-compatible API для микросервиса api
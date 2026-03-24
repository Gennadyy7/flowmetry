---
trigger: glob
description: 
globs: **/Dockerfile*, **/docker-compose.yml
---

# Docker Standards для Flowmetry

## Dockerfile
- Один Dockerfile на микросервис (Dockerfile.collector, Dockerfile.aggregator, Dockerfile.api)
- Использовать multi-stage build если возможно
- Указывать конкретные версии базовых образов

## Docker Compose
- Все сервисы в одном docker-compose.yml
- Health checks для всех сервисов
- depends_on с condition: service_healthy
- Volumes для персистентных данных
- env_file для конфигурации каждого сервиса

## Сервисы
- collector: порт 8001 (внешний), 8000 (внутренний)
- aggregator: health check на порту 8080
- api: порт 8000
- redis: redis:7.2-alpine
- timescaledb: timescale/timescaledb:latest-pg17
- pgadmin: dpage/pgadmin4:latest
- grafana: grafana/grafana:latest
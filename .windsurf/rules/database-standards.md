---
trigger: glob
description: 
globs: **/db.py, **/schemas.py
---

# Database Standards для Flowmetry

## TimescaleDB
- Использовать hypertables для временных рядов
- Таблицы:
  - metrics_info — метаданные метрик
  - metrics_values — значения метрик (counter, gauge)
  - metrics_histograms — гистограммные метрики

## SQL паттерны
- Использовать ON CONFLICT DO NOTHING для upsert операций
- Использовать DISTINCT ON для получения последних значений
- Использовать time_bucket() для агрегации по временным интервалам
- JSONB для хранения атрибутов метрик

## Connection Pool
- min_size и max_size настраиваются через .env
- Всегда закрывать pool при shutdown

## Миграции
- SQL файлы инициализации в timescaledb/init/
- Порядок выполнения: 00_extensions.sql → 01_schema.sql → 02_hypertables.sql
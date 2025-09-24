# Graduation project "Flowmetry"

## System architecture

```
[Application + OpenTelemetry SDK] 
  →(pushing)→ [Flowmetry Metrics Collector + Redis] 
  →(Redis Streams)→ [Flowmetry Aggregation Service + TimescaleDB] 
  →(SQL queries to TimescaleDB)→ [Flowmetry REST API / Prometheus Exporter service] 
  ←(scraping)← [Grafana]
```
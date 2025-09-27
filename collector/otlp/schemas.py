from typing import Any

from pydantic import BaseModel


class AnyValue(BaseModel):
    string_value: str | None = None
    bool_value: bool | None = None
    int_value: str | None = None
    double_value: float | None = None


class KeyValue(BaseModel):
    key: str
    value: AnyValue


class NumberDataPoint(BaseModel):
    attributes: list[KeyValue] = []
    start_time_unix_nano: str | None = None
    time_unix_nano: str
    as_int: str | None = None
    as_double: float | None = None


class HistogramDataPoint(BaseModel):
    attributes: list[KeyValue] = []
    start_time_unix_nano: str | None = None
    time_unix_nano: str
    count: str
    sum: float | None = None
    bucket_counts: list[str]
    explicit_bounds: list[float] = []


class Sum(BaseModel):
    data_points: list[NumberDataPoint]
    aggregation_temporality: str
    is_monotonic: bool


class Gauge(BaseModel):
    data_points: list[NumberDataPoint]


class Histogram(BaseModel):
    data_points: list[HistogramDataPoint]


class Metric(BaseModel):
    name: str
    description: str = ''
    unit: str = ''
    sum: Sum | None = None
    gauge: Gauge | None = None
    histogram: Histogram | None = None


class ScopeMetrics(BaseModel):
    scope: dict[str, Any] | None = None
    metrics: list[Metric]


class Resource(BaseModel):
    attributes: list[KeyValue] = []


class ResourceMetrics(BaseModel):
    resource: Resource
    scope_metrics: list[ScopeMetrics]


class OTLPMetricsRequest(BaseModel):
    resource_metrics: list[ResourceMetrics]

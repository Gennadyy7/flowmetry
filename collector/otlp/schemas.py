from typing import Any

from pydantic import BaseModel


class AnyValue(BaseModel):
    stringValue: str | None = None
    boolValue: bool | None = None
    intValue: str | None = None
    doubleValue: float | None = None


class KeyValue(BaseModel):
    key: str
    value: AnyValue


class NumberDataPoint(BaseModel):
    attributes: list[KeyValue] = []
    startTimeUnixNano: str
    timeUnixNano: str
    asInt: str | None = None
    asDouble: float | None = None


class HistogramDataPoint(BaseModel):
    attributes: list[KeyValue] = []
    startTimeUnixNano: str
    timeUnixNano: str
    count: str
    sum: float | None = None
    bucketCounts: list[str]
    explicitBounds: list[float] = []


class Sum(BaseModel):
    dataPoints: list[NumberDataPoint]
    aggregationTemporality: str
    isMonotonic: bool


class Gauge(BaseModel):
    dataPoints: list[NumberDataPoint]


class Histogram(BaseModel):
    dataPoints: list[HistogramDataPoint]


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
    scopeMetrics: list[ScopeMetrics]


class OTLPMetricsRequest(BaseModel):
    resourceMetrics: list[ResourceMetrics]

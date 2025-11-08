from enum import Enum

from pydantic import BaseModel


class MetricType(str, Enum):
    COUNTER = 'counter'
    GAUGE = 'gauge'
    HISTOGRAM = 'histogram'


class MetricPoint(BaseModel):
    name: str
    description: str
    unit: str
    type: MetricType

    timestamp_nano: int

    attributes: dict[str, str]

    value: int | float | None = None

    sum: float | None = None
    count: int | None = None
    bucket_counts: list[int] | None = None
    explicit_bounds: list[float] | None = None


class MetricBatch(BaseModel):
    resource_attributes: dict[str, str]
    points: list[MetricPoint]

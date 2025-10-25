from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class MetricType(str, Enum):
    COUNTER = 'counter'
    GAUGE = 'gauge'
    HISTOGRAM = 'histogram'


class DBMetric(BaseModel):
    name: str
    description: str
    unit: str
    type: MetricType
    attributes: dict[str, Any]
    time: datetime
    value: float | None = None
    sum: float | None = None
    count: int | None = None
    bucket_counts: list[int] | None = None
    explicit_bounds: list[float] | None = None

from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar('T')
Sample = tuple[float, str]


class BasePrometheusResponse(BaseModel, Generic[T]):
    status: str = 'success'
    data: T

    model_config = ConfigDict(extra='forbid')


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


class MetricLabels(BaseModel):
    __name__: str
    model_config = ConfigDict(extra='allow')


class ResultItem(BaseModel):
    metric: MetricLabels
    values: list[Sample]


class QueryRangeData(BaseModel):
    resultType: str = 'matrix'
    result: list[ResultItem]


class QueryRangeResponse(BasePrometheusResponse[QueryRangeData]):
    pass


class InstantResultItem(BaseModel):
    metric: MetricLabels
    value: Sample


class InstantQueryData(BaseModel):
    resultType: str = 'vector'
    result: list[InstantResultItem]


class InstantQueryResponse(BasePrometheusResponse[InstantQueryData]):
    pass


class LabelNamesResponse(BasePrometheusResponse[list[str]]):
    pass


class LabelValuesResponse(BasePrometheusResponse[list[str]]):
    pass


class BuildInfoData(BaseModel):
    version: str = '0.1.0'
    revision: str = 'custom'
    branch: str = 'master'
    buildUser: str = 'flowmetry'
    buildDate: str = Field(
        default_factory=lambda: datetime.now().strftime('%Y%m%d-%H:%M:%SZ')
    )

    model_config = ConfigDict(extra='forbid')


class BuildInfoResponse(BasePrometheusResponse[BuildInfoData]):
    pass

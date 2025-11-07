from datetime import datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar('T')

Sample = tuple[float, str]


class MetricLabels(BaseModel):
    __name__: str
    model_config = ConfigDict(extra='allow')

    def to_dict(self) -> dict[str, str]:
        return {k: str(v) for k, v in self.__dict__.items() if not k.startswith('_')}


class InstantResultItem(BaseModel):
    metric: MetricLabels
    value: Sample


class ResultItem(BaseModel):
    metric: MetricLabels
    values: list[Sample]


Vector = list[InstantResultItem]
Matrix = list[ResultItem]


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


class InstantQueryData(BaseModel):
    resultType: str = Field('vector', pattern='^vector$')
    result: Vector


class QueryRangeData(BaseModel):
    resultType: str = Field('matrix', pattern='^matrix$')
    result: Matrix


class LabelNamesResponse(BasePrometheusResponse[list[str]]):
    pass


class LabelValuesResponse(BasePrometheusResponse[list[str]]):
    pass


class InstantQueryResponse(BasePrometheusResponse[InstantQueryData]):
    pass


class QueryRangeResponse(BasePrometheusResponse[QueryRangeData]):
    pass


class SeriesItem(BaseModel):
    model_config = ConfigDict(extra='allow')

    def to_dict(self) -> dict[str, str]:
        return {k: str(v) for k, v in self.__dict__.items() if not k.startswith('_')}


class SeriesResponse(BaseModel):
    status: str = 'success'
    data: list[dict[str, str]]


class BuildInfoData(BaseModel):
    version: str = '0.1.0'
    revision: str = 'custom'
    branch: str = 'master'
    buildUser: str = 'flowmetry'
    buildDate: str = Field(
        default_factory=lambda: datetime.now().strftime('%Y%m%d-%H:%M:%SZ')
    )
    goVersion: str = 'go1.21'
    platform: str = 'linux/amd64'

    model_config = ConfigDict(extra='forbid')


class BuildInfoResponse(BasePrometheusResponse[BuildInfoData]):
    pass

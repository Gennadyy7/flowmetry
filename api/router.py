from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.db import timescale_db
from api.promql_parser import parser
from api.schemas import (
    MetricLabels,
    QueryRangeData,
    QueryRangeResponse,
    ResultItem,
    Sample,
)

router = APIRouter()


@router.get('/api/v1/query_range', response_model=QueryRangeResponse)
async def query_range(
    query: str = Query(..., example='http_requests_total{job="api"}'),
    start: float = Query(...),
    end: float = Query(...),
    _step: int = Query(15, ge=1),
) -> QueryRangeResponse:
    try:
        metric_name, labels = parser.parse(query)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f'Invalid query: {e}') from e
    try:
        series = await timescale_db.fetch_metric_timeseries(
            metric_name=metric_name,
            labels=labels,
            start_ts=start,
            end_ts=end,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Database error: {e}') from e
    if not series:
        return QueryRangeResponse(data=QueryRangeData(resultType='matrix', result=[]))
    grouped: dict[tuple[tuple[str, Any], ...], list[tuple[Any, float]]] = defaultdict(
        list
    )
    for _name, attrs, value, ts in series:
        label_key = tuple(sorted(attrs.items()))
        grouped[label_key].append((ts, value))
    result_items: list[ResultItem] = []
    for label_key, points in grouped.items():
        labels_dict = dict(label_key)
        labels_dict['__name__'] = metric_name
        samples = [
            Sample(timestamp=ts.replace(tzinfo=None).timestamp(), value=str(value))
            for ts, value in points
        ]
        result_items.append(
            ResultItem(metric=MetricLabels(**labels_dict), values=samples)
        )
    return QueryRangeResponse(
        data=QueryRangeData(resultType='matrix', result=result_items)
    )

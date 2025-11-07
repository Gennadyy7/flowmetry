from datetime import datetime
import logging
from typing import Annotated

from fastapi import APIRouter, Form, HTTPException, Path, Query

from api.schemas import (
    BuildInfoResponse,
    InstantQueryResponse,
    LabelNamesResponse,
    LabelValuesResponse,
    QueryRangeResponse,
    SeriesResponse,
)
from api.services.prometheus import PrometheusService

logger = logging.getLogger(__name__)
router = APIRouter(prefix='/api/v1')


@router.get('/query', response_model=InstantQueryResponse)
@router.post('/query', response_model=InstantQueryResponse)
async def instant_query(
    query: Annotated[str | None, Form(..., example='up')] = None,
    time: Annotated[
        float | None, Form(default_factory=lambda: datetime.now().timestamp())
    ] = None,
    query_get: Annotated[str | None, Query(alias='query', example='up')] = None,
    time_get: Annotated[float | None, Query(alias='time')] = None,
) -> InstantQueryResponse:
    resolved_query = query_get if query_get is not None else query
    resolved_time = (
        time_get if time_get is not None else (time or datetime.now().timestamp())
    )

    if not resolved_query:
        raise HTTPException(status_code=400, detail='Missing query parameter')

    try:
        return await PrometheusService.handle_instant_query(
            resolved_query, resolved_time
        )
    except ValueError as e:
        logger.warning(
            'Invalid PromQL in /query', extra={'query': resolved_query, 'error': str(e)}
        )
        raise HTTPException(status_code=400, detail=f'Invalid query: {e}') from e
    except Exception as e:
        logger.exception('Database error in /query', extra={'query': resolved_query})
        raise HTTPException(status_code=500, detail='Database error') from e


@router.get('/query_range', response_model=QueryRangeResponse)
@router.post('/query_range', response_model=QueryRangeResponse)
async def query_range(
    query: Annotated[
        str | None, Form(..., example='http_requests_total{job="api"}')
    ] = None,
    start: Annotated[float | None, Form(...)] = None,
    end: Annotated[float | None, Form(...)] = None,
    step: Annotated[int | None, Form(..., ge=1, description='Step in seconds')] = None,
    query_get: Annotated[str | None, Query(alias='query')] = None,
    start_get: Annotated[float | None, Query(alias='start')] = None,
    end_get: Annotated[float | None, Query(alias='end')] = None,
    step_get: Annotated[int | None, Query(alias='step', ge=1)] = None,
) -> QueryRangeResponse:
    resolved_query = query_get if query_get is not None else query
    resolved_start = start_get if start_get is not None else start
    resolved_end = end_get if end_get is not None else end
    resolved_step = step_get if step_get is not None else step

    if resolved_query is None:
        raise HTTPException(status_code=400, detail='Missing query parameter')
    if resolved_start is None:
        raise HTTPException(status_code=400, detail='Missing start parameter')
    if resolved_end is None:
        raise HTTPException(status_code=400, detail='Missing end parameter')
    if resolved_step is None:
        raise HTTPException(status_code=400, detail='Missing step parameter')

    try:
        return await PrometheusService.handle_range_query(
            resolved_query, resolved_start, resolved_end, resolved_step
        )
    except ValueError as e:
        logger.warning(
            'Invalid PromQL in /query_range',
            extra={'query': resolved_query, 'error': str(e)},
        )
        raise HTTPException(status_code=400, detail=f'Invalid query: {e}') from e
    except Exception as e:
        logger.exception(
            'Database error in /query_range', extra={'query': resolved_query}
        )
        raise HTTPException(status_code=500, detail='Database error') from e


@router.get('/series', response_model=SeriesResponse)
async def get_series(
    match: Annotated[list[str], Query(alias='match[]', default_factory=list)],
) -> SeriesResponse:
    try:
        return await PrometheusService.get_series(match)
    except Exception as e:
        logger.exception('Error in /series', extra={'match': match})
        raise HTTPException(status_code=500, detail='Database error') from e


@router.get('/labels', response_model=LabelNamesResponse)
async def get_label_names() -> LabelNamesResponse:
    try:
        return await PrometheusService.get_label_names()
    except Exception as e:
        logger.exception('Database error in /labels')
        raise HTTPException(status_code=500, detail='Database error') from e


@router.get('/label/{label_name}/values', response_model=LabelValuesResponse)
async def get_label_values(
    label_name: Annotated[str, Path(..., example='job')],
) -> LabelValuesResponse:
    try:
        return await PrometheusService.get_label_values(label_name)
    except Exception as e:
        logger.exception(
            'Database error in /label/.../values', extra={'label_name': label_name}
        )
        raise HTTPException(status_code=500, detail='Database error') from e


@router.get('/status/buildinfo', response_model=BuildInfoResponse)
async def get_build_info() -> BuildInfoResponse:
    logger.debug('Build info requested')
    return PrometheusService.get_build_info()

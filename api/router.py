from datetime import datetime
import logging
from typing import Annotated

from fastapi import APIRouter, Form, HTTPException, Path

from api.schemas import (
    BuildInfoResponse,
    InstantQueryResponse,
    LabelNamesResponse,
    LabelValuesResponse,
    QueryRangeResponse,
)
from api.services.prometheus import PrometheusService

logger = logging.getLogger(__name__)
router = APIRouter(prefix='/api/v1')


@router.post('/query', response_model=InstantQueryResponse)
async def instant_query(
    query: Annotated[str, Form(..., example='up')],
    time: Annotated[float, Form(default_factory=lambda: datetime.now().timestamp())],
) -> InstantQueryResponse:
    try:
        return await PrometheusService.handle_instant_query(query, time)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f'Invalid query: {e}') from e
    except Exception as e:
        logger.exception('Database error in /query')
        raise HTTPException(status_code=500, detail='Database error') from e


@router.post('/query_range', response_model=QueryRangeResponse)
async def query_range(
    query: Annotated[str, Form(..., example='http_requests_total{job="api"}')],
    start: Annotated[float, Form(...)],
    end: Annotated[float, Form(...)],
    step: Annotated[int, Form(..., ge=1, description='Step in seconds')],
) -> QueryRangeResponse:
    try:
        return await PrometheusService.handle_range_query(query, start, end, step)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f'Invalid query: {e}') from e
    except Exception as e:
        logger.exception('Database error in /query_range')
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
        logger.exception('Database error in /label/.../values')
        raise HTTPException(status_code=500, detail='Database error') from e


@router.get('/status/buildinfo', response_model=BuildInfoResponse)
async def get_build_info() -> BuildInfoResponse:
    logger.debug('Build info requested')
    return PrometheusService.get_build_info()

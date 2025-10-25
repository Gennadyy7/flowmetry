from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from api.db import timescale_db
from api.prometheus_formatter import formatter

router = APIRouter()


@router.get('/metrics', response_class=PlainTextResponse)
async def prometheus_metrics() -> str:
    metrics = await timescale_db.fetch_all_metrics()
    output = formatter.format_metrics(metrics)
    return output

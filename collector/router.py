from fastapi import APIRouter, HTTPException

from collector.converters import convert_otlp_to_internal
from collector.main import redis_stream_client
from collector.otlp.schemas import OTLPMetricsRequest

router = APIRouter(prefix='/v1')


@router.post('/metrics')
async def ingest_metrics(request: OTLPMetricsRequest) -> dict[str, int]:
    try:
        points = convert_otlp_to_internal(request)
        for point in points:
            await redis_stream_client.send_message(point.model_dump())
        return {'received': len(points)}
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f'Invalid OTLP data: {str(e)}'
        ) from None

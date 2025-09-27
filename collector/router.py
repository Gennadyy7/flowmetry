from fastapi import APIRouter, Depends, HTTPException

from collector.converters import convert_otlp_to_internal
from collector.otlp.dependencies import parse_otlp_metrics_request
from collector.otlp.schemas import OTLPMetricsRequest
from collector.redis_stream_client import redis_stream_client

router = APIRouter(prefix='/v1')


@router.post('/metrics')
async def ingest_metrics(
    request: OTLPMetricsRequest = Depends(parse_otlp_metrics_request),  # noqa: B008
) -> dict[str, int]:
    print(f'{request=}')
    try:
        points = convert_otlp_to_internal(request)
        i = 1
        for point in points:
            print(f'{i=} {point=}')
            i += 1
            await redis_stream_client.send_message(point.model_dump())
        return {'received': len(points)}
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f'Invalid OTLP data: {str(e)}'
        ) from None

from fastapi import HTTPException, Request
from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)

from collector.otlp.schemas import OTLPMetricsRequest


async def parse_otlp_metrics_request(request: Request) -> OTLPMetricsRequest:
    content_type = request.headers.get('content-type', '').lower()
    if 'application/x-protobuf' in content_type:
        try:
            body = await request.body()

            otlp_request = ExportMetricsServiceRequest()
            otlp_request.ParseFromString(body)

            otlp_dict = MessageToDict(
                otlp_request,
                use_integers_for_enums=False,
                preserving_proto_field_name=True,
            )
            return OTLPMetricsRequest.model_validate(otlp_dict)
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f'Failed to parse OTLP/HTTP+Protobuf: {str(e)}'
            ) from e
    else:
        raise HTTPException(
            status_code=415, detail=f'Unsupported Content-Type: {content_type}'
        )

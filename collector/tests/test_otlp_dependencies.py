from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
import pytest

from collector.otlp.dependencies import parse_otlp_metrics_request


class TestOTLPDependencies:
    async def test_parse_protobuf_success(self) -> None:
        mock_request = AsyncMock()
        mock_request.headers = {'content-type': 'application/x-protobuf'}

        mock_body = b'\n\x00'
        mock_request.body.return_value = mock_body

        with patch(
            'collector.otlp.dependencies.ExportMetricsServiceRequest'
        ) as mock_proto:
            with patch('collector.otlp.dependencies.MessageToDict') as mock_to_dict:
                mock_to_dict.return_value = {'resource': {}, 'scope_metrics': []}

                with patch(
                    'collector.otlp.dependencies.OTLPMetricsRequest'
                ) as mock_schema:
                    mock_schema.model_validate.return_value = AsyncMock()

                    await parse_otlp_metrics_request(mock_request)

                    mock_proto.assert_called_once()
                    mock_proto.return_value.ParseFromString.assert_called_once_with(
                        mock_body
                    )
                    mock_to_dict.assert_called_once()
                    mock_schema.model_validate.assert_called_once()

    async def test_parse_protobuf_parse_error(self) -> None:
        mock_request = AsyncMock()
        mock_request.headers = {'content-type': 'application/x-protobuf'}
        mock_request.body.return_value = b'invalid_protobuf'

        with patch(
            'collector.otlp.dependencies.ExportMetricsServiceRequest'
        ) as mock_proto:
            mock_proto.return_value.ParseFromString.side_effect = Exception(
                'Parse error'
            )

            with pytest.raises(HTTPException) as exc_info:
                await parse_otlp_metrics_request(mock_request)

            assert exc_info.value.status_code == 400
            assert 'Failed to parse OTLP/HTTP+Protobuf: Parse error' in str(
                exc_info.value.detail
            )

    async def test_unsupported_content_type(self) -> None:
        mock_request = AsyncMock()
        mock_request.headers = {'content-type': 'application/json'}

        with pytest.raises(HTTPException) as exc_info:
            await parse_otlp_metrics_request(mock_request)

        assert exc_info.value.status_code == 415
        assert 'Unsupported Content-Type: application/json' in str(
            exc_info.value.detail
        )

    async def test_missing_content_type(self) -> None:
        mock_request = AsyncMock()
        mock_request.headers = {}

        with pytest.raises(HTTPException) as exc_info:
            await parse_otlp_metrics_request(mock_request)

        assert exc_info.value.status_code == 415
        assert 'Unsupported Content-Type: ' in str(exc_info.value.detail)

    async def test_content_type_case_insensitive(self) -> None:
        mock_request = AsyncMock()
        mock_request.headers = {'content-type': 'APPLICATION/X-PROTOBUF'}
        mock_request.body.return_value = b'\n\x00'

        with patch(
            'collector.otlp.dependencies.ExportMetricsServiceRequest'
        ) as mock_proto:
            with patch('collector.otlp.dependencies.MessageToDict') as mock_to_dict:
                mock_to_dict.return_value = {'resource': {}, 'scope_metrics': []}

                with patch(
                    'collector.otlp.dependencies.OTLPMetricsRequest'
                ) as mock_schema:
                    mock_schema.model_validate.return_value = AsyncMock()

                    await parse_otlp_metrics_request(mock_request)

                    mock_proto.assert_called_once()
                    mock_proto.return_value.ParseFromString.assert_called_once_with(
                        b'\n\x00'
                    )

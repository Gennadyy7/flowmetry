from api.schemas import (
    BuildInfoData,
    BuildInfoResponse,
    InstantQueryData,
    InstantQueryResponse,
    InstantResultItem,
    LabelNamesResponse,
    LabelValuesResponse,
    MetricLabels,
    QueryRangeData,
    QueryRangeResponse,
    ResultItem,
    SeriesItem,
    SeriesResponse,
)


class TestSchemas:
    def test_metric_labels_to_dict(self) -> None:
        labels = MetricLabels(__name__='test_metric')
        setattr(labels, 'service', 'api')  # noqa: B010
        result = labels.to_dict()

        assert result == {'__name__': 'test_metric', 'service': 'api'}

    def test_metric_labels_to_dict_empty(self) -> None:
        labels = MetricLabels(__name__='test')
        result = labels.to_dict()

        assert result == {'__name__': 'test'}

    def test_instant_result_item(self) -> None:
        labels = MetricLabels(__name__='test')
        setattr(labels, 'service', 'api')  # noqa: B010
        item = InstantResultItem(metric=labels, value=(1234567890.0, '42'))

        assert item.metric.__name__ == 'test'
        assert getattr(item.metric, 'service', None) == 'api'
        assert item.value == (1234567890.0, '42')

    def test_result_item(self) -> None:
        labels = MetricLabels(__name__='test')
        setattr(labels, 'service', 'api')  # noqa: B010
        values = [(1234567890.0, '42'), (1234567950.0, '43')]
        item = ResultItem(metric=labels, values=values)

        assert item.metric.__name__ == 'test'
        assert getattr(item.metric, 'service', None) == 'api'
        assert len(item.values) == 2

    def test_series_item_to_dict(self) -> None:
        item = SeriesItem()
        setattr(item, '__name__', 'test')  # noqa: B010
        setattr(item, 'service', 'api')  # noqa: B010
        setattr(item, 'version', '1.0')  # noqa: B010
        result = item.to_dict()

        # SeriesItem doesn't have __name__ as a field, so it won't be in model_dump()
        assert result == {'service': 'api', 'version': '1.0'}

    def test_build_info_data_defaults(self) -> None:
        data = BuildInfoData()

        assert data.version == '0.1.0'
        assert data.revision == 'custom'
        assert data.branch == 'master'
        assert data.buildUser == 'flowmetry'
        assert data.goVersion == 'go1.21'
        assert data.platform == 'linux/amd64'

    def test_build_info_response(self) -> None:
        data = BuildInfoData()
        response = BuildInfoResponse(data=data)

        assert response.status == 'success'
        assert response.data.version == '0.1.0'

    def test_instant_query_response(self) -> None:
        data = InstantQueryData(resultType='vector', result=[])
        response = InstantQueryResponse(data=data)

        assert response.status == 'success'
        assert response.data.resultType == 'vector'
        assert len(response.data.result) == 0

    def test_query_range_response(self) -> None:
        data = QueryRangeData(resultType='matrix', result=[])
        response = QueryRangeResponse(data=data)

        assert response.status == 'success'
        assert response.data.resultType == 'matrix'
        assert len(response.data.result) == 0

    def test_label_names_response(self) -> None:
        response = LabelNamesResponse(data=['service', 'job'])

        assert response.status == 'success'
        assert len(response.data) == 2
        assert 'service' in response.data
        assert 'job' in response.data

    def test_label_values_response(self) -> None:
        response = LabelValuesResponse(data=['api', 'worker'])

        assert response.status == 'success'
        assert len(response.data) == 2
        assert 'api' in response.data
        assert 'worker' in response.data

    def test_series_response(self) -> None:
        data = [{'__name__': 'test', 'service': 'api'}]
        response = SeriesResponse(status='success', data=data)

        assert response.status == 'success'
        assert len(response.data) == 1
        assert response.data[0]['__name__'] == 'test'

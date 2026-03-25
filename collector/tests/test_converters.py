from collector.converters import (
    _attributes_to_dict,
    _convert_histogram_data_point,
    _convert_number_data_point,
    _normalize_attribute_key,
    _parse_any_value,
    _should_keep_attribute,
    convert_otlp_to_internal,
)
from collector.internal.schemas import MetricType
from collector.otlp.schemas import (
    AnyValue,
    Gauge,
    Histogram,
    HistogramDataPoint,
    KeyValue,
    Metric,
    NumberDataPoint,
    OTLPMetricsRequest,
    Resource,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)


class TestParseAnyValue:
    def test_parse_string_value(self) -> None:
        kv = KeyValue(key='test', value=AnyValue(string_value='hello'))
        assert _parse_any_value(kv) == 'hello'

    def test_parse_bool_value_true(self) -> None:
        kv = KeyValue(key='test', value=AnyValue(bool_value=True))
        assert _parse_any_value(kv) == 'true'

    def test_parse_bool_value_false(self) -> None:
        kv = KeyValue(key='test', value=AnyValue(bool_value=False))
        assert _parse_any_value(kv) == 'false'

    def test_parse_int_value(self) -> None:
        kv = KeyValue(key='test', value=AnyValue(int_value='42'))
        assert _parse_any_value(kv) == '42'

    def test_parse_double_value_integer(self) -> None:
        kv = KeyValue(key='test', value=AnyValue(double_value=42.0))
        assert _parse_any_value(kv) == '42'

    def test_parse_double_value_float(self) -> None:
        kv = KeyValue(key='test', value=AnyValue(double_value=42.5))
        assert _parse_any_value(kv) == '42.5'

    def test_parse_empty_value(self) -> None:
        kv = KeyValue(key='test', value=AnyValue())
        assert _parse_any_value(kv) == ''


class TestNormalizeAttributeKey:
    def test_normalize_with_dots(self) -> None:
        assert _normalize_attribute_key('service.name') == 'service_name'

    def test_normalize_without_dots(self) -> None:
        assert _normalize_attribute_key('service_name') == 'service_name'

    def test_normalize_empty_string(self) -> None:
        assert _normalize_attribute_key('') == ''


class TestShouldKeepAttribute:
    def test_keep_regular_attribute(self) -> None:
        assert _should_keep_attribute('custom.attribute') is True

    def test_filter_telemetry_sdk_attribute(self) -> None:
        assert _should_keep_attribute('telemetry.sdk.name') is False

    def test_filter_otel_scope_attribute(self) -> None:
        assert _should_keep_attribute('otel.scope.name') is False

    def test_filter_otel_library_attribute(self) -> None:
        assert _should_keep_attribute('otel.library.name') is False

    def test_keep_similar_attribute(self) -> None:
        assert _should_keep_attribute('custom.telemetry.sdk.name') is True


class TestAttributesToDict:
    def test_convert_valid_attributes(self) -> None:
        attributes = [
            KeyValue(key='attr1', value=AnyValue(string_value='value1')),
            KeyValue(key='attr2', value=AnyValue(int_value='123')),
        ]
        result = _attributes_to_dict(attributes)
        assert result == {'attr1': 'value1', 'attr2': '123'}

    def test_filter_system_attributes(self) -> None:
        attributes = [
            KeyValue(key='good.attr', value=AnyValue(string_value='keep')),
            KeyValue(key='telemetry.sdk.name', value=AnyValue(string_value='filter')),
        ]
        result = _attributes_to_dict(attributes)
        assert result == {'good_attr': 'keep'}

    def test_skip_empty_values(self) -> None:
        attributes = [
            KeyValue(key='empty', value=AnyValue()),
            KeyValue(key='valid', value=AnyValue(string_value='value')),
        ]
        result = _attributes_to_dict(attributes)
        assert result == {'valid': 'value'}

    def test_normalize_keys(self) -> None:
        attributes = [
            KeyValue(key='service.name', value=AnyValue(string_value='myservice')),
            KeyValue(key='normal_key', value=AnyValue(string_value='normal')),
        ]
        result = _attributes_to_dict(attributes)
        assert result == {'service_name': 'myservice', 'normal_key': 'normal'}


class TestConvertNumberDataPoint:
    def test_convert_counter_metric(self) -> None:
        dp = NumberDataPoint(
            attributes=[KeyValue(key='env', value=AnyValue(string_value='prod'))],
            time_unix_nano='1234567890',
            as_int='42',
        )
        metric = Metric(
            name='requests_total', description='Total requests', unit='count'
        )

        result = _convert_number_data_point(dp, metric, MetricType.COUNTER)

        assert result.name == 'requests_total'
        assert result.description == 'Total requests'
        assert result.unit == 'count'
        assert result.type == MetricType.COUNTER
        assert result.timestamp_nano == 1234567890
        assert result.attributes == {'env': 'prod'}
        assert result.value == 42

    def test_convert_gauge_metric_with_double(self) -> None:
        dp = NumberDataPoint(
            attributes=[],
            time_unix_nano='1234567890',
            as_double=3.14,
        )
        metric = Metric(name='cpu_usage', description='CPU usage', unit='percent')

        result = _convert_number_data_point(dp, metric, MetricType.GAUGE)

        assert result.name == 'cpu_usage'
        assert result.type == MetricType.GAUGE
        assert result.value == 3.14

    def test_convert_metric_without_value(self) -> None:
        dp = NumberDataPoint(
            attributes=[],
            time_unix_nano='1234567890',
        )
        metric = Metric(name='test_metric', description='Test', unit='unit')

        result = _convert_number_data_point(dp, metric, MetricType.COUNTER)

        assert result.value == 0


class TestConvertHistogramDataPoint:
    def test_convert_histogram_metric(self) -> None:
        dp = HistogramDataPoint(
            attributes=[KeyValue(key='le', value=AnyValue(string_value='100'))],
            time_unix_nano='1234567890',
            count='100',
            sum=1500.5,
            bucket_counts=['10', '50', '90', '100'],
            explicit_bounds=[1.0, 5.0, 10.0],
        )
        metric = Metric(name='response_time', description='Response time', unit='ms')

        result = _convert_histogram_data_point(dp, metric)

        assert result.name == 'response_time'
        assert result.description == 'Response time'
        assert result.unit == 'ms'
        assert result.type == MetricType.HISTOGRAM
        assert result.timestamp_nano == 1234567890
        assert result.attributes == {'le': '100'}
        assert result.count == 100
        assert result.sum == 1500.5
        assert result.bucket_counts == [10, 50, 90, 100]
        assert result.explicit_bounds == [1.0, 5.0, 10.0]


class TestConvertOtlpToInternal:
    def test_convert_counter_metrics(self) -> None:
        request = OTLPMetricsRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=Resource(
                        attributes=[
                            KeyValue(
                                key='service.name',
                                value=AnyValue(string_value='test-service'),
                            )
                        ]
                    ),
                    scope_metrics=[
                        ScopeMetrics(
                            metrics=[
                                Metric(
                                    name='requests_total',
                                    description='Total requests',
                                    unit='count',
                                    sum=Sum(
                                        data_points=[
                                            NumberDataPoint(
                                                attributes=[
                                                    KeyValue(
                                                        key='method',
                                                        value=AnyValue(
                                                            string_value='GET'
                                                        ),
                                                    )
                                                ],
                                                time_unix_nano='1234567890',
                                                as_int='10',
                                            )
                                        ],
                                        aggregation_temporality='cumulative',
                                        is_monotonic=True,
                                    ),
                                )
                            ]
                        )
                    ],
                )
            ]
        )

        result = convert_otlp_to_internal(request)

        assert len(result) == 1
        point = result[0]
        assert point.name == 'requests_total'
        assert point.type == MetricType.COUNTER
        assert point.value == 10
        assert point.attributes == {'method': 'GET', 'service_name': 'test-service'}

    def test_convert_gauge_metrics(self) -> None:
        request = OTLPMetricsRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=Resource(attributes=[]),
                    scope_metrics=[
                        ScopeMetrics(
                            metrics=[
                                Metric(
                                    name='cpu_usage',
                                    description='CPU usage',
                                    unit='percent',
                                    gauge=Gauge(
                                        data_points=[
                                            NumberDataPoint(
                                                attributes=[],
                                                time_unix_nano='1234567890',
                                                as_double=75.5,
                                            )
                                        ]
                                    ),
                                )
                            ]
                        )
                    ],
                )
            ]
        )

        result = convert_otlp_to_internal(request)

        assert len(result) == 1
        point = result[0]
        assert point.name == 'cpu_usage'
        assert point.type == MetricType.GAUGE
        assert point.value == 75.5

    def test_convert_histogram_metrics(self) -> None:
        request = OTLPMetricsRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=Resource(attributes=[]),
                    scope_metrics=[
                        ScopeMetrics(
                            metrics=[
                                Metric(
                                    name='response_time',
                                    description='Response time',
                                    unit='ms',
                                    histogram=Histogram(
                                        data_points=[
                                            HistogramDataPoint(
                                                attributes=[],
                                                time_unix_nano='1234567890',
                                                count='50',
                                                sum=1250.0,
                                                bucket_counts=['5', '25', '45', '50'],
                                                explicit_bounds=[10.0, 50.0, 100.0],
                                            )
                                        ]
                                    ),
                                )
                            ]
                        )
                    ],
                )
            ]
        )

        result = convert_otlp_to_internal(request)

        assert len(result) == 1
        point = result[0]
        assert point.name == 'response_time'
        assert point.type == MetricType.HISTOGRAM
        assert point.count == 50
        assert point.sum == 1250.0
        assert point.bucket_counts == [5, 25, 45, 50]
        assert point.explicit_bounds == [10.0, 50.0, 100.0]

    def test_convert_multiple_metrics(self) -> None:
        request = OTLPMetricsRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=Resource(attributes=[]),
                    scope_metrics=[
                        ScopeMetrics(
                            metrics=[
                                Metric(
                                    name='counter1',
                                    sum=Sum(
                                        data_points=[
                                            NumberDataPoint(
                                                attributes=[],
                                                time_unix_nano='1234567890',
                                                as_int='5',
                                            )
                                        ],
                                        aggregation_temporality='cumulative',
                                        is_monotonic=True,
                                    ),
                                ),
                                Metric(
                                    name='counter2',
                                    sum=Sum(
                                        data_points=[
                                            NumberDataPoint(
                                                attributes=[],
                                                time_unix_nano='1234567890',
                                                as_int='10',
                                            )
                                        ],
                                        aggregation_temporality='cumulative',
                                        is_monotonic=True,
                                    ),
                                ),
                            ]
                        )
                    ],
                )
            ]
        )

        result = convert_otlp_to_internal(request)

        assert len(result) == 2
        assert result[0].name == 'counter1'
        assert result[0].value == 5
        assert result[1].name == 'counter2'
        assert result[1].value == 10

    def test_convert_empty_request(self) -> None:
        request = OTLPMetricsRequest(resource_metrics=[])
        result = convert_otlp_to_internal(request)
        assert result == []

    def test_convert_request_with_empty_resource_metrics(self) -> None:
        request = OTLPMetricsRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=Resource(attributes=[]),
                    scope_metrics=[],
                )
            ]
        )
        result = convert_otlp_to_internal(request)
        assert result == []

from collector.internal.schemas import MetricPoint, MetricType
from collector.otlp.schemas import (
    HistogramDataPoint,
    KeyValue,
    Metric,
    NumberDataPoint,
    OTLPMetricsRequest,
)


def _parse_any_value(kv: KeyValue) -> str:
    v = kv.value
    if v.string_value is not None:
        return v.string_value
    if v.bool_value is not None:
        return str(v.bool_value).lower()
    if v.int_value is not None:
        return str(int(v.int_value))
    if v.double_value is not None:
        val = v.double_value
        return str(int(val)) if val.is_integer() else str(val)
    return ''  # fallback


def _normalize_attribute_key(key: str) -> str:
    return key.replace('.', '_')


def _should_keep_attribute(key: str) -> bool:
    system_prefixes = {
        'telemetry.sdk.',
        'otel.scope.',
        'otel.library.',
    }

    return not any(key.startswith(prefix) for prefix in system_prefixes)


def _attributes_to_dict(
    attributes: list[KeyValue],
) -> dict[str, str]:
    result = {}
    for kv in attributes:
        if not _should_keep_attribute(kv.key):
            continue
        clean_key = _normalize_attribute_key(kv.key)
        clean_value = _parse_any_value(kv)
        if clean_value:
            result[clean_key] = clean_value

    return result


def _convert_number_data_point(
    dp: NumberDataPoint, metric: Metric, metric_type: MetricType
) -> MetricPoint:
    value: int | float
    if dp.as_int is not None:
        value = int(dp.as_int)
    elif dp.as_double is not None:
        value = dp.as_double
    else:
        value = 0

    return MetricPoint(
        name=metric.name,
        description=metric.description,
        unit=metric.unit,
        type=metric_type,
        timestamp_nano=int(dp.time_unix_nano),
        attributes=_attributes_to_dict(dp.attributes),
        value=value,
    )


def _convert_histogram_data_point(
    dp: HistogramDataPoint, metric: Metric
) -> MetricPoint:
    return MetricPoint(
        name=metric.name,
        description=metric.description,
        unit=metric.unit,
        type=MetricType.HISTOGRAM,
        timestamp_nano=int(dp.time_unix_nano),
        attributes=_attributes_to_dict(dp.attributes),
        sum=dp.sum,
        count=int(dp.count),
        bucket_counts=[int(bc) for bc in dp.bucket_counts],
        explicit_bounds=dp.explicit_bounds,
    )


def convert_otlp_to_internal(request: OTLPMetricsRequest) -> list[MetricPoint]:
    all_points: list[MetricPoint] = []

    for rm in request.resource_metrics:
        resource_attrs = _attributes_to_dict(rm.resource.attributes)

        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.sum:
                    for dp_num in metric.sum.data_points:
                        point = _convert_number_data_point(
                            dp_num, metric, MetricType.COUNTER
                        )
                        point.attributes.update(resource_attrs)
                        all_points.append(point)
                elif metric.gauge:
                    for dp_num in metric.gauge.data_points:
                        point = _convert_number_data_point(
                            dp_num, metric, MetricType.GAUGE
                        )
                        point.attributes.update(resource_attrs)
                        all_points.append(point)
                elif metric.histogram:
                    for dp_hist in metric.histogram.data_points:
                        point = _convert_histogram_data_point(dp_hist, metric)
                        point.attributes.update(resource_attrs)
                        all_points.append(point)
    return all_points

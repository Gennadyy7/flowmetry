from collector.internal.schemas import MetricPoint, MetricType
from collector.otlp.schemas import (
    HistogramDataPoint,
    KeyValue,
    Metric,
    NumberDataPoint,
    OTLPMetricsRequest,
)


def _parse_any_value(kv: KeyValue) -> str | int | float | bool:
    v = kv.value
    if v.stringValue is not None:
        return v.stringValue
    if v.boolValue is not None:
        return v.boolValue
    if v.intValue is not None:
        return int(v.intValue)
    if v.doubleValue is not None:
        return v.doubleValue
    return ''  # fallback


def _attributes_to_dict(
    attributes: list[KeyValue],
) -> dict[str, str | int | float | bool]:
    return {kv.key: _parse_any_value(kv) for kv in attributes}


def _convert_number_data_point(
    dp: NumberDataPoint, metric: Metric, metric_type: MetricType
) -> MetricPoint:
    value: int | float
    if dp.asInt is not None:
        value = int(dp.asInt)
    elif dp.asDouble is not None:
        value = dp.asDouble
    else:
        value = 0

    return MetricPoint(
        name=metric.name,
        description=metric.description,
        unit=metric.unit,
        type=metric_type,
        timestamp_nano=int(dp.timeUnixNano),
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
        timestamp_nano=int(dp.timeUnixNano),
        attributes=_attributes_to_dict(dp.attributes),
        sum=dp.sum,
        count=int(dp.count),
        bucket_counts=[int(bc) for bc in dp.bucketCounts],
        explicit_bounds=dp.explicitBounds,
    )


def convert_otlp_to_internal(request: OTLPMetricsRequest) -> list[MetricPoint]:
    all_points: list[MetricPoint] = []

    for rm in request.resourceMetrics:
        resource_attrs = _attributes_to_dict(rm.resource.attributes)

        for sm in rm.scopeMetrics:
            for metric in sm.metrics:
                if metric.sum:
                    for dp_num in metric.sum.dataPoints:
                        point = _convert_number_data_point(
                            dp_num, metric, MetricType.COUNTER
                        )
                        point.attributes.update(resource_attrs)
                        all_points.append(point)
                elif metric.gauge:
                    for dp_num in metric.gauge.dataPoints:
                        point = _convert_number_data_point(
                            dp_num, metric, MetricType.GAUGE
                        )
                        point.attributes.update(resource_attrs)
                        all_points.append(point)
                elif metric.histogram:
                    for dp_hist in metric.histogram.dataPoints:
                        point = _convert_histogram_data_point(dp_hist, metric)
                        point.attributes.update(resource_attrs)
                        all_points.append(point)
    return all_points

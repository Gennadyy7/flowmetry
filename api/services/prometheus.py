from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
import logging
from typing import Any

from api.db import timescale_db
from api.promql_parser import ParsedQuery, parser
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
    SeriesResponse,
)

logger = logging.getLogger(__name__)


class PrometheusService:
    @classmethod
    async def handle_instant_query(
        cls, query: str, timestamp: float
    ) -> InstantQueryResponse:
        logger.debug(
            'Handling instant query', extra={'query': query, 'timestamp': timestamp}
        )

        try:
            parsed: ParsedQuery = parser.parse(query)
        except Exception as e:
            logger.warning(
                'Invalid PromQL query', extra={'query': query, 'error': str(e)}
            )
            raise

        if parsed.quantile is not None and parsed.histogram_metric is not None:
            logger.debug(
                'Handling histogram_quantile query',
                extra={
                    'quantile': parsed.quantile,
                    'metric': parsed.histogram_metric,
                    'function': parsed.function,
                },
            )
            series = await cls._handle_histogram_quantile(parsed, timestamp)
            return cls._generate_instant_query_response(
                parsed=parsed,
                series=series,
                timestamp=datetime.fromtimestamp(timestamp, tz=UTC),
            )

        if parsed.scalar_value is not None:
            return InstantQueryResponse(
                data=InstantQueryData(
                    resultType='vector',
                    result=[
                        InstantResultItem(
                            metric=MetricLabels(__name__=parsed.raw),
                            value=(timestamp, parsed.scalar_value),
                        )
                    ],
                )
            )

        base_name, component = cls._extract_histogram_components(
            parsed.metric_name or ''
        )

        if base_name and await timescale_db.is_histogram_metric(base_name):
            series = await timescale_db.fetch_histogram_series_for_range(
                metric_name=base_name,
                component=component,
                labels=parsed.labels,
                start_ts=timestamp - 60,
                end_ts=timestamp,
                step_seconds=60,
            )
            return cls._generate_instant_query_response(
                parsed=parsed,
                series=series,
                timestamp=datetime.fromtimestamp(timestamp, tz=UTC),
            )

        timestamp_dt = datetime.fromtimestamp(timestamp, tz=UTC)

        if parsed.metric_name == 'up':
            series = [
                (
                    'up',
                    parsed.labels,
                    1.0,
                    timestamp_dt,
                )
            ]

            return cls._generate_instant_query_response(
                parsed=parsed,
                series=series,
                timestamp=timestamp_dt,
            )

        if parsed.function in ('rate', 'increase'):
            lookback = parsed.get_lookback_seconds()
            if lookback <= 0:
                raise ValueError(
                    f'Function {parsed.function} requires lookback window, got {lookback}'
                )

            start_ts = timestamp - lookback
            raw_series = await timescale_db.fetch_timeseries_for_range(
                metric_name=parsed.metric_name or '',
                labels=parsed.labels,
                start_ts=start_ts,
                end_ts=timestamp,
                step_seconds=lookback,
                function=parsed.function,
                lookback_seconds=lookback,
            )

            series = raw_series
            return cls._generate_instant_query_response(
                parsed=parsed,
                series=series,
                timestamp=timestamp_dt,
            )

        series = await timescale_db.fetch_metric_instant(
            metric_name=parsed.metric_name or '',
            labels=parsed.labels,
            timestamp=timestamp,
        )

        return cls._generate_instant_query_response(
            parsed=parsed,
            series=series,
            timestamp=None,
        )

    @staticmethod
    def _generate_instant_query_response(
        parsed: ParsedQuery,
        series: list[tuple[str, dict[str, Any], float, datetime]],
        timestamp: datetime | None = None,
    ) -> InstantQueryResponse:
        result_items = []
        for name, attrs, value, ts in series:
            if '_bucket' in name or '_sum' in name or '_count' in name:
                effective_name = name
            else:
                effective_name = parsed.get_effective_metric_name()

            result_items.append(
                InstantResultItem(
                    metric=MetricLabels(**{'__name__': effective_name, **attrs}),
                    value=((timestamp or ts).timestamp(), str(value)),
                )
            )

        return InstantQueryResponse(
            data=InstantQueryData(
                resultType='vector',
                result=result_items,
            )
        )

    @classmethod
    async def handle_range_query(
        cls, query: str, start: float, end: float, step: int
    ) -> QueryRangeResponse:
        logger.debug(
            'Handling range query',
            extra={'query': query, 'start': start, 'end': end, 'step': step},
        )

        try:
            parsed: ParsedQuery = parser.parse(query)
        except Exception as e:
            logger.warning(
                'Invalid PromQL query', extra={'query': query, 'error': str(e)}
            )
            raise

        logger.debug(
            'PARSED QUERY DETAILS',
            extra={
                'raw_query': query,
                'metric_name': parsed.metric_name,
                'labels': parsed.labels,
                'function': parsed.function,
                'range_seconds': parsed.range.seconds if parsed.range else None,
                'step_input': step,
            },
        )

        if parsed.quantile is not None and parsed.histogram_metric is not None:
            logger.debug(
                'Handling histogram_quantile range query',
                extra={
                    'quantile': parsed.quantile,
                    'metric': parsed.histogram_metric,
                    'function': parsed.function,
                    'start': start,
                    'end': end,
                    'step': step,
                },
            )
            series = await cls._handle_histogram_quantile_range(
                parsed, start, end, step
            )
            return cls._generate_query_range_response(parsed=parsed, series=series)

        if parsed.scalar_value is not None:
            logger.warning(
                'Scalar in range query',
                extra={'query': query, 'scalar_value': parsed.scalar_value},
            )
            raise ValueError('Invalid expression type "scalar" for range query')

        base_name, component = cls._extract_histogram_components(
            parsed.metric_name or ''
        )

        if base_name and await timescale_db.is_histogram_metric(base_name):
            series = await timescale_db.fetch_histogram_series_for_range(
                metric_name=base_name,
                component=component,
                labels=parsed.labels,
                start_ts=start,
                end_ts=end,
                step_seconds=step,
            )
            return cls._generate_query_range_response(
                parsed=parsed,
                series=series,
            )

        if parsed.metric_name == 'up':
            series = []
            current_ts = start
            while current_ts <= end:
                series.append(
                    (
                        'up',
                        parsed.labels,
                        1.0,
                        datetime.fromtimestamp(current_ts, tz=UTC),
                    )
                )
                current_ts += step

            return cls._generate_query_range_response(
                parsed=parsed,
                series=series,
            )

        lookback_seconds = 0
        if parsed.function in ('rate', 'increase'):
            lookback_seconds = parsed.get_lookback_seconds()
            if lookback_seconds <= 0:
                raise ValueError(
                    f'Function {parsed.function} requires lookback window, got {lookback_seconds}'
                )
            elif step > lookback_seconds:
                logger.warning(
                    f'Step ({step}s) > lookback window ({lookback_seconds}s) for {parsed.function}. '
                    f'Results may be inaccurate.'
                )

        series = await timescale_db.fetch_timeseries_for_range(
            metric_name=parsed.metric_name or '',
            labels=parsed.labels,
            start_ts=start,
            end_ts=end,
            step_seconds=step,
            function=parsed.function,
            lookback_seconds=lookback_seconds,
        )

        logger.debug(f'Fetched {len(series)} series points')

        if parsed.aggregation:
            series = cls._apply_aggregation(
                series, parsed.aggregation, parsed.by_labels
            )

        return cls._generate_query_range_response(
            parsed=parsed,
            series=series,
        )

    @staticmethod
    def _generate_query_range_response(
        parsed: ParsedQuery,
        series: list[tuple[str, dict[str, Any], float, datetime]],
    ) -> QueryRangeResponse:
        grouped: dict[tuple[tuple[str, str], ...], list[tuple[float, str]]] = (
            defaultdict(list)
        )
        for _name, attrs, value, ts in series:
            str_attrs = {k: str(v) for k, v in attrs.items()}
            key = tuple(sorted(str_attrs.items()))
            grouped[key].append((ts.timestamp(), str(value)))

        result_items = []
        effective_name = parsed.get_effective_metric_name()
        for label_key, points in grouped.items():
            labels_dict = dict(label_key)
            labels_dict['__name__'] = effective_name
            result_items.append(
                ResultItem(
                    metric=MetricLabels(**labels_dict),
                    values=points,
                )
            )

        return QueryRangeResponse(
            data=QueryRangeData(
                resultType='matrix',
                result=result_items,
            )
        )

    @staticmethod
    async def get_series(match: list[str]) -> SeriesResponse:
        logger.debug('Fetching series', extra={'match': match})
        try:
            series_list = await timescale_db.fetch_series(matchers=match)
            return SeriesResponse(data=series_list)
        except Exception as e:
            logger.exception(f'Error in get_series: {e}', extra={'match': match})
            raise

    @staticmethod
    async def get_label_names() -> LabelNamesResponse:
        logger.debug('Fetching all label names')
        try:
            labels = await timescale_db.fetch_all_label_names()
            return LabelNamesResponse(data=labels)
        except Exception as e:
            logger.exception(f'Error in get_label_names: {e}')
            raise

    @staticmethod
    async def get_label_values(label_name: str) -> LabelValuesResponse:
        logger.debug('Fetching label values', extra={'label_name': label_name})
        try:
            values = await timescale_db.fetch_label_values(label_name)
            return LabelValuesResponse(data=values)
        except Exception as e:
            logger.exception(
                f'Error in get_label_values: {e}', extra={'label_name': label_name}
            )
            raise

    @staticmethod
    def get_build_info() -> BuildInfoResponse:
        logger.debug('Returning build info')
        return BuildInfoResponse(data=BuildInfoData())

    @staticmethod
    def _apply_aggregation(
        series: Sequence[tuple[str, dict[str, Any], float, datetime]],
        op: str,
        by_labels: list[str],
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        grouped: dict[tuple[tuple[str, ...], datetime], list[float]] = defaultdict(list)

        for name, attrs, value, ts in series:
            key_items = []
            for lbl in by_labels:
                if lbl == '__name__':
                    key_items.append(name)
                else:
                    key_items.append(str(attrs.get(lbl, '')))
            key = tuple(key_items)

            grouped[(key, ts)].append(value)

        result = []
        for (key, timestamp), values in grouped.items():
            if op == 'sum':
                agg_value = sum(values)
            elif op == 'avg':
                agg_value = sum(values) / len(values) if values else 0.0
            elif op == 'min':
                agg_value = min(values) if values else 0.0
            elif op == 'max':
                agg_value = max(values) if values else 0.0
            elif op == 'count':
                agg_value = float(len(values))
            else:
                agg_value = 0.0

            attrs = {}
            for i, lbl in enumerate(by_labels):
                if lbl != '__name__' and key[i]:
                    attrs[lbl] = key[i]

            result.append((op, attrs, agg_value, timestamp))

        return result

    @classmethod
    async def _handle_histogram_quantile(
        cls,
        parsed: ParsedQuery,
        timestamp: float,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        if parsed.quantile is None or parsed.histogram_metric is None:
            return []

        base_name, _ = cls._extract_histogram_components(parsed.histogram_metric or '')

        need_rate = parsed.function in ('rate', 'increase')
        lookback = parsed.get_lookback_seconds(default=300)

        start_ts = timestamp - (lookback if need_rate else 300)

        logger.debug(
            'Fetching histogram data for quantile',
            extra={
                'base_name': base_name,
                'need_rate': need_rate,
                'lookback': lookback,
                'start_ts': start_ts,
                'end_ts': timestamp,
            },
        )

        histogram_data = await timescale_db.fetch_histogram_data(
            metric_name=base_name,
            labels=parsed.labels,
            start_ts=start_ts,
            end_ts=timestamp,
        )

        if not histogram_data:
            logger.debug(
                'No histogram data found for quantile calculation',
                extra={'base_name': base_name, 'timestamp': timestamp},
            )
            return []

        result = []
        timestamp_dt = datetime.fromtimestamp(timestamp, tz=UTC)

        histogram_data.sort(key=lambda x: x[6])

        latest_entry = histogram_data[-1]
        name, attrs, bucket_counts, sum_val, count_val, bounds, time = latest_entry

        processed_bucket_counts: list[float] = [float(x) for x in bucket_counts]
        processed_count: float = float(count_val)

        if need_rate and len(histogram_data) >= 2:
            prev_entry = None
            for i in range(len(histogram_data) - 2, -1, -1):
                candidate = histogram_data[i]
                if candidate[6] < time:
                    prev_entry = candidate
                    break

            if prev_entry:
                prev_bucket_counts = prev_entry[2]
                prev_count = prev_entry[4]
                prev_time = prev_entry[6]

                time_diff = (time - prev_time).total_seconds()

                if time_diff > 0:
                    bucket_deltas: list[float] = [
                        max(0.0, float(curr - prev))
                        for curr, prev in zip(
                            bucket_counts, prev_bucket_counts, strict=False
                        )
                    ]

                    count_delta: float = max(0.0, float(count_val - prev_count))

                    processed_bucket_counts = [
                        delta / time_diff for delta in bucket_deltas
                    ]
                    processed_count = count_delta / time_diff

                    logger.debug(
                        'Computed histogram rate for quantile',
                        extra={
                            'time_diff': time_diff,
                            'count_delta': count_delta,
                            'processed_count': processed_count,
                            'sample_buckets': processed_bucket_counts[:3],
                        },
                    )
                else:
                    logger.warning(
                        'Time difference is zero or negative after deduplication',
                        extra={'time_diff': time_diff},
                    )
            else:
                logger.warning(
                    'No previous point with different timestamp found for rate calculation',
                    extra={'available_points': len(histogram_data)},
                )

        quantile_value = cls._calculate_histogram_quantile(
            quantile=parsed.quantile,
            bucket_counts=processed_bucket_counts,
            bounds=bounds,
            total_count=processed_count,
        )

        result_name = parsed.histogram_metric if parsed.histogram_metric else base_name

        result.append(
            (
                result_name,
                attrs,
                quantile_value,
                timestamp_dt,
            )
        )

        logger.debug(
            'Computed histogram quantile',
            extra={
                'quantile': parsed.quantile,
                'value': quantile_value,
                'count': processed_count,
                'buckets_count': len(processed_bucket_counts),
            },
        )

        return result

    @classmethod
    async def _handle_histogram_quantile_range(
        cls,
        parsed: ParsedQuery,
        start: float,
        end: float,
        step: int,
    ) -> list[tuple[str, dict[str, Any], float, datetime]]:
        if parsed.quantile is None or parsed.histogram_metric is None:
            return []

        base_name, _ = cls._extract_histogram_components(parsed.histogram_metric or '')

        need_rate = parsed.function in ('rate', 'increase')
        lookback = parsed.get_lookback_seconds(default=300)

        fetch_start = start - (lookback if need_rate else 0)

        logger.debug(
            'Fetching histogram data for range quantile',
            extra={
                'base_name': base_name,
                'need_rate': need_rate,
                'lookback': lookback,
                'fetch_start': fetch_start,
                'end': end,
            },
        )

        histogram_data = await timescale_db.fetch_histogram_data(
            metric_name=base_name,
            labels=parsed.labels,
            start_ts=fetch_start,
            end_ts=end,
        )

        if not histogram_data:
            logger.debug(
                'No histogram data found for range quantile',
                extra={'base_name': base_name, 'start': start, 'end': end},
            )
            return []

        result = []
        current_ts = start

        data_by_time: dict[datetime, list[tuple[Any, ...]]] = {}
        for entry in histogram_data:
            name, attrs, bucket_counts, sum_val, count_val, bounds, time = entry
            data_by_time.setdefault(time, []).append(entry)

        sorted_times = sorted(data_by_time.keys())

        while current_ts <= end:
            current_dt = datetime.fromtimestamp(current_ts, tz=UTC)

            closest_time = None
            min_diff = float('inf')
            max_diff = step * 1.5

            for time_key in sorted_times:
                diff = abs((time_key - current_dt).total_seconds())
                if diff < min_diff and diff <= max_diff:
                    min_diff = diff
                    closest_time = time_key

            if closest_time:
                entries = data_by_time[closest_time]

                for entry in entries:
                    name, attrs, bucket_counts, sum_val, count_val, bounds, time = entry

                    processed_bucket_counts: list[float] = [
                        float(x) for x in bucket_counts
                    ]
                    processed_count: float = float(count_val)

                    if need_rate:
                        prev_time = None
                        for t in reversed(sorted_times):
                            if t < closest_time:
                                prev_time = t
                                break

                        if prev_time and prev_time in data_by_time:
                            prev_entry = data_by_time[prev_time][0]
                            prev_bucket_counts = prev_entry[2]
                            prev_count = prev_entry[4]

                            time_diff = (closest_time - prev_time).total_seconds()

                            if time_diff > 0:
                                bucket_deltas: list[float] = [
                                    max(0.0, float(curr - prev))
                                    for curr, prev in zip(
                                        bucket_counts, prev_bucket_counts, strict=False
                                    )
                                ]

                                count_delta: float = max(
                                    0.0, float(count_val - prev_count)
                                )

                                processed_bucket_counts = [
                                    delta / time_diff for delta in bucket_deltas
                                ]
                                processed_count = count_delta / time_diff

                    quantile_value = cls._calculate_histogram_quantile(
                        quantile=parsed.quantile,
                        bucket_counts=processed_bucket_counts,
                        bounds=bounds,
                        total_count=processed_count,
                    )

                    result_name = (
                        parsed.histogram_metric
                        if parsed.histogram_metric
                        else base_name
                    )

                    result.append(
                        (
                            result_name,
                            attrs,
                            quantile_value,
                            current_dt,
                        )
                    )

            current_ts += step

        logger.debug(
            'Computed histogram quantile range',
            extra={
                'quantile': parsed.quantile,
                'points_count': len(result),
                'start': start,
                'end': end,
                'step': step,
            },
        )

        return result

    @staticmethod
    def _calculate_histogram_quantile(
        quantile: float,
        bucket_counts: list[float],
        bounds: list[float],
        total_count: float,
    ) -> float:
        if total_count == 0 or quantile < 0 or quantile > 1:
            return 0.0

        cumulative_counts = []
        cumulative = 0.0
        for count in bucket_counts:
            cumulative += count
            cumulative_counts.append(cumulative)

        target_count = quantile * total_count

        bucket_index = None
        for i, cum_count in enumerate(cumulative_counts):
            if cum_count >= target_count:
                bucket_index = i
                break

        if bucket_index is None:
            return bounds[-1] if bounds else 0.0

        lower_bound = 0.0 if bucket_index == 0 else bounds[bucket_index - 1]
        upper_bound = bounds[bucket_index]

        bucket_count = bucket_counts[bucket_index]

        count_below = 0.0 if bucket_index == 0 else cumulative_counts[bucket_index - 1]

        if bucket_count > 0:
            fraction = (target_count - count_below) / bucket_count
            return lower_bound + (upper_bound - lower_bound) * fraction
        else:
            return lower_bound

    @staticmethod
    def _extract_histogram_components(metric_name: str) -> tuple[str, str | None]:
        if metric_name.endswith('_bucket'):
            return metric_name[:-7], 'bucket'
        elif metric_name.endswith('_sum'):
            return metric_name[:-4], 'sum'
        elif metric_name.endswith('_count'):
            return metric_name[:-6], 'count'
        return metric_name, None

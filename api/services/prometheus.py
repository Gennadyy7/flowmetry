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
        for _name, attrs, value, ts in series:
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

        if parsed.scalar_value is not None:
            logger.warning(
                'Scalar in range query',
                extra={'query': query, 'scalar_value': parsed.scalar_value},
            )
            raise ValueError('Invalid expression type "scalar" for range query')

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

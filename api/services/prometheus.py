from collections import defaultdict
import logging
from typing import Any, cast

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
)

logger = logging.getLogger(__name__)


class PrometheusService:
    @staticmethod
    async def handle_instant_query(
        query: str, timestamp: float
    ) -> InstantQueryResponse:
        logger.debug(
            'Handling instant query', extra={'query': query, 'timestamp': timestamp}
        )

        try:
            parsed: ParsedQuery = parser.parse(query)
        except ValueError as e:
            logger.warning(
                'Invalid PromQL query', extra={'query': query, 'error': str(e)}
            )
            raise

        if parsed.func == 'scalar':
            return InstantQueryResponse(
                data=InstantQueryData(
                    resultType='vector',
                    result=[
                        InstantResultItem(
                            metric=MetricLabels(__name__=parsed.raw),
                            value=(timestamp, cast(str, parsed.scalar_value)),
                        )
                    ],
                )
            )

        series = await timescale_db.fetch_metric_instant(
            metric_name=parsed.metric_name,
            labels=parsed.labels,
            timestamp=timestamp,
        )

        result_items = [
            InstantResultItem(
                metric=MetricLabels(**{'__name__': name, **attrs}),
                value=(ts.timestamp(), str(value)),
            )
            for name, attrs, value, ts in series
        ]

        return InstantQueryResponse(
            data=InstantQueryData(
                resultType='vector',
                result=result_items,
            )
        )

    @staticmethod
    async def handle_range_query(
        query: str, start: float, end: float, step: int
    ) -> QueryRangeResponse:
        logger.debug(
            'Handling range query',
            extra={'query': query, 'start': start, 'end': end, 'step': step},
        )

        try:
            parsed: ParsedQuery = parser.parse(query)
        except ValueError as e:
            logger.warning(
                'Invalid PromQL query', extra={'query': query, 'error': str(e)}
            )
            raise

        if parsed.func == 'rate':
            series = await timescale_db.fetch_timeseries_rate(
                metric_name=parsed.metric_name,
                labels=parsed.labels,
                start_ts=start,
                end_ts=end,
                step_seconds=step,
            )
        else:
            series = await timescale_db.fetch_timeseries_gauge(
                metric_name=parsed.metric_name,
                labels=parsed.labels,
                start_ts=start,
                end_ts=end,
                step_seconds=step,
            )

        grouped: dict[tuple[tuple[str, Any], ...], list[tuple[float, str]]] = (
            defaultdict(list)
        )
        for _name, attrs, value, ts in series:
            key = tuple(sorted(attrs.items()))
            grouped[key].append((ts.timestamp(), str(value)))

        result_items = []
        for label_key, points in grouped.items():
            labels_dict = dict(label_key)
            labels_dict['__name__'] = parsed.metric_name or 'scalar'
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
    async def get_label_names() -> LabelNamesResponse:
        logger.debug('Fetching all label names')
        labels = await timescale_db.fetch_all_label_names()
        return LabelNamesResponse(data=labels)

    @staticmethod
    async def get_label_values(label_name: str) -> LabelValuesResponse:
        logger.debug('Fetching label values', extra={'label_name': label_name})
        values = await timescale_db.fetch_label_values(label_name)
        return LabelValuesResponse(data=values)

    @staticmethod
    def get_build_info() -> BuildInfoResponse:
        logger.debug('Returning build info')
        return BuildInfoResponse(data=BuildInfoData())

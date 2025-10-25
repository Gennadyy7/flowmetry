from typing import Any

from api.schemas import DBMetric, MetricType


class PrometheusFormatter:
    @staticmethod
    def _escape_label_value(value: Any) -> str:
        return str(value).replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"')

    @classmethod
    def _format_labels(cls, attributes: dict[str, Any]) -> str:
        if not attributes:
            return ''
        labels = [f'{k}="{cls._escape_label_value(v)}"' for k, v in attributes.items()]
        return '{' + ','.join(labels) + '}'

    def _format_simple_metric(self, metric: DBMetric) -> list[str]:
        label_str = self._format_labels(metric.attributes)
        return [
            f'# HELP {metric.name} {metric.description}',
            f'# TYPE {metric.name} {metric.type.value}',
            f'{metric.name}{label_str} {metric.value}',
        ]

    def _format_histogram_metric(self, metric: DBMetric) -> list[str]:
        if (
            metric.bucket_counts is None
            or metric.explicit_bounds is None
            or metric.sum is None
            or metric.count is None
        ):
            return []
        label_str = self._format_labels(metric.attributes)
        lines = [
            f'# HELP {metric.name} {metric.description}',
            f'# TYPE {metric.name} histogram',
        ]
        cumulative = 0
        for bound, count in zip(
            metric.explicit_bounds, metric.bucket_counts, strict=False
        ):
            cumulative += count
            lines.append(
                f'{metric.name}_bucket{label_str} {{le="{bound}"}} {cumulative}'
            )
        lines.append(f'{metric.name}_bucket{label_str} {{le="+Inf"}} {metric.count}')
        lines.append(f'{metric.name}_sum{label_str} {metric.sum}')
        lines.append(f'{metric.name}_count{label_str} {metric.count}')
        return lines

    def format_metrics(self, metrics: list[DBMetric]) -> str:
        lines = []
        for metric in metrics:
            if metric.type in (MetricType.GAUGE, MetricType.COUNTER):
                if metric.value is not None:
                    lines.extend(self._format_simple_metric(metric))
            elif metric.type == MetricType.HISTOGRAM:
                lines.extend(self._format_histogram_metric(metric))
        return '\n'.join(lines) + '\n'

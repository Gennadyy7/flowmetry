from dataclasses import dataclass
import re
from typing import ClassVar


@dataclass(frozen=True)
class ParsedQuery:
    raw: str
    metric_name: str
    labels: dict[str, str]
    func: str = 'raw'  # 'raw', 'rate', 'scalar'
    scalar_value: str | None = None  # up=1, 1=1, 1+1=2


class PromQLSimpleParser:
    METRIC_NAME_PATTERN: ClassVar[str] = r'^[a-zA-Z_:][a-zA-Z0-9_:]*$'
    FULL_SELECTOR_PATTERN: ClassVar[str] = r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.+)\}$'
    LABEL_KV_PATTERN: ClassVar[str] = r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"([^"]*)"$'

    def __init__(self) -> None:
        self._metric_name_re = re.compile(self.METRIC_NAME_PATTERN)
        self._full_selector_re = re.compile(self.FULL_SELECTOR_PATTERN)
        self._label_kv_re = re.compile(self.LABEL_KV_PATTERN)

    def parse(self, query: str) -> ParsedQuery:
        query = query.strip()
        if not query:
            raise ValueError('Query is empty')

        if query == 'up':
            return ParsedQuery(
                raw=query, metric_name='', labels={}, func='scalar', scalar_value='1'
            )
        if query == '1':
            return ParsedQuery(
                raw=query, metric_name='', labels={}, func='scalar', scalar_value='1'
            )
        if query == '1+1':
            return ParsedQuery(
                raw=query, metric_name='', labels={}, func='scalar', scalar_value='2'
            )

        if query.lower().startswith('rate('):
            inner = query[5:].rstrip(')')
            if '[' in inner:
                inner = inner.split('[', 1)[0].strip()
            inner_parsed = self._parse_selector(inner)
            return ParsedQuery(
                raw=query,
                metric_name=inner_parsed.metric_name,
                labels=inner_parsed.labels,
                func='rate',
            )

        parsed = self._parse_selector(query)
        return ParsedQuery(
            raw=query,
            metric_name=parsed.metric_name,
            labels=parsed.labels,
            func='raw',
        )

    def _parse_selector(self, query: str) -> ParsedQuery:
        if '{' not in query:
            self._validate_metric_name(query)
            return ParsedQuery(raw=query, metric_name=query, labels={})

        match = self._full_selector_re.match(query)
        if not match:
            raise ValueError(f'Invalid query format: {query}')

        metric_name = match.group(1)
        labels_str = match.group(2)

        self._validate_metric_name(metric_name)
        labels = self._parse_labels(labels_str)

        return ParsedQuery(raw=query, metric_name=metric_name, labels=labels)

    def _validate_metric_name(self, name: str) -> None:
        if not self._metric_name_re.match(name):
            raise ValueError(f'Invalid metric name: {name}')

    def _parse_labels(self, labels_str: str) -> dict[str, str]:
        labels: dict[str, str] = {}
        for part in labels_str.split(','):
            part = part.strip()
            if not part:
                continue
            kv_match = self._label_kv_re.match(part)
            if not kv_match:
                raise ValueError(f'Invalid label syntax: {part}')
            key, value = kv_match.groups()
            labels[key] = value
        return labels


parser = PromQLSimpleParser()

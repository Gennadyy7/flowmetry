import re
from typing import ClassVar


class PromQLSimpleParser:
    METRIC_NAME_PATTERN: ClassVar[str] = r'^[a-zA-Z_:][a-zA-Z0-9_:]*$'
    FULL_SELECTOR_PATTERN: ClassVar[str] = r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.+)\}$'
    LABEL_KV_PATTERN: ClassVar[str] = r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"([^"]*)"$'

    def __init__(self) -> None:
        self._metric_name_re = re.compile(self.METRIC_NAME_PATTERN)
        self._full_selector_re = re.compile(self.FULL_SELECTOR_PATTERN)
        self._label_kv_re = re.compile(self.LABEL_KV_PATTERN)

    def parse(self, query: str) -> tuple[str, dict[str, str]]:
        query = query.strip()
        if not query:
            raise ValueError('Query is empty')

        if '{' not in query:
            self._validate_metric_name(query)
            return query, {}

        match = self._full_selector_re.match(query)
        if not match:
            raise ValueError(f'Invalid query format: {query}')

        metric_name = match.group(1)
        labels_str = match.group(2)

        self._validate_metric_name(metric_name)
        labels = self._parse_labels(labels_str)

        return metric_name, labels

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

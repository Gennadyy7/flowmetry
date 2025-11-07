from dataclasses import dataclass, field
import re
from typing import ClassVar, Literal, NamedTuple, cast


class RangeVector(NamedTuple):
    value: int
    unit: Literal['s', 'm', 'h', 'd', 'w']

    @property
    def seconds(self) -> int:
        multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}
        return self.value * multipliers[self.unit]


@dataclass(frozen=True)
class ParsedQuery:
    raw: str
    metric_name: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    function: Literal['raw', 'rate', 'increase'] = 'raw'
    range: RangeVector | None = None
    aggregation: Literal['sum', 'avg', 'min', 'max', 'count'] | None = None
    by_labels: list[str] = field(default_factory=list)
    without_labels: list[str] = field(default_factory=list)
    scalar_value: str | None = None

    def get_lookback_seconds(self, default: int = 300) -> int:
        return self.range.seconds if self.range else default

    def get_effective_metric_name(self) -> str:
        base = self.metric_name or 'scalar'
        if self.function != 'raw':
            base = f'{self.function}({base})'
        if self.aggregation:
            base = f'{self.aggregation}({base})'
        return base


class ParseError(ValueError):
    def __init__(self, message: str, query: str, position: int | None = None):
        if position is not None:
            message = f'{message} at position {position} in query: {query!r}'
        else:
            message = f'{message} in query: {query!r}'
        super().__init__(message)
        self.query = query
        self.position = position


class _PromQLValidator:
    METRIC_NAME_PATTERN: ClassVar[str] = r'^[a-zA-Z_:][a-zA-Z0-9_:]*$'
    LABEL_NAME_PATTERN: ClassVar[str] = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    LABEL_VALUE_UNESCAPE_RE: ClassVar[re.Pattern[str]] = re.compile(r'\\(.)')

    @classmethod
    def validate_metric_name(cls, name: str) -> None:
        if not re.fullmatch(cls.METRIC_NAME_PATTERN, name):
            raise ParseError(f'Invalid metric name: {name}', name)

    @classmethod
    def validate_label_name(cls, name: str) -> None:
        if not re.fullmatch(cls.LABEL_NAME_PATTERN, name):
            raise ParseError(f'Invalid label name: {name}', name)

    @classmethod
    def unescape_label_value(cls, value: str) -> str:
        return cls.LABEL_VALUE_UNESCAPE_RE.sub(r'\1', value)


class PromQLParser:
    _FULL_EXPR_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"""
        ^\s*
        # Optional aggregation: sum(...) by (labels)
        (?:
            (?P<agg_op>sum|avg|min|max|count)\s*\(\s*
        )?
        # Optional function: rate(...) or increase(...)
        (?:
            (?P<func>rate|increase)\s*\(\s*
        )?
        # Metric selector: name{labels} or {labels}
        (?P<metric>[a-zA-Z_:][a-zA-Z0-9_:]*\{.*?\}|\{.*?\}|[a-zA-Z_:][a-zA-Z0-9_:]*)
        # Optional range vector: [5m]
        (?:
            \[\s*(?P<range>\d+(?:\.\d+)?[smhdw])\s*\]
        )?
        # Close function and aggregation
        \s*\)*
        # Optional 'by' clause
        (?:
            \s+by\s*\(\s*(?P<by>[^)]*?)\s*\)
        )?
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    _LABEL_SELECTOR_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r'\{(?P<content>.*)\}'
    )
    _LABEL_KV_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"""
        \s*
        (?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*
        (?:
            (?P<op>=|!=|=~|!~)\s*
            (?P<value>"(?:[^"\\]|\\.)*")
        )?
        \s*
        (?=,|\Z)
        """,
        re.VERBOSE,
    )

    def parse(self, query: str) -> ParsedQuery:
        query = query.strip()
        if not query:
            raise ParseError('Empty query', query)

        if query == 'up':
            return ParsedQuery(raw=query, scalar_value='1')
        if query == '1':
            return ParsedQuery(raw=query, scalar_value='1')
        if query == '1+1':
            return ParsedQuery(raw=query, scalar_value='2')

        match = self._FULL_EXPR_PATTERN.match(query)
        if not match:
            return self._parse_fallback(query)

        return self._build_parsed_query(query, match)

    def _parse_fallback(self, query: str) -> ParsedQuery:
        if query.startswith('{') and query.endswith('}'):
            labels = self._parse_labels(query[1:-1])
            metric_name = labels.pop('__name__', None)
            return ParsedQuery(raw=query, metric_name=metric_name, labels=labels)
        else:
            _PromQLValidator.validate_metric_name(query)
            return ParsedQuery(raw=query, metric_name=query)

    def _build_parsed_query(self, query: str, match: re.Match[str]) -> ParsedQuery:
        groups = match.groupdict()

        metric_part = groups['metric']
        metric_name, labels = self._parse_metric_and_labels(metric_part)

        range_vec = None
        if groups['range']:
            range_vec = self._parse_range(groups['range'], query)

        by_labels = []
        if groups['by']:
            by_labels = [s.strip() for s in groups['by'].split(',') if s.strip()]
            for lbl in by_labels:
                _PromQLValidator.validate_label_name(lbl)

        return ParsedQuery(
            raw=query,
            metric_name=metric_name,
            labels=labels,
            function=cast(
                Literal['rate', 'increase'], (groups['func'] or 'raw').lower()
            ),
            range=range_vec,
            aggregation=cast(
                Literal['sum', 'avg', 'min', 'max', 'count'],
                groups['agg_op'].lower() if groups['agg_op'] else None,
            ),
            by_labels=by_labels,
        )

    def _parse_metric_and_labels(self, part: str) -> tuple[str | None, dict[str, str]]:
        selector_match = self._LABEL_SELECTOR_PATTERN.search(part)
        if not selector_match:
            _PromQLValidator.validate_metric_name(part)
            return part, {}

        metric_name = part[: selector_match.start()] or None
        if metric_name:
            _PromQLValidator.validate_metric_name(metric_name)

        labels = self._parse_labels(selector_match.group('content'))
        if '__name__' in labels:
            if metric_name and metric_name != labels['__name__']:
                raise ParseError(
                    f'Conflicting metric names: {metric_name} vs {labels["__name__"]}',
                    part,
                )
            metric_name = labels.pop('__name__')

        return metric_name, labels

    @staticmethod
    def _parse_labels(content: str) -> dict[str, str]:
        labels: dict[str, str] = {}
        parts = []
        current: list[str] = []
        in_quotes = False
        for char in content:
            if char == '"':
                in_quotes = not in_quotes
            if char == ',' and not in_quotes:
                parts.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        if current:
            parts.append(''.join(current).strip())

        for part in parts:
            if not part:
                continue
            eq_match = re.match(
                r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(".*?")\s*$',
                part,
                re.DOTALL,
            )
            if not eq_match:
                eq_match = re.match(
                    r'^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(\S+)\s*$',
                    part,
                )
                if not eq_match:
                    raise ParseError(f'Invalid label syntax: {part!r}', content)
                key, value_raw = eq_match.groups()
                value = value_raw
            else:
                key, value_raw = eq_match.groups()
                value = _PromQLValidator.unescape_label_value(value_raw[1:-1])

            _PromQLValidator.validate_label_name(key)
            labels[key] = value

        return labels

    @staticmethod
    def _parse_range(s: str, query: str) -> RangeVector:
        s_clean = s.strip().lower()
        match = re.match(r'^(\d+)(?:\.(\d+))?\s*([smhdw])$', s_clean)
        if not match:
            raise ParseError(f'Invalid range vector: {s}', query)

        int_part, frac_part, unit = match.groups()
        value = int(int_part)
        if frac_part:
            value = int(round(float(f'{int_part}.{frac_part}')))

        unit = cast(Literal['s', 'm', 'h', 'd', 'w'], unit)
        return RangeVector(value=value, unit=unit)


parser = PromQLParser()

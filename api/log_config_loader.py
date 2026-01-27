from collections.abc import Callable
from functools import partial
import logging
from pathlib import Path
import sys
import time
from typing import Any

import orjson


def _load_default_log_config() -> dict[str, Any]:
    config_path = Path(__file__).parent.parent / 'log_config.json'
    try:
        data = orjson.loads(config_path.read_bytes())
        if not isinstance(data, dict):
            raise RuntimeError(
                f'Expected JSON object in {config_path}, got {type(data).__name__}'
            )
        if 'standard_fields' in data and isinstance(data['standard_fields'], list):
            data['standard_fields'] = set(data['standard_fields'])
        return data
    except FileNotFoundError as e:
        raise RuntimeError(f'Log config file not found: {config_path}') from e
    except orjson.JSONDecodeError as e:
        raise RuntimeError(f'Invalid JSON in log config file {config_path}: {e}') from e


DEFAULT_LOG_CONFIG: dict[str, Any] = _load_default_log_config()


def _serialize_log(record: dict[str, Any]) -> str:
    return orjson.dumps(record).decode('utf-8')


class BaseFormatter(logging.Formatter):
    def __init__(self, service_name: str, version: str, datefmt: str | None = None):
        super().__init__(datefmt=datefmt or DEFAULT_LOG_CONFIG['datefmt'])
        self.service_name = service_name
        self.version = version
        self.standard_fields = DEFAULT_LOG_CONFIG['standard_fields']

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        ct = self.converter(record.created)
        if datefmt:
            return time.strftime(datefmt, ct)
        else:
            s = time.strftime('%d.%m.%Y %H:%M:%S', ct)
            msecs = int(record.msecs)
            return f'{s}.{msecs:03d}'

    def _get_extra(self, record: logging.LogRecord) -> dict[str, Any]:
        return {
            key: value
            for key, value in record.__dict__.items()
            if key not in self.standard_fields and not key.startswith('_')
        }

    def _get_base_record(self, record: logging.LogRecord) -> dict[str, Any]:
        return {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'service': self.service_name,
            'version': self.version,
            'logger': record.name,
            'message': record.getMessage(),
        }


class JsonFormatter(BaseFormatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = self._get_base_record(record)
        log_entry.update(self._get_extra(record))
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        return _serialize_log(log_entry)


class TextFormatter(BaseFormatter):
    def format(self, record: logging.LogRecord) -> str:
        extra_str = ' '.join(f'[{k}={v}]' for k, v in self._get_extra(record).items())
        message = f'{record.getMessage()} {extra_str}'.strip()
        base = f'{self.formatTime(record, self.datefmt)} [{record.levelname:<8}] {record.name}: {message}'
        if record.exc_info:
            base += f'\n{self.formatException(record.exc_info)}'
        return base


def _create_formatter(
    log_format: str, service_name: str, version: str
) -> logging.Formatter:
    formatters: dict[str, Callable[[], BaseFormatter]] = {
        'json': partial(JsonFormatter, service_name=service_name, version=version),
        'text': partial(TextFormatter, service_name=service_name, version=version),
    }
    return formatters.get(log_format.lower(), formatters['text'])()


def setup_logging(
    service_name: str,
    level: str,
    log_format: str,
    version: str,
) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    formatter = _create_formatter(log_format, service_name, version)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    for logger_name in ('uvicorn.access', 'uvicorn.error', 'redis', 'asyncio'):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

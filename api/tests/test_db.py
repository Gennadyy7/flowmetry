import json
from unittest.mock import AsyncMock, patch

import pytest

from api.db import TimescaleDB


class TestTimescaleDB:
    def test_init(self) -> None:
        db = TimescaleDB()
        assert db._pool is None

    async def test_connect_already_connected(self) -> None:
        db = TimescaleDB()
        mock_pool = AsyncMock()
        db._pool = mock_pool

        await db.connect()

        assert db._pool is mock_pool

    async def test_close_not_connected(self) -> None:
        db = TimescaleDB()

        await db.close()

        assert db._pool is None

    async def test_close_connected(self) -> None:
        db = TimescaleDB()
        mock_pool = AsyncMock()
        db._pool = mock_pool

        await db.close()

        mock_pool.close.assert_called_once()
        assert db._pool is None

    def test_parse_attributes_string_valid_json(self) -> None:
        json_str = '{"key": "value", "number": 42}'
        result = TimescaleDB._parse_attributes(json_str)

        assert result == {'key': 'value', 'number': 42}

    def test_parse_attributes_string_invalid_json(self) -> None:
        json_str = '{"invalid": json}'

        with pytest.raises(json.JSONDecodeError):
            TimescaleDB._parse_attributes(json_str)

    def test_parse_attributes_string_not_object(self) -> None:
        json_str = '["not", "an", "object"]'

        with pytest.raises(ValueError, match='Expected JSON object'):
            TimescaleDB._parse_attributes(json_str)

    def test_parse_attributes_dict_like_object(self) -> None:
        class DictLike:
            def keys(self) -> list[str]:
                return ['key1', 'key2']

            def items(self) -> list[tuple[str, object]]:
                return [('key1', 'value1'), ('key2', 42)]

        obj = DictLike()
        result = TimescaleDB._parse_attributes(obj)

        assert result == {'key1': 'value1', 'key2': 42}

    def test_parse_attributes_unsupported_type(self) -> None:
        with pytest.raises(ValueError, match='Unsupported attributes type'):
            TimescaleDB._parse_attributes(123)

    async def test_get_connection_not_connected(self) -> None:
        db = TimescaleDB()

        with pytest.raises(RuntimeError, match='TimescaleDB not connected'):
            async with db._get_connection():
                pass

    async def test_fetch_series_with_matchers(self) -> None:
        db = TimescaleDB()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {'name': 'metric1', 'attributes': '{"label": "value1"}'},
            {'name': 'metric2', 'attributes': '{}'},
        ]

        with patch.object(db, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn

            result = await db.fetch_series(['metric1', 'metric2'])

            assert len(result) == 2
            assert result[0] == {'__name__': 'metric1', 'label': 'value1'}
            assert result[1] == {'__name__': 'metric2'}
            mock_conn.fetch.assert_called_once()

    async def test_fetch_series_without_matchers(self) -> None:
        db = TimescaleDB()
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [{'name': 'metric1', 'attributes': '{}'}]

        with patch.object(db, '_get_connection') as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn

            result = await db.fetch_series()

            assert len(result) == 1
            assert result[0] == {'__name__': 'metric1'}
            mock_conn.fetch.assert_called_once_with(
                'SELECT DISTINCT name, attributes FROM metrics_info'
            )

from unittest.mock import patch

import pytest

from aggregator import log_config_loader


class TestLogConfigLoader:
    def test_load_default_log_config_success(self) -> None:
        # Test the actual function with real file
        config = log_config_loader._load_default_log_config()

        assert isinstance(config, dict)
        assert 'standard_fields' in config
        assert isinstance(config['standard_fields'], set)

    def test_load_default_log_config_file_not_found(self) -> None:
        with patch('pathlib.Path.read_bytes', side_effect=FileNotFoundError):
            with pytest.raises(RuntimeError, match='Log config file not found'):
                log_config_loader._load_default_log_config()

    def test_load_default_log_config_not_dict(self) -> None:
        with patch('pathlib.Path.read_bytes', return_value=b'["not", "a", "dict"]'):
            with pytest.raises(RuntimeError, match='Expected JSON object'):
                log_config_loader._load_default_log_config()

    def test_default_log_config_constant(self) -> None:
        # Test that DEFAULT_LOG_CONFIG is loaded
        assert hasattr(log_config_loader, 'DEFAULT_LOG_CONFIG')
        assert isinstance(log_config_loader.DEFAULT_LOG_CONFIG, dict)
        assert 'standard_fields' in log_config_loader.DEFAULT_LOG_CONFIG
        assert isinstance(log_config_loader.DEFAULT_LOG_CONFIG['standard_fields'], set)

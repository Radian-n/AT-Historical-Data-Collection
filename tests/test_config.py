"""Tests for configuration validation."""

import sys
from unittest.mock import patch

import pytest


class TestConfigValidation:
    """Tests for app/config.py module."""

    def test_api_key_required(self, monkeypatch) -> None:
        """Missing AT_API_KEY raises RuntimeError."""
        # Remove the key from environment
        monkeypatch.delenv("AT_API_KEY", raising=False)

        # Remove cached module to force reimport
        if "app.config" in sys.modules:
            del sys.modules["app.config"]

        # Patch load_dotenv to prevent it from loading .env file
        with patch("dotenv.load_dotenv"):
            with pytest.raises(RuntimeError, match="AT_API_KEY"):
                import app.config  # noqa: F401

    def test_api_key_set(self, monkeypatch) -> None:
        """AT_API_KEY is read from environment."""
        monkeypatch.setenv("AT_API_KEY", "test_key_123")

        # Remove cached module to force reimport
        if "app.config" in sys.modules:
            del sys.modules["app.config"]

        import app.config

        assert app.config.AT_API_KEY == "test_key_123"

    def test_data_root_has_default(self, monkeypatch) -> None:
        """DATA_ROOT has a default value."""
        monkeypatch.setenv("AT_API_KEY", "test_key")
        monkeypatch.delenv("DATA_ROOT", raising=False)

        if "app.config" in sys.modules:
            del sys.modules["app.config"]

        import app.config

        assert app.config.DATA_ROOT is not None

    def test_buffer_checkpoint_root_has_default(self, monkeypatch) -> None:
        """BUFFER_CHECKPOINT_ROOT has a default value."""
        monkeypatch.setenv("AT_API_KEY", "test_key")
        monkeypatch.delenv("BUFFER_CHECKPOINT_ROOT", raising=False)

        if "app.config" in sys.modules:
            del sys.modules["app.config"]

        import app.config

        assert app.config.BUFFER_CHECKPOINT_ROOT is not None

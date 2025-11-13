"""Helpers for managing password strength policy across the application."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

DEFAULT_MIN_PASSWORD_LENGTH: int = 8
MIN_ALLOWED_PASSWORD_LENGTH: int = 6
MAX_ALLOWED_PASSWORD_LENGTH: int = 128
ENV_PASSWORD_MIN_LENGTH_KEY: str = "PASSWORD_MIN_LENGTH"
_PASSWORD_LENGTH_SOURCES = ("env", "config", "default")


def _parse_length(value: Union[str, int, float, None]) -> Optional[int]:
    """Convert raw input into a sanitized password length."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        length = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if length < MIN_ALLOWED_PASSWORD_LENGTH:
        length = MIN_ALLOWED_PASSWORD_LENGTH
    if length > MAX_ALLOWED_PASSWORD_LENGTH:
        length = MAX_ALLOWED_PASSWORD_LENGTH
    return length


def _resolve_data_dir() -> Path:
    """Best-effort resolution of the application's data directory."""
    env_dir = os.getenv("MYBIBLIOTHECA_DATA_DIR") or os.getenv("DATA_DIR")
    if env_dir:
        return Path(env_dir)
    try:
        from flask import current_app

        data_dir = current_app.config.get("DATA_DIR")  # type: ignore[attr-defined]
        if data_dir:
            return Path(str(data_dir))
    except Exception:
        pass
    try:
        return Path(__file__).resolve().parents[2] / "data"
    except Exception:
        return Path.cwd() / "data"


def _load_system_config() -> Dict[str, object]:
    """Load the persisted system configuration if present."""
    config_path = _resolve_data_dir() / "system_config.json"
    if not config_path.exists():
        return {}
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def coerce_min_password_length(value: Union[str, int, float, None]) -> Optional[int]:
    """Normalize input into a valid minimum password length."""
    return _parse_length(value)


def get_env_password_min_length() -> Optional[int]:
    """Return the password length defined via environment variable, if any."""
    return _parse_length(os.getenv(ENV_PASSWORD_MIN_LENGTH_KEY))


def get_persisted_password_min_length() -> Optional[int]:
    """Return the password length saved in system configuration (if present)."""
    config = _load_system_config()
    security_settings = config.get("security_settings")
    if isinstance(security_settings, dict):
        return _parse_length(security_settings.get("min_password_length"))
    return None


def resolve_min_password_length(include_source: bool = False) -> Union[int, Tuple[int, str]]:
    """Resolve the active minimum password length with precedence: env > config > default."""
    env_value = get_env_password_min_length()
    if env_value is not None:
        return (env_value, _PASSWORD_LENGTH_SOURCES[0]) if include_source else env_value

    persisted_value = get_persisted_password_min_length()
    if persisted_value is not None:
        return (persisted_value, _PASSWORD_LENGTH_SOURCES[1]) if include_source else persisted_value

    return (DEFAULT_MIN_PASSWORD_LENGTH, _PASSWORD_LENGTH_SOURCES[2]) if include_source else DEFAULT_MIN_PASSWORD_LENGTH


def get_password_requirements() -> List[str]:
    """Return a human-readable list of password requirements."""
    min_length = resolve_min_password_length()
    return [
        f"At least {min_length} characters long",
        "Contains at least one letter (A-Z or a-z)",
        "Contains at least one number (0-9) OR one special character (!@#$%^&*()_+-=[]{};':\"\\|,.<>/?)",
        "Not a commonly used password"
    ]
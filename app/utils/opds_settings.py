"""Helpers for reading and writing OPDS sync configuration."""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from flask import current_app


DEFAULTS: Dict[str, Any] = {
    "base_url": "",
    "username": "",
    "password": "",
    "user_agent": "",
    "mapping": {},
    "last_probe_summary": None,
    "last_sync_summary": None,
    "last_field_inventory": {},
    "last_sync_at": None,
    "last_sync_status": None,
    "auto_sync_enabled": False,
    "auto_sync_every_hours": 24,
    "auto_sync_user_id": "",
    "last_auto_sync": None,
    "last_auto_sync_status": None,
    "last_test_summary": None,
    "last_test_preview": [],
    "last_test_task_id": None,
    "last_test_task_api_url": None,
    "last_test_task_progress_url": None,
    "last_sync_task_id": None,
    "last_sync_task_api_url": None,
    "last_sync_task_progress_url": None,
}


def _settings_path() -> str:
    try:
        data_dir = current_app.config.get("DATA_DIR", "data")  # type: ignore[attr-defined]
    except Exception:
        data_dir = "data"
    return os.path.join(data_dir, "opds_settings.json")


def _normalize_mapping(value: Any) -> Dict[str, str]:
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items() if k is not None and v is not None}
    return {}


def load_opds_settings() -> Dict[str, Any]:
    path = _settings_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    merged = DEFAULTS.copy()
                    for key in DEFAULTS:
                        if key == "mapping":
                            merged[key] = _normalize_mapping(data.get(key))
                        elif key == "last_field_inventory":
                            inv = data.get(key)
                            merged[key] = inv if isinstance(inv, dict) else {}
                        elif key in {"auto_sync_enabled"}:
                            merged[key] = bool(data.get(key))
                        elif key in {"auto_sync_every_hours"}:
                            try:
                                raw_value = data.get(key)
                                merged[key] = max(1, int(raw_value if raw_value not in (None, "") else DEFAULTS[key]))
                            except Exception:
                                merged[key] = DEFAULTS[key]
                        elif key in data:
                            merged[key] = data[key]
                    if isinstance(data.get("last_test_preview"), list):
                        merged["last_test_preview"] = data.get("last_test_preview")  # type: ignore[assignment]
                    else:
                        merged["last_test_preview"] = []
                    merged["password_present"] = bool(merged.get("password"))
                    return merged
    except Exception:
        pass
    merged = DEFAULTS.copy()
    merged["password_present"] = False
    return merged


def save_opds_settings(update: Dict[str, Any]) -> bool:
    path = _settings_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        current = load_opds_settings()
        # Strip helper key added during load to avoid persisting.
        current.pop("password_present", None)

        payload = current.copy()
        for key, value in (update or {}).items():
            if key not in DEFAULTS:
                continue
            if key == "mapping":
                payload[key] = _normalize_mapping(value)
            elif key == "last_field_inventory":
                payload[key] = value if isinstance(value, dict) else {}
            elif key == "auto_sync_enabled":
                payload[key] = bool(value)
            elif key == "auto_sync_every_hours":
                try:
                    payload[key] = max(1, int(value if value not in (None, "") else DEFAULTS[key]))
                except Exception:
                    payload[key] = DEFAULTS[key]
            elif key in {"last_sync_at", "last_sync_status", "auto_sync_user_id", "last_auto_sync", "last_auto_sync_status", "last_test_summary"}:
                payload[key] = value
            elif key == "last_test_preview":
                payload[key] = value if isinstance(value, list) else []
            elif key == "password":
                if value is None:
                    # No change requested.
                    continue
                payload[key] = str(value)
            elif key in {
                "last_test_task_id",
                "last_test_task_api_url",
                "last_test_task_progress_url",
                "last_sync_task_id",
                "last_sync_task_api_url",
                "last_sync_task_progress_url",
            }:
                payload[key] = value
            else:
                payload[key] = value

        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        return True
    except Exception:
        return False


__all__ = ["load_opds_settings", "save_opds_settings"]

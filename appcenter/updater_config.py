from __future__ import annotations

import json
from pathlib import Path

DEFAULT_UPDATE_FEED_URL = '@UPDATE_FEED_URL@'

DEFAULT_SETTINGS = {
    'enabled': True,
    'notifications': True,
    'interval_value': 12,
    'interval_unit': 'hours',
    'update_flatpaks': False,
    'update_feed_url': DEFAULT_UPDATE_FEED_URL,
}

VALID_UNITS = {'hours', 'days', 'weeks'}


def _config_path() -> Path:
    return Path.home() / '.config' / 'dnf-app-center' / 'updater.json'


def load_updater_settings() -> dict:
    path = _config_path()
    settings = dict(DEFAULT_SETTINGS)
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                settings.update(data)
    except Exception:
        pass
    try:
        settings['interval_value'] = max(1, int(settings.get('interval_value', DEFAULT_SETTINGS['interval_value'])))
    except Exception:
        settings['interval_value'] = DEFAULT_SETTINGS['interval_value']
    unit = str(settings.get('interval_unit', DEFAULT_SETTINGS['interval_unit']))
    if unit not in VALID_UNITS:
        unit = DEFAULT_SETTINGS['interval_unit']
    settings['interval_unit'] = unit
    settings['enabled'] = bool(settings.get('enabled', DEFAULT_SETTINGS['enabled']))
    settings['notifications'] = bool(settings.get('notifications', DEFAULT_SETTINGS['notifications']))
    settings['update_flatpaks'] = bool(settings.get('update_flatpaks', DEFAULT_SETTINGS['update_flatpaks']))
    feed_url = str(settings.get('update_feed_url', DEFAULT_SETTINGS['update_feed_url']) or DEFAULT_SETTINGS['update_feed_url']).strip()
    settings['update_feed_url'] = feed_url or DEFAULT_SETTINGS['update_feed_url']
    return settings


def save_updater_settings(settings: dict) -> None:
    current = load_updater_settings()
    current.update(settings or {})
    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(current, indent=2, sort_keys=True), encoding='utf-8')


def updater_interval_seconds(settings: dict | None = None) -> int:
    settings = load_updater_settings() if settings is None else settings
    value = max(1, int(settings.get('interval_value', DEFAULT_SETTINGS['interval_value'])))
    unit = str(settings.get('interval_unit', DEFAULT_SETTINGS['interval_unit']))
    multipliers = {
        'hours': 3600,
        'days': 86400,
        'weeks': 604800,
    }
    return value * multipliers.get(unit, 3600)


def _view_mode_config_path() -> Path:
    return Path.home() / '.config' / 'dnf-app-center' / 'view_modes.json'


def load_view_modes() -> dict:
    """Load view mode preferences per page."""
    path = _view_mode_config_path()
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_view_mode(page_key: str, view_mode: str) -> None:
    """Save view mode preference for a specific page."""
    view_modes = load_view_modes()
    view_modes[page_key] = view_mode
    path = _view_mode_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(view_modes, indent=2, sort_keys=True), encoding='utf-8')


def get_view_mode(page_key: str, default: str = "grid") -> str:
    """Get view mode preference for a specific page."""
    view_modes = load_view_modes()
    return view_modes.get(page_key, default)

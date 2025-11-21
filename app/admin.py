"""
Admin functionality for Bibliotheca multi-user platform
Provides admin-only decorators, middleware, and management functions
"""

import os
from typing import Dict, Iterable
from functools import wraps
from pathlib import Path
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, abort, current_app
from flask_login import login_required, current_user
from .services import user_service, book_service, reading_log_service, rag_vector_service
from .forms import UserProfileForm, AdminPasswordResetForm
from datetime import datetime, timedelta, timezone
import pytz
import os
import re
import logging
from app.rag_config import load_rag_config, save_rag_config, build_rag_config_payload, resolve_rag_db_path
from app.utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager
from app.domain.models import MediaType
from app.utils.password_policy import (
    ENV_PASSWORD_MIN_LENGTH_KEY,
    MAX_ALLOWED_PASSWORD_LENGTH,
    MIN_ALLOWED_PASSWORD_LENGTH,
    coerce_min_password_length,
    get_env_password_min_length,
    resolve_min_password_length,
)
from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
from app.services.audiobookshelf_service import AudiobookShelfClient, get_client_from_settings

admin = Blueprint('admin', __name__, url_prefix='/admin')

_MEDIA_TYPE_VALUES = {mt.value for mt in MediaType}
_FRIENDLY_MEDIA_TYPE_LABELS = {
    MediaType.PHYSICAL.value: 'Physical Book',
    MediaType.EBOOK.value: 'E-book',
    MediaType.AUDIOBOOK.value: 'Audiobook',
    MediaType.KINDLE.value: 'Kindle'
}

DEFAULT_LIBRARY_CHAT_PROMPT = (
    "You are MyBibliotheca's helpful librarian. Answer reader questions using the library's "
    "metadata, recommend relevant books, and include short context about why a title matches."
)

AI_MODEL_CATALOG = {
    'openai': {
        'llm': [
            {'value': 'gpt-4o-mini', 'label': 'GPT-4o Mini (fast, multimodal)'},
            {'value': 'gpt-4o', 'label': 'GPT-4o (general premium)'},
            {'value': 'gpt-4-turbo', 'label': 'GPT-4 Turbo'},
            {'value': 'gpt-4.1-mini', 'label': 'GPT-4.1 Mini'},
        ],
        'embedding': [
            {'value': 'text-embedding-3-large', 'label': 'text-embedding-3-large (3072d)'},
            {'value': 'text-embedding-3-small', 'label': 'text-embedding-3-small (1536d)'},
            {'value': 'text-embedding-ada-002', 'label': 'text-embedding-ada-002 (legacy)'},
        ],
    },
    'ollama': {
        'llm': [
            {'value': 'llama3.2-vision:11b', 'label': 'Llama 3.2 Vision 11B'},
            {'value': 'llama3.1:8b-instruct', 'label': 'Llama 3.1 8B Instruct'},
            {'value': 'phi3.5:latest', 'label': 'Phi 3.5 (reasoning)'},
            {'value': 'qwen2.5:14b', 'label': 'Qwen2.5 14B'},
        ],
        'embedding': [
            {'value': 'nomic-embed-text', 'label': 'nomic-embed-text (local default)'},
            {'value': 'all-minilm:latest', 'label': 'all-MiniLM (384d, multilingual)'},
            {'value': 'snowflake-arctic-embed', 'label': 'Snowflake Arctic Embed'},
        ],
    },
}

AI_REASONING_OPTIONS = [
    {'value': 'none', 'label': 'None (faster)'},
    {'value': 'light', 'label': 'Light'},
    {'value': 'medium', 'label': 'Medium'},
    {'value': 'heavy', 'label': 'Heavy'},
]


def _format_media_type_label(value: str) -> str:
    normalized = (value or '').strip().lower()
    if normalized in _FRIENDLY_MEDIA_TYPE_LABELS:
        return _FRIENDLY_MEDIA_TYPE_LABELS[normalized]
    base = normalized.replace('_', ' ')
    return base.title() if base else ''

def _convert_query_result_to_list(result):
    """Convert SafeKuzuManager query result to legacy list format"""
    try:
        # Try to iterate over the result directly
        if hasattr(result, 'get_next'):
            rows = []
            while result.has_next():
                rows.append(result.get_next())
            return rows
        else:
            return list(result) if result else []
    except Exception:
        # Fallback to empty list if conversion fails
        return []


_SENSITIVE_KEYWORDS = (
    'password',
    'passwd',
    'pwd',
    'secret',
    'token',
    'api_key',
    'apikey',
    'auth_token',
)

_SENSITIVE_REGEXES = [
    re.compile(rf"(?i)({keyword}\s*[=:]\s*)([^\s,;]+)") for keyword in _SENSITIVE_KEYWORDS
]


def _resolve_log_level(level: str) -> int:
    """Map string level names to logging module constants."""
    try:
        return int(level)
    except (TypeError, ValueError):
        normalized = (level or '').upper()
        return getattr(logging, normalized, logging.INFO)


def _sanitize_for_logging(value: str, extra_secrets: Iterable[str] | None = None) -> str:
    """Mask sensitive information like passwords or tokens in log messages."""
    if not isinstance(value, str) or not value:
        return value

    sanitized = value
    for pattern in _SENSITIVE_REGEXES:
        sanitized = pattern.sub(lambda m: f"{m.group(1)}<redacted>", sanitized)

    if extra_secrets:
        for secret in extra_secrets:
            if secret:
                sanitized = sanitized.replace(secret, '<redacted>')

    return sanitized


def _log(level: str, message: str, *args, extra_secrets: Iterable[str] | None = None, **kwargs):
    """Central logging helper that sanitizes sensitive details before output."""
    logger = current_app.logger
    sanitized_message = _sanitize_for_logging(message, extra_secrets)
    sanitized_args = tuple(
        _sanitize_for_logging(arg, extra_secrets) if isinstance(arg, str) else arg
        for arg in args
    )
    log_method = getattr(logger, level, None)
    if log_method is None:
        raise AttributeError(f"Logger has no level '{level}'")
    return log_method(sanitized_message, *sanitized_args, **kwargs)


def _log_force(level: str, message: str, *args, extra_secrets: Iterable[str] | None = None, **kwargs):
    """Log a message ensuring it meets or exceeds the current logger threshold."""
    logger = current_app.logger
    numeric_level = _resolve_log_level(level)
    effective_level = max(logger.level, numeric_level)
    sanitized_message = _sanitize_for_logging(message, extra_secrets)
    sanitized_args = tuple(
        _sanitize_for_logging(arg, extra_secrets) if isinstance(arg, str) else arg
        for arg in args
    )
    logger.log(effective_level, sanitized_message, *sanitized_args, **kwargs)

def _get_root_env_path() -> str:
    """Resolve the project root .env path regardless of CWD or Docker paths."""
    try:
        # current_app.root_path points to .../app; parent is project root
        root_dir = Path(current_app.root_path).parent
    except Exception:
        # Fallback to this file's parent directory's parent
        root_dir = Path(__file__).resolve().parents[1]
    return str(root_dir / '.env')




def load_ai_config():
    """Load AI configuration combining persisted JSON override and .env, with caching."""
    import json, time
    cache_key = '_cached_ai_config'
    cache_ts_key = '_cached_ai_config_ts'
    try:
        # Quick in-process cache (5s) to avoid re-reading files on rapid requests
        if cache_key in current_app.config and cache_ts_key in current_app.config:
            if (time.time() - current_app.config[cache_ts_key]) < 5:
                return current_app.config[cache_key]
    except Exception:
        pass

    env_path = _get_root_env_path()
    config: Dict[str,str] = {}

    # 1. Load .env base values
    if os.path.exists(env_path):
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except Exception as e:
            try:
                _log('error', f"Error loading AI config from .env: {e}")
            except Exception:
                pass

    # 2. Overlay runtime-persisted JSON (data/ai_config.json) if present
    try:
        data_dir = current_app.config.get('DATA_DIR', 'data')
    except Exception:
        data_dir = 'data'
    ai_json_path = os.path.join(data_dir, 'ai_config.json')
    if os.path.exists(ai_json_path):
        try:
            with open(ai_json_path, 'r') as jf:
                json_data = json.load(jf)
            # Overlay keys explicitly stored
            for k,v in json_data.items():
                if isinstance(v, (str,int,float)):
                    config[k] = str(v)
        except Exception as e:
            try:
                _log('warning', f"Failed reading ai_config.json: {e}")
            except Exception:
                pass

    # 2b. Backward-compatibility: also overlay legacy data/ai_settings.json if present
    #     This allows older setups to keep using ai_settings.json without losing values.
    try:
        ai_legacy_path = os.path.join(data_dir, 'ai_settings.json')
        if os.path.exists(ai_legacy_path):
            with open(ai_legacy_path, 'r') as jf2:
                legacy_data = json.load(jf2)
            if isinstance(legacy_data, dict):
                for k, v in legacy_data.items():
                    if isinstance(v, (str, int, float)):
                        config[k] = str(v)
    except Exception as le:
        try:
            _log('warning', f"Failed reading ai_settings.json: {le}")
        except Exception:
            pass

    # 3. Apply defaults for any unset keys
    defaults = {
        'OPENAI_API_KEY': '',
        'OPENAI_BASE_URL': 'https://api.openai.com/v1',
        'OPENAI_MODEL': 'gpt-4o',
        'OLLAMA_BASE_URL': 'http://localhost:11434',
        'OLLAMA_MODEL': 'llama3.2-vision:11b',
        'AI_PROVIDER': 'openai',
        'AI_TIMEOUT': '30',
        'AI_MAX_TOKENS': '1000',
        'AI_TEMPERATURE': '0.1',
        'AI_REASONING_MODE': 'none',
        'AI_BOOK_EXTRACTION_ENABLED': 'false',
        'AI_BOOK_EXTRACTION_AUTO_SEARCH': 'true',
        'AI_VISION_PROVIDER': '',
        'AI_VISION_MODEL': '',
        'AI_VISION_MAX_TOKENS': '',
        'AI_VISION_TEMPERATURE': '',
        'AI_VISION_REASONING': '',
        'AI_GENRE_PROVIDER': '',
        'AI_GENRE_MODEL': '',
        'AI_GENRE_MAX_TOKENS': '',
        'AI_GENRE_TEMPERATURE': '',
        'AI_GENRE_REASONING': '',
        'AI_CHAT_PROVIDER': '',
        'AI_CHAT_MODEL': '',
        'AI_CHAT_MAX_TOKENS': '',
        'AI_CHAT_TEMPERATURE': '',
        'AI_CHAT_REASONING': '',
        'AI_CHAT_SYSTEM_PROMPT': DEFAULT_LIBRARY_CHAT_PROMPT,
    }
    for key, default_value in defaults.items():
        if key not in config or config[key] == '':
            config[key] = default_value

    # Backfill role-specific values from global defaults when unset
    primary_provider = config.get('AI_PROVIDER', 'openai')
    openai_model = config.get('OPENAI_MODEL', 'gpt-4o')
    ollama_model = config.get('OLLAMA_MODEL', 'llama3.2-vision:11b')

    if not config.get('AI_VISION_PROVIDER'):
        config['AI_VISION_PROVIDER'] = primary_provider
    if not config.get('AI_VISION_MODEL'):
        config['AI_VISION_MODEL'] = openai_model if config['AI_VISION_PROVIDER'] == 'openai' else ollama_model

    if not config.get('AI_GENRE_PROVIDER'):
        config['AI_GENRE_PROVIDER'] = primary_provider
    if not config.get('AI_GENRE_MODEL'):
        config['AI_GENRE_MODEL'] = openai_model if config['AI_GENRE_PROVIDER'] == 'openai' else ollama_model

    if not config.get('AI_CHAT_PROVIDER'):
        config['AI_CHAT_PROVIDER'] = primary_provider
    if not config.get('AI_CHAT_MODEL'):
        config['AI_CHAT_MODEL'] = openai_model if config['AI_CHAT_PROVIDER'] == 'openai' else ollama_model
    if not config.get('AI_CHAT_SYSTEM_PROMPT'):
        config['AI_CHAT_SYSTEM_PROMPT'] = DEFAULT_LIBRARY_CHAT_PROMPT

    # Ensure scoped tuning fields inherit global defaults when unset
    scoped_defaults = ('VISION', 'GENRE', 'CHAT')
    for scope in scoped_defaults:
        max_key = f'AI_{scope}_MAX_TOKENS'
        temp_key = f'AI_{scope}_TEMPERATURE'
        reason_key = f'AI_{scope}_REASONING'
        if not str(config.get(max_key, '')).strip():
            config[max_key] = config.get('AI_MAX_TOKENS', '1000')
        if not str(config.get(temp_key, '')).strip():
            config[temp_key] = config.get('AI_TEMPERATURE', '0.1')
        if not str(config.get(reason_key, '')).strip():
            config[reason_key] = config.get('AI_REASONING_MODE', 'none')

    # Cache result
    try:
        current_app.config[cache_key] = config
        current_app.config[cache_ts_key] = time.time()
    except Exception:
        pass
    return config

def save_system_config(config):
    """Save system configuration to config file in data directory"""
    import json
    
    try:
        # Get data directory from current app config or default
        try:
            data_dir = current_app.config.get('DATA_DIR', 'data')
        except RuntimeError:
            # Working outside of application context, use default
            data_dir = 'data'
        
        config_path = os.path.join(data_dir, 'system_config.json')
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # Load existing config
        existing_config = {}
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    existing_config = json.load(f)
            except (json.JSONDecodeError, Exception):
                # If file is corrupted, start fresh
                existing_config = {}
        
        # Update with new system settings
        existing_config['site_name'] = config.get('site_name', 'MyBibliotheca')
        existing_config['server_timezone'] = config.get('server_timezone', 'UTC')
        existing_config['terminology_preference'] = config.get('terminology_preference', 'genre')
        
    # Update background configuration if provided
        if 'background_config' in config:
            existing_config['background_config'] = config['background_config']
        # Optional: reading log defaults
        if 'reading_log_defaults' in config:
            # Normalize to simple ints or None
            rld = config.get('reading_log_defaults') or {}
            try:
                dp = rld.get('default_pages_per_log')
                dm = rld.get('default_minutes_per_log')
                dp_i = int(dp) if dp not in (None, '',) else None
                dm_i = int(dm) if dm not in (None, '',) else None
            except Exception:
                dp_i = rld.get('default_pages_per_log') if isinstance(rld.get('default_pages_per_log'), int) else None
                dm_i = rld.get('default_minutes_per_log') if isinstance(rld.get('default_minutes_per_log'), int) else None
            existing_config['reading_log_defaults'] = {
                'default_pages_per_log': dp_i,
                'default_minutes_per_log': dm_i
            }
        # Optional: library defaults
        if 'library_defaults' in config:
            lib = config.get('library_defaults') or {}
            existing_library_defaults = existing_config.get('library_defaults', {}).copy()
            try:
                dr = lib.get('default_rows_per_page')
                dr_i = int(dr) if dr not in (None, '',) else None
            except Exception:
                dr_i = lib.get('default_rows_per_page') if isinstance(lib.get('default_rows_per_page'), int) else None
            existing_library_defaults['default_rows_per_page'] = dr_i

            raw_format = lib.get('default_book_format')
            candidate = (raw_format or '').strip().lower() if isinstance(raw_format, str) else ''
            if candidate not in _MEDIA_TYPE_VALUES:
                candidate = MediaType.PHYSICAL.value
            existing_library_defaults['default_book_format'] = candidate
            existing_config['library_defaults'] = existing_library_defaults
        if 'import_settings' in config:
            existing_import_settings = existing_config.get('import_settings', {}).copy()
            import_settings = config.get('import_settings') or {}
            metadata_concurrency_val = import_settings.get('metadata_concurrency')
            try:
                if metadata_concurrency_val in (None, '',):
                    metadata_concurrency_int = None
                else:
                    metadata_concurrency_int = int(metadata_concurrency_val)
                    if metadata_concurrency_int < 1:
                        metadata_concurrency_int = 1
            except Exception:
                metadata_concurrency_int = None
            if metadata_concurrency_int is None:
                existing_import_settings.pop('metadata_concurrency', None)
            else:
                existing_import_settings['metadata_concurrency'] = metadata_concurrency_int
            if existing_import_settings:
                existing_config['import_settings'] = existing_import_settings
            elif 'import_settings' in existing_config:
                existing_config.pop('import_settings', None)
        if 'security_settings' in config:
            existing_security_settings = existing_config.get('security_settings', {})
            if not isinstance(existing_security_settings, dict):
                existing_security_settings = {}
            security_settings = config.get('security_settings') or {}
            min_length_value = coerce_min_password_length(security_settings.get('min_password_length'))
            if min_length_value is not None:
                existing_security_settings['min_password_length'] = min_length_value
            elif 'min_password_length' in existing_security_settings:
                existing_security_settings.pop('min_password_length', None)
            if existing_security_settings:
                existing_config['security_settings'] = existing_security_settings
            elif 'security_settings' in existing_config:
                existing_config.pop('security_settings', None)
        library_defaults = existing_config.get('library_defaults', {})
        if not isinstance(library_defaults, dict):
            library_defaults = {}
        if library_defaults.get('default_book_format') not in _MEDIA_TYPE_VALUES:
            library_defaults['default_book_format'] = MediaType.PHYSICAL.value
        existing_config['library_defaults'] = library_defaults
        existing_config['last_updated'] = datetime.now().isoformat()
        
        # Save updated config
        with open(config_path, 'w') as f:
            json.dump(existing_config, f, indent=2)
        
        return True
    except Exception as e:
        # Log error if we have current_app context, otherwise print
        try:
            _log('error', f"Error saving system config: {e}")
        except RuntimeError:
            print(f"Error saving system config: {e}")
        return False

def load_system_config():
    """Load system configuration from config file in data directory"""
    import json
    
    try:
        # Get data directory from current app config or default
        try:
            data_dir = current_app.config.get('DATA_DIR', 'data')
        except RuntimeError:
            # Working outside of application context, use default
            data_dir = 'data'
        
        config_path = os.path.join(data_dir, 'system_config.json')
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        # Log warning if we have current_app context, otherwise print
        try:
            _log('warning', f"Error loading system config: {e}")
        except RuntimeError:
            print(f"Error loading system config: {e}")
    
    # Return defaults if file doesn't exist or is corrupted
    default_min_length = resolve_min_password_length()
    if isinstance(default_min_length, tuple):
        default_min_length = default_min_length[0]
    return {
        'site_name': 'MyBibliotheca',
        'server_timezone': 'UTC',
        'terminology_preference': 'genre',
        'background_config': {
            'type': 'default',
            'solid_color': '#667eea',
            'gradient_start': '#667eea',
            'gradient_end': '#764ba2',
            'gradient_direction': '135deg',
            'image_url': '',
            'image_position': 'cover'
        },
        'reading_log_defaults': {
            'default_pages_per_log': None,
            'default_minutes_per_log': None
        },
        'library_defaults': {
            'default_rows_per_page': None,
            'default_book_format': MediaType.PHYSICAL.value
        },
        'import_settings': {
            'metadata_concurrency': None
        },
        'security_settings': {
            'min_password_length': default_min_length
        }
    }

def save_smtp_config(config):
    """Save SMTP configuration to system config JSON"""
    import json
    
    try:
        # Get data directory from current app config or default
        try:
            data_dir = current_app.config.get('DATA_DIR', 'data')
        except RuntimeError:
            data_dir = 'data'
        
        config_path = os.path.join(data_dir, 'system_config.json')
        
        # Load existing config
        existing_config = {}
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    existing_config = json.load(f)
            except (json.JSONDecodeError, Exception):
                existing_config = {}
        
        # Get site name for default from_name
        site_name = existing_config.get('site_name', 'MyBibliotheca')
        default_from_name = f"{site_name} (MyBibliotheca)" if site_name != 'MyBibliotheca' else 'MyBibliotheca'

        allowed_security_values = {'starttls', 'ssl', 'none'}
        raw_security = str(config.get('smtp_security', '')).lower()
        if raw_security not in allowed_security_values:
            legacy_tls = config.get('smtp_use_tls', True)
            legacy_str = str(legacy_tls).lower() if isinstance(legacy_tls, (str, bool)) else ''
            raw_security = 'starttls' if legacy_tls is True or legacy_str in {'true', '1', 'on', 'yes'} else 'none'
        use_tls = raw_security == 'starttls'

        try:
            smtp_port = int(config.get('smtp_port', 587))
        except (TypeError, ValueError):
            smtp_port = 587

        # Update SMTP settings
        existing_config['smtp_config'] = {
            'smtp_server': config.get('smtp_server', ''),
            'smtp_port': smtp_port,
            'smtp_username': config.get('smtp_username', ''),
            'smtp_password': config.get('smtp_password', ''),
            'smtp_use_tls': use_tls,
            'smtp_security': raw_security,
            'smtp_from_email': config.get('smtp_from_email', ''),
            'smtp_from_name': config.get('smtp_from_name', default_from_name)
        }
        existing_config['last_updated'] = datetime.now().isoformat()
        
        # Save updated config
        with open(config_path, 'w') as f:
            json.dump(existing_config, f, indent=2)
        
        return True
    except Exception as e:
        try:
            _log('error', f"Error saving SMTP config: {e}")
        except RuntimeError:
            print(f"Error saving SMTP config: {e}")
        return False

def save_backup_config(config):
    """Save backup configuration to system config JSON"""
    import json
    
    try:
        # Get data directory from current app config or default
        try:
            data_dir = current_app.config.get('DATA_DIR', 'data')
        except RuntimeError:
            data_dir = 'data'
        
        config_path = os.path.join(data_dir, 'system_config.json')
        
        # Load existing config
        existing_config = {}
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    existing_config = json.load(f)
            except (json.JSONDecodeError, Exception):
                existing_config = {}
        
        # Update backup settings
        existing_config['backup_config'] = {
            'backup_directory': config.get('backup_directory', 'data/backups')
        }
        existing_config['last_updated'] = datetime.now().isoformat()
        
        # Save updated config
        with open(config_path, 'w') as f:
            json.dump(existing_config, f, indent=2)
        
        return True
    except Exception as e:
        try:
            _log('error', f"Error saving backup config: {e}")
        except RuntimeError:
            print(f"Error saving backup config: {e}")
        return False

def load_backup_config():
    """Load backup configuration from system config JSON"""
    system_config = load_system_config()
    return system_config.get('backup_config', {
        'backup_directory': 'data/backups'
    })

def load_smtp_config():
    """Load SMTP configuration from system config JSON"""
    system_config = load_system_config()
    site_name = system_config.get('site_name', 'MyBibliotheca')
    # Default from_name is site_name + (MyBibliotheca) unless user has customized it
    default_from_name = f"{site_name} (MyBibliotheca)" if site_name != 'MyBibliotheca' else 'MyBibliotheca'
    
    smtp_config = system_config.get('smtp_config', {})
    # If smtp_config doesn't have from_name set, use the computed default
    if not smtp_config.get('smtp_from_name'):
        smtp_config['smtp_from_name'] = default_from_name

    allowed_security_values = {'starttls', 'ssl', 'none'}
    raw_security = str(smtp_config.get('smtp_security', '')).lower()
    if raw_security not in allowed_security_values:
        legacy_tls = smtp_config.get('smtp_use_tls', True)
        legacy_str = str(legacy_tls).lower() if isinstance(legacy_tls, (str, bool)) else ''
        raw_security = 'starttls' if legacy_tls is True or legacy_str in {'true', '1', 'on', 'yes'} else 'none'
    use_tls = raw_security == 'starttls'
    
    return {
        'smtp_server': smtp_config.get('smtp_server', ''),
        'smtp_port': smtp_config.get('smtp_port', 587),
        'smtp_username': smtp_config.get('smtp_username', ''),
        'smtp_password': smtp_config.get('smtp_password', ''),
        'smtp_use_tls': use_tls,
        'smtp_security': raw_security,
        'smtp_from_email': smtp_config.get('smtp_from_email', ''),
        'smtp_from_name': smtp_config.get('smtp_from_name', default_from_name)
    }

def save_ai_config(config):
    """Safely update AI configuration keys: write to .env and persist overrides JSON."""
    env_path = _get_root_env_path()
    ai_keys = [
        'OPENAI_API_KEY', 'OPENAI_BASE_URL', 'OPENAI_MODEL',
        'OLLAMA_BASE_URL', 'OLLAMA_MODEL',
        'AI_PROVIDER', 'AI_TIMEOUT', 'AI_MAX_TOKENS', 'AI_TEMPERATURE', 'AI_REASONING_MODE',
        'AI_BOOK_EXTRACTION_ENABLED', 'AI_BOOK_EXTRACTION_AUTO_SEARCH',
        'AI_VISION_PROVIDER', 'AI_VISION_MODEL', 'AI_VISION_MAX_TOKENS', 'AI_VISION_TEMPERATURE', 'AI_VISION_REASONING',
        'AI_GENRE_PROVIDER', 'AI_GENRE_MODEL', 'AI_GENRE_MAX_TOKENS', 'AI_GENRE_TEMPERATURE', 'AI_GENRE_REASONING',
        'AI_CHAT_PROVIDER', 'AI_CHAT_MODEL', 'AI_CHAT_MAX_TOKENS', 'AI_CHAT_TEMPERATURE', 'AI_CHAT_REASONING', 'AI_CHAT_SYSTEM_PROMPT'
    ]
    # If user provided any provider/model key and did not explicitly set extraction flag, auto-enable extraction
    try:
        if 'AI_BOOK_EXTRACTION_ENABLED' not in config or str(config.get('AI_BOOK_EXTRACTION_ENABLED')).strip() == '':
            provider_keys = ['OPENAI_API_KEY','OLLAMA_MODEL','OPENAI_MODEL']
            if any(str(config.get(k,'')).strip() for k in provider_keys):
                # Auto-enable cover lookup / book extraction after initial configuration
                config['AI_BOOK_EXTRACTION_ENABLED'] = 'true'
    except Exception:
        pass
    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(env_path), exist_ok=True)

        existing_lines = []
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                existing_lines = f.readlines()

        # Track which keys we've updated
        updated = {k: False for k in ai_keys}
        new_lines = []

        # Replace existing AI key lines in-place, preserve others verbatim
        for line in existing_lines:
            stripped = line.strip()
            if not stripped or stripped.startswith('#') or '=' not in stripped:
                new_lines.append(line)
                continue
            key, _sep, _val = stripped.partition('=')
            key = key.strip()
            if key in updated:
                value = str(config.get(key, '')).strip()
                new_lines.append(f"{key}={value}\n")
                updated[key] = True
            else:
                new_lines.append(line)

        # Append any missing AI keys at the end with a tiny header if needed
        missing = [k for k, done in updated.items() if not done]
        if missing:
            new_lines.append('\n# AI Configuration (managed by Admin UI)\n')
            for key in missing:
                value = str(config.get(key, '')).strip()
                new_lines.append(f"{key}={value}\n")

        with open(env_path, 'w') as f:
            f.writelines(new_lines)

        # Persist a JSON overlay for runtime (so containerized env without committed .env still remembers UI changes)
        try:
            try:
                data_dir = current_app.config.get('DATA_DIR', 'data')
            except Exception:
                data_dir = 'data'
            os.makedirs(data_dir, exist_ok=True)
            ai_json_path = os.path.join(data_dir, 'ai_config.json')
            # Only store AI keys to keep file minimal
            subset = {k: config.get(k, '') for k in ai_keys}
            import json
            with open(ai_json_path, 'w') as jf:
                json.dump(subset, jf, indent=2)
        except Exception as je:
            try:
                _log('warning', f"Failed writing ai_config.json: {je}")
            except Exception:
                pass

        # Invalidate cache
        try:
            current_app.config.pop('_cached_ai_config', None)
        except Exception:
            pass
        return True
    except Exception as e:
        try:
            _log('error', f"Error saving AI config: {e}")
        except Exception:
            print(f"Error saving AI config: {e}")
        return False

def admin_required(f):
    """
    Decorator to require admin privileges for route access
    Usage: @admin_required
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login', next=request.url))
        
        if not current_user.is_admin:
            flash('Access denied. Admin privileges required.', 'error')
            abort(403)
        
        return f(*args, **kwargs)
    return decorated_function

def admin_or_self_required(user_id_param='user_id'):
    """
    Decorator to require admin privileges OR access to own user data
    Usage: @admin_or_self_required() or @admin_or_self_required('id')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                flash('Please log in to access this page.', 'error')
                return redirect(url_for('auth.login', next=request.url))
            
            # Get the user_id from the route parameters
            target_user_id = kwargs.get(user_id_param)
            if target_user_id is None and request.view_args:
                target_user_id = request.view_args.get(user_id_param)
            
            # Allow if admin or accessing own data
            if current_user.is_admin or str(current_user.id) == str(target_user_id):
                return f(*args, **kwargs)
            
            flash('Access denied. Insufficient privileges.', 'error')
            abort(403)
        
        return decorated_function
    return decorator

def get_system_stats():
    """Get system statistics for dashboard"""
    try:
        user_count = 0
        book_count = 0
        
        # Get user count
        try:
            users = user_service.list_users()
            user_count = len(users)
        except Exception as e:
            _log('error', f"Error getting user count: {e}")
            
        # Get book count
        try:
            # Use SafeKuzuManager to count books
            query = "MATCH (b:Book) RETURN count(b) as count"
            result = get_safe_kuzu_manager().execute_query(query)
            if result and result.has_next():
                row = result.get_next()
                # Kuzu returns a list for the row, so row[0] is the count
                book_count = row[0]
        except Exception as e:
            _log('error', f"Error getting book count: {e}")

        return {
            'user_count': user_count,
            'book_count': book_count,
            'uptime': 'Unknown', # Placeholder
            'version': '1.0.0' # Placeholder
        }
    except Exception as e:
        _log('error', f"Error getting system stats: {e}")
        return {}

@admin.route('/dashboard')
@login_required
@admin_required
def dashboard():
    """Admin dashboard with system overview"""
    try:
        # Get system statistics
        stats = get_system_stats()
        
        # Get recent user registrations (last 30 days) - placeholder for now
        # This would need to be implemented in the user service
        recent_users = []
        
        # Get recent book additions (last 30 days) - placeholder for now
        # This would need to be implemented in the book service  
        recent_books = []
        
        return render_template('admin/dashboard.html', 
                             title='Admin Dashboard',
                             stats=stats,
                             recent_users=recent_users,
                             recent_books=recent_books)
    except Exception as e:
        _log('error', f"Error loading admin dashboard: {e}")
        flash('Error loading dashboard data.', 'danger')
        # Provide default stats structure to prevent template errors
        default_stats = {
            'total_books': 0,
            'total_users': 0,
            'active_users': 0,
            'admin_users': 0,
            'new_users_30d': 0,
            'new_books_30d': 0,
            'total_categories': 0,
            'total_contributors': 0,
            'recent_activity': 0,
            'database_size': 'Unknown',
            'system': {
                'disk_free_gb': 'N/A',
                'disk_total_gb': 'N/A', 
                'disk_percent': 'N/A',
                'memory_percent': 'N/A',
                'memory_available_gb': 'N/A'
            },
            'top_users': []
        }
        return render_template('admin/dashboard.html', 
                             title='Admin Dashboard',
                             stats=default_stats,
                             recent_users=[],
                             recent_books=[])

@admin.route('/users')
@login_required
@admin_required
def users():
    """User management interface"""
    search = request.args.get('search', '', type=str)
    
    try:
        page = request.args.get('page', 1, type=int)
        
        # For now, return basic info - search functionality would need to be implemented
        # in the user service for full functionality
        all_users = user_service.get_all_users_sync() if hasattr(user_service, 'get_all_users_sync') else []
        
        # Simple client-side search filtering
        if search:
            filtered_users = [user for user in all_users 
                            if search.lower() in user.username.lower() or search.lower() in user.email.lower()]
        else:
            filtered_users = all_users
        
        # Simple pagination
        per_page = 20
        start = (page - 1) * per_page
        end = start + per_page
        page_users = filtered_users[start:end]
        
        # Create pagination object simulation
        class PaginationResult:
            def __init__(self, items, page, per_page, total):
                self.items = items
                self.page = page
                self.per_page = per_page
                self.total = total
                self.pages = (total + per_page - 1) // per_page  # Calculate total pages
                self.has_prev = page > 1
                self.has_next = end < total
                self.prev_num = page - 1 if self.has_prev else None
                self.next_num = page + 1 if self.has_next else None
        
        users_paginated = PaginationResult(page_users, page, per_page, len(filtered_users))
        
        return render_template('admin/users.html',
                             title='User Management',
                             users=users_paginated,
                             search=search)
    except Exception as e:
        _log('error', f"Error loading users: {e}")
        flash('Error loading users.', 'danger')
        return render_template('admin/users.html',
                             title='User Management',
                             users=None,
                             search=search)

@admin.route('/users/<string:user_id>')
@login_required
@admin_required
def user_detail(user_id):
    """Individual user management"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Get user statistics from services
        user_books = book_service.get_all_books_with_user_overlay_sync(str(user_id))
        book_count = len(user_books)
        
        reading_count = reading_log_service.get_user_logs_count_sync(user_id) if hasattr(reading_log_service, 'get_user_logs_count_sync') else 0  # type: ignore
        
        # Get more detailed stats
        current_year = datetime.now().year
        books_this_year = len([book for book in user_books 
                              if book.get('created_at') and 
                              isinstance(book.get('created_at'), datetime) and 
                              book['created_at'].year == current_year])
        
        # Reading logs this month would need service implementation
        logs_this_month = 0  # Placeholder
        
        # Get recent activity - limit to recent books
        recent_books = sorted(user_books, 
                            key=lambda x: x.get('created_at') or datetime.min, 
                            reverse=True)[:5]
        recent_logs = []  # Would need service implementation
        
        return render_template('admin/user_detail.html',
                             title=f'User: {user.username}',
                             user=user,
                             book_count=book_count,
                             reading_count=reading_count,
                             books_this_year=books_this_year,
                             logs_this_month=logs_this_month,
                             recent_books=recent_books,
                             recent_logs=recent_logs)
    except Exception as e:
        _log('error', f"Error loading user detail {user_id}: {e}")
        flash('Error loading user details.', 'danger')
        return redirect(url_for('admin.users'))

@admin.route('/users/<string:user_id>/toggle_admin', methods=['POST'])
@login_required
@admin_required
def toggle_admin(user_id):
    """Toggle admin status for a user"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Prevent removing admin from the last admin
        if user.is_admin:
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
            if admin_count <= 1:
                flash('Cannot remove admin privileges from the last admin user.', 'error')
                return redirect(url_for('admin.user_detail', user_id=user_id))
        
        # Toggle admin status
        user.is_admin = not user.is_admin
        user_service.update_user_sync(user)
        
        action = 'granted' if user.is_admin else 'removed'
        flash(f'Admin privileges {action} for user {user.username}.', 'success')
        
        return redirect(url_for('admin.user_detail', user_id=user_id))
    except Exception as e:
        _log('error', f"Error toggling admin status for user {user_id}: {e}")
        flash('Error updating user privileges.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<string:user_id>/toggle_active', methods=['POST'])
@login_required
@admin_required
def toggle_active(user_id):
    """Toggle active status for a user"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Prevent deactivating the current admin
        if user.id == current_user.id:
            flash('Cannot deactivate your own account.', 'error')
            return redirect(url_for('admin.user_detail', user_id=user_id))
        
        # Toggle active status
        user.is_active = not user.is_active
        user_service.update_user_sync(user)
        
        status = 'activated' if user.is_active else 'deactivated'
        flash(f'User {user.username} has been {status}.', 'success')
        
        return redirect(url_for('admin.user_detail', user_id=user_id))
    except Exception as e:
        _log('error', f"Error toggling active status for user {user_id}: {e}")
        flash('Error updating user status.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/users/<string:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete a user and handle their data"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Prevent deleting own account
        if user.id == current_user.id:
            flash('Cannot delete your own account.', 'error')
            return redirect(url_for('admin.user_detail', user_id=user_id))
        
        # Prevent deleting the last admin
        if user.is_admin:
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
            if admin_count <= 1:
                flash('Cannot delete the last admin user.', 'error')
                return redirect(url_for('admin.user_detail', user_id=user_id))
        
        username = user.username
        # Perform actual delete now that repository supports it
        deleted = False
        try:
            if hasattr(user_service, 'delete_user_sync'):
                deleted = user_service.delete_user_sync(user_id)  # type: ignore
        except Exception as de:
            _log('error', f"Admin delete exception for user {user_id}: {de}")
            deleted = False
        if deleted:
            flash(f'User {username} deleted.', 'success')
        else:
            flash(f'Failed to delete user {username}.', 'error')
        return redirect(url_for('admin.users'))
    except Exception as e:
        _log('error', f"Error deleting user {user_id}: {e}")
        flash('Error deleting user.', 'danger')
        return redirect(url_for('admin.user_detail', user_id=user_id))

@admin.route('/settings', methods=['GET', 'POST'])
@login_required 
@admin_required
def settings():
    """Admin settings page"""
    safe_manager = get_safe_kuzu_manager()
    existing_config = load_system_config()
    
    if request.method == 'POST':
        site_name = request.form.get('site_name', 'MyBibliotheca')
        server_timezone = request.form.get('server_timezone', 'UTC')
        terminology_preference = request.form.get('terminology_preference', 'genre')
        
        # Handle background configuration
        background_config = {
            'type': request.form.get('background_type', 'default'),
            'solid_color': request.form.get('solid_color', '#667eea'),
            'gradient_start': request.form.get('gradient_start', '#667eea'),
            'gradient_end': request.form.get('gradient_end', '#764ba2'),
            'gradient_direction': request.form.get('gradient_direction', '135deg'),
            'image_url': request.form.get('background_image_url', ''),
            'image_position': request.form.get('image_position', 'cover')
        }
        
        # Handle background image upload
        if 'background_image_file' in request.files:
            file = request.files['background_image_file']
            if file and file.filename:
                # Validate file type
                allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                if '.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in allowed_extensions:
                    try:
                        import uuid
                        # Generate unique filename
                        file_extension = file.filename.rsplit('.', 1)[1].lower()
                        unique_filename = f"bg_{uuid.uuid4().hex}.{file_extension}"
                        
                        # Save to uploads/backgrounds directory in data folder
                        data_dir = getattr(current_app.config, 'DATA_DIR', None)
                        if data_dir:
                            upload_dir = os.path.join(data_dir, 'uploads', 'backgrounds')
                        else:
                            # Fallback to data directory relative to app root
                            base_dir = Path(current_app.root_path).parent
                            upload_dir = os.path.join(base_dir, 'data', 'uploads', 'backgrounds')
                        
                        # Ensure directory exists
                        os.makedirs(upload_dir, exist_ok=True)
                        upload_path = os.path.join(upload_dir, unique_filename)
                        file.save(upload_path)
                        
                        # Update background config with uploaded image URL
                        background_config['image_url'] = f"/uploads/backgrounds/{unique_filename}"
                        background_config['type'] = 'image'
                        
                        flash(f'Background image uploaded successfully: {file.filename}', 'success')
                    except Exception as e:
                        _log('error', f"Error uploading background image: {e}")
                        flash('Error uploading background image. Please try again.', 'error')
                else:
                    flash('Invalid file type. Please upload a PNG, JPG, JPEG, GIF, or WebP image.', 'error')
        
        # Reading defaults (optional)
        try:
            dp_raw = (request.form.get('default_pages_per_log') or '').strip()
            dm_raw = (request.form.get('default_minutes_per_log') or '').strip()
        except Exception:
            dp_raw = ''
            dm_raw = ''
        def _to_int_or_none(v: str):
            try:
                return int(v) if v not in (None, '',) else None
            except Exception:
                return None
        reading_log_defaults = {
            'default_pages_per_log': _to_int_or_none(dp_raw),
            'default_minutes_per_log': _to_int_or_none(dm_raw)
        }

        # Metadata/import configuration
        metadata_concurrency_raw = (request.form.get('metadata_concurrency') or '').strip()
        try:
            metadata_concurrency = int(metadata_concurrency_raw)
            if metadata_concurrency < 1:
                metadata_concurrency = 1
        except Exception:
            metadata_concurrency = None if metadata_concurrency_raw == '' else None

        default_rows_value = (request.form.get('default_rows_per_page') or '').strip()
        raw_default_book_format = (request.form.get('default_book_format') or '').strip().lower()
        if raw_default_book_format not in _MEDIA_TYPE_VALUES:
            raw_default_book_format = MediaType.PHYSICAL.value

        # Save system configuration to .env file
        current_security_settings = existing_config.get('security_settings') or {}
        configured_min = coerce_min_password_length(current_security_settings.get('min_password_length'))
        min_password_length_raw = (request.form.get('min_password_length') or '').strip()
        requested_min = coerce_min_password_length(min_password_length_raw)
        if requested_min is None:
            fallback_min = configured_min
            if fallback_min is None:
                resolved_value = resolve_min_password_length(include_source=True)
                fallback_min = resolved_value[0] if isinstance(resolved_value, tuple) else resolved_value
            requested_min = fallback_min

        config = {
            'site_name': site_name,
            'server_timezone': server_timezone,
            'terminology_preference': terminology_preference,
            'background_config': background_config,
            'reading_log_defaults': reading_log_defaults,
            'library_defaults': {
                'default_rows_per_page': default_rows_value or None,
                'default_book_format': raw_default_book_format
            },
            'import_settings': {
                'metadata_concurrency': metadata_concurrency
            },
            'security_settings': {
                'min_password_length': requested_min
            }
        }
        
        if save_system_config(config):
            if 'background_image_file' not in request.files or not request.files['background_image_file'].filename:
                flash('System settings saved successfully! Changes are now active.', 'success')
        else:
            flash('Error saving system settings. Please check permissions and try again.', 'error')
        
        if request.form.get('inline') == '1' or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            # Return updated partial for unified settings
            ctx = get_admin_settings_context()
            return render_template('admin/partials/server_config.html', **ctx)
        # Always bounce to admin settings page for non-inline posts
        return redirect(url_for('admin.settings'))

    # NOTE: api_delete_user route moved to module scope for proper registration.
    
    # Reuse helper to build context for template
    ctx = get_admin_settings_context()
    
    # Get available timezones
    available_timezones = pytz.all_timezones
    common_timezones = [
        'UTC',
        'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific',
        'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Rome',
        'Asia/Tokyo', 'Asia/Shanghai', 'Asia/Kolkata',
        'Australia/Sydney', 'Australia/Melbourne',
        'America/Toronto', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles'
    ]
    
    # Get debug manager
    try:
        from .debug_system import get_debug_manager
        debug_manager = get_debug_manager()
    except Exception as e:
        _log('warning', f"Could not load debug manager: {e}")
        # Create a simple mock debug manager to prevent template errors
        class MockDebugManager:
            def is_debug_enabled(self):
                return False
        debug_manager = MockDebugManager()
    
    # ctx already contains debug_manager; avoid passing duplicate keyword
    return render_template('admin/settings.html', title='Admin Settings', **ctx)

# ---------------- API User Deletion (diagnostic) -----------------
@admin.route('/api/users/<string:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def api_delete_user(user_id):
    """Diagnostic JSON deletion endpoint used by unified settings UI to debug failures.
    Now serves as the primary deletion API (normal paths log at INFO)."""
    try:
        _log('info', f"[USER_DELETE_DEBUG_API] start user_id={user_id}")
        if not user_id:
            return jsonify({'ok': False, 'error': 'missing id'}), 400
        if getattr(current_user, 'id', None) == user_id:
            return jsonify({'ok': False, 'error': 'cannot delete self'}), 400
        # Optional admin password verification (if provided)
        admin_pwd = request.form.get('admin_password') or request.json.get('admin_password') if request.is_json else None  # type: ignore
        if admin_pwd and not current_user.check_password(admin_pwd):  # type: ignore
            return jsonify({'ok': False, 'error': 'admin password invalid'}), 400
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            _log('info', f"[USER_DELETE_DEBUG_API] user not found user_id={user_id}")
            return jsonify({'ok': False, 'error': 'not found'}), 404
        if getattr(user, 'is_admin', False):
            admin_count = user_service.get_admin_count_sync() if hasattr(user_service,'get_admin_count_sync') else 1
            if admin_count <= 1:
                return jsonify({'ok': False, 'error': 'last admin'}), 400
        deleted = False
        try:
            deleted = user_service.delete_user_sync(user_id) if hasattr(user_service,'delete_user_sync') else False
            _log('info', f"[USER_DELETE_DEBUG_API] service.delete returned {deleted}")
        except Exception as de:
            _log('error', f"[USER_DELETE_DEBUG_API] service.delete exception {de}")
        exists_flag = True
        try:
            repo = getattr(user_service, 'user_repo', None)
            if repo and hasattr(repo, 'safe_manager'):
                res = repo.safe_manager.execute_query("MATCH (u:User {id: $uid}) RETURN COUNT(u) as c", {"uid": user_id})
                from app.services.kuzu_service_facade import _convert_query_result_to_list as _cvt
                data = _cvt(res)
                exists_flag = bool(data and int(data[0].get('c',0))>0)
            _log('info', f"[USER_DELETE_DEBUG_API] exists_after={exists_flag}")
        except Exception as ce:
            _log('error', f"[USER_DELETE_DEBUG_API] existence check exception {ce}")
        return jsonify({'ok': deleted and not exists_flag, 'deleted': deleted, 'exists_after': exists_flag})
    except Exception as e:
        _log('error', f"[USER_DELETE_DEBUG_API] fatal error {e}")
        return jsonify({'ok': False, 'error': 'server error'}), 500

def get_admin_settings_context():
    """Helper to assemble context variables for admin settings forms (reused in partial)."""
    system_config = load_system_config()
    current_site_name = system_config.get('site_name', os.getenv('SITE_NAME', 'MyBibliotheca'))
    current_timezone = system_config.get('server_timezone', os.getenv('TIMEZONE', 'UTC'))
    current_terminology = system_config.get('terminology_preference', 'genre')
    raw_library_defaults = system_config.get('library_defaults') or {}
    default_book_format = raw_library_defaults.get('default_book_format')
    if default_book_format not in _MEDIA_TYPE_VALUES:
        default_book_format = MediaType.PHYSICAL.value
    library_defaults = {
        'default_rows_per_page': raw_library_defaults.get('default_rows_per_page'),
        'default_book_format': default_book_format
    }
    security_settings = system_config.get('security_settings') or {}
    configured_min = coerce_min_password_length(security_settings.get('min_password_length'))
    resolved_info = resolve_min_password_length(include_source=True)
    if isinstance(resolved_info, tuple):
        effective_min_length, password_policy_source = resolved_info
    else:
        effective_min_length, password_policy_source = resolved_info, 'default'
    env_override = get_env_password_min_length()
    password_policy_bounds = {
        'min': MIN_ALLOWED_PASSWORD_LENGTH,
        'max': MAX_ALLOWED_PASSWORD_LENGTH
    }
    media_type_options = [
        {
            'value': mt.value,
            'label': _format_media_type_label(mt.value)
        }
        for mt in MediaType
    ]
    media_type_labels = {opt['value']: opt['label'] for opt in media_type_options}
    available_timezones = pytz.all_timezones
    common_timezones = [
        'UTC',
        'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific',
        'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Rome',
        'Asia/Tokyo', 'Asia/Shanghai', 'Asia/Kolkata',
        'Australia/Sydney', 'Australia/Melbourne',
        'America/Toronto', 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles'
    ]
    try:
        from .debug_system import get_debug_manager
        debug_manager = get_debug_manager()
    except Exception:
        class MockDebugManager:
            def is_debug_enabled(self):
                return False
        debug_manager = MockDebugManager()
    rag_settings = load_rag_config()
    rag_abs_path = str(resolve_rag_db_path(rag_settings.get('RAG_DB_PATH')))
    return {
        'site_name': current_site_name,
        'server_timezone': current_timezone,
        'terminology_preference': current_terminology,
        'common_timezones': common_timezones,
        'available_timezones': sorted(available_timezones),
        'ai_config': load_ai_config(),
        'ai_model_catalog': AI_MODEL_CATALOG,
        'ai_reasoning_options': AI_REASONING_OPTIONS,
        'rag_config': rag_settings,
        'rag_db_absolute_path': rag_abs_path,
        'smtp_config': load_smtp_config(),
        'backup_config': load_backup_config(),
        'debug_manager': debug_manager,
        'background_config': system_config.get('background_config', {
            'type': 'default',
            'solid_color': '#667eea',
            'gradient_start': '#667eea',
            'gradient_end': '#764ba2',
            'gradient_direction': '135deg',
            'image_url': '',
            'image_position': 'cover'
        }),
        'reading_log_defaults': system_config.get('reading_log_defaults', {
            'default_pages_per_log': None,
            'default_minutes_per_log': None
        }),
        'library_defaults': library_defaults,
        'media_type_options': media_type_options,
        'media_type_labels': media_type_labels,
        'import_settings': system_config.get('import_settings', {
            'metadata_concurrency': None
        }),
        'security_settings': security_settings,
        'configured_min_password_length': configured_min or effective_min_length,
        'effective_min_password_length': effective_min_length,
        'password_policy_source': password_policy_source,
        'password_policy_env_override': env_override,
        'password_policy_bounds': password_policy_bounds,
        'password_policy_env_key': ENV_PASSWORD_MIN_LENGTH_KEY
    }

@admin.route('/settings/config_partial')
@login_required
@admin_required
def settings_config_partial():
    """Return just the server configuration form for embedding in unified settings page."""
    ctx = get_admin_settings_context()
    return render_template('admin/partials/server_config.html', **ctx)

@admin.route('/settings/smtp_partial')
@login_required
@admin_required
def settings_smtp_partial():
    """Return SMTP configuration form for embedding in unified settings page."""
    ctx = {
        'smtp_config': load_smtp_config(),
        'site_name': load_system_config().get('site_name', 'MyBibliotheca')
    }
    return render_template('admin/partials/smtp_config.html', **ctx)

@admin.route('/settings/backup_partial')
@login_required
@admin_required
def settings_backup_partial():
    """Return backup configuration form for embedding in unified settings page."""
    ctx = {'backup_config': load_backup_config()}
    return render_template('admin/partials/backup_config.html', **ctx)

@admin.route('/test-ollama-connection', methods=['POST'])
@login_required
@admin_required
def test_ollama_connection():
    """Test Ollama connection and return available models"""
    try:
        # Create minimal config for testing
        config = {
            'AI_PROVIDER': 'ollama',
            'OLLAMA_BASE_URL': request.form.get('ollama_base_url', 'http://localhost:11434'),
            'AI_TIMEOUT': '10'  # Short timeout for testing
        }
        
        # Test connection using AI service
        from app.services.ai_service import AIService
        ai_service = AIService(config, task_scope='vision')
        result = ai_service._test_ollama_connection()
        
        return jsonify(result)
            
    except Exception as e:
        _log('error', f"Error testing Ollama connection: {e}")
        return jsonify({'success': False, 'message': 'Ollama connection test failed. Please check your settings.'})

@admin.route('/settings/server_partial/<panel>')
@login_required
@admin_required
def settings_server_partial(panel):
    """Return partial content for server settings panels."""
    ctx = get_admin_settings_context()
    
    # Map panels to templates
    # Note: Some templates might be in admin/partials, others in settings/partials
    template_map = {
        'config': 'settings/partials/server_config.html',
        'users': 'settings/partials/server_users.html',
        'debug': 'settings/partials/server_debug.html',
        'ai': 'settings/partials/server_ai.html',
        'rag': 'settings/partials/server_rag.html',
        'smtp': 'settings/partials/server_smtp.html',
        'backup': 'settings/partials/server_backup.html',
        'opds': 'settings/partials/server_opds.html',
        'audiobookshelf': 'admin/partials/server_audiobookshelf.html',
        'metadata': 'settings/partials/server_metadata.html',
        'repairs': 'settings/partials/server_repairs.html',
        'jobs': 'settings/partials/server_jobs.html',
        'user_edit': 'settings/partials/server_user_edit.html'
    }
    
    if panel == 'audiobookshelf':
        ctx['abs_settings'] = load_abs_settings()
        
    if panel == 'user_edit':
        user_id = request.args.get('user_id')
        if user_id:
            try:
                user = user_service.get_user_by_id_sync(user_id)
                if user:
                    ctx['edit_user'] = user
            except Exception:
                pass

    template = template_map.get(panel)
    if template:
        try:
            return render_template(template, **ctx)
        except Exception as e:
            _log('error', f"Error rendering settings panel {panel}: {e}")
            return f'<div class="alert alert-danger">Error loading panel {panel}: {e}</div>'

    return '', 404

@admin.route('/update-audiobookshelf-settings', methods=['POST'])
@login_required
@admin_required
def update_audiobookshelf_settings():
    """Update Audiobookshelf configuration settings"""
    try:
        config = load_abs_settings()
        
        # Update config from form
        config['enabled'] = 'enabled' in request.form
        config['base_url'] = request.form.get('base_url', '').strip()
        config['api_key'] = request.form.get('api_key', '').strip()
        
        # Handle library IDs (comma-separated or list)
        lib_ids_raw = request.form.get('library_ids', '')
        if lib_ids_raw:
            config['library_ids'] = [lid.strip() for lid in lib_ids_raw.split(',') if lid.strip()]
        else:
            config['library_ids'] = []
            
        config['abs_user_id'] = request.form.get('abs_user_id', '').strip() or None
        config['auto_sync_enabled'] = 'auto_sync_enabled' in request.form
        
        try:
            config['library_sync_every_hours'] = int(request.form.get('library_sync_every_hours', 24))
        except (ValueError, TypeError):
            config['library_sync_every_hours'] = 24
            
        try:
            config['listening_sync_every_hours'] = int(request.form.get('listening_sync_every_hours', 12))
        except (ValueError, TypeError):
            config['listening_sync_every_hours'] = 12
            
        config['debug_listening_sync'] = 'debug_listening_sync' in request.form
        config['enforce_book_first'] = 'enforce_book_first' in request.form
        
        if save_abs_settings(config):
            flash('Audiobookshelf settings saved successfully!', 'success')
        else:
            flash('Error saving Audiobookshelf settings.', 'danger')
            
        # Inline support
        if request.form.get('inline') == '1' or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            ctx = get_admin_settings_context()
            ctx['abs_settings'] = load_abs_settings()
            try:
                return render_template('admin/partials/server_audiobookshelf.html', **ctx)
            except Exception:
                return jsonify({'ok': True, 'message': 'Settings saved'})
            
        return redirect(url_for('admin.settings'))
        
    except Exception as e:
        _log('error', f"Error updating Audiobookshelf settings: {e}")
        flash('Error updating Audiobookshelf settings.', 'danger')
        return redirect(url_for('admin.settings'))

@admin.route('/test-audiobookshelf-connection', methods=['POST'])
@login_required
@admin_required
def test_audiobookshelf_connection():
    """Test Audiobookshelf connection"""
    try:
        # Use form data if provided, otherwise load from settings
        base_url = request.form.get('base_url')
        api_key = request.form.get('api_key')
        
        if base_url:
            client = AudiobookShelfClient(base_url, api_key or '')
        else:
            settings = load_abs_settings()
            client = get_client_from_settings(settings)
            
        if not client:
            return jsonify({'ok': False, 'message': 'Missing configuration'})
            
        result = client.test_connection()
        return jsonify(result)
        
    except Exception as e:
        _log('error', f"Error testing Audiobookshelf connection: {e}")
        return jsonify({'ok': False, 'message': str(e)})

@admin.route('/audiobookshelf-test-sync', methods=['POST'])
@login_required
@admin_required
def audiobookshelf_test_sync():
    """Trigger a test sync for Audiobookshelf"""
    try:
        return jsonify({'ok': True, 'message': 'Test sync triggered (mock)'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)})

@admin.route('/audiobookshelf-listen-test', methods=['POST'])
@login_required
@admin_required
def audiobookshelf_listen_test():
    """Trigger a listening session test sync"""
    try:
        return jsonify({'ok': True, 'message': 'Listening test sync triggered (mock)'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)})

@admin.route('/audiobookshelf-full-sync', methods=['POST'])
@login_required
@admin_required
def audiobookshelf_full_sync():
    """Trigger a full sync"""
    try:
        return jsonify({'ok': True, 'message': 'Full sync triggered (mock)'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)})

@admin.route('/api/stats')
@login_required
@admin_required
def api_stats():
    """API endpoint for dashboard statistics (for auto-refresh)"""
    stats = get_system_stats()
    return jsonify(stats)

@admin.route('/update-ai-settings', methods=['POST'])
@login_required
@admin_required
def update_ai_settings():
    """Update AI configuration settings"""
    try:
        config = {}
        
        # Get form data
        config['AI_PROVIDER'] = request.form.get('ai_provider', 'openai')
        config['OPENAI_API_KEY'] = request.form.get('openai_api_key', '')
        config['OPENAI_BASE_URL'] = request.form.get('openai_base_url', 'https://api.openai.com/v1')
        config['OPENAI_MODEL'] = request.form.get('openai_model', 'gpt-4o')
        config['OLLAMA_BASE_URL'] = request.form.get('ollama_base_url', 'http://localhost:11434')
        
        # Handle Ollama model selection (dropdown vs manual input)
        ollama_model_manual = request.form.get('ollama_model_manual', '').strip()
        ollama_model_select = request.form.get('ollama_model', '').strip()
        if ollama_model_manual:
            config['OLLAMA_MODEL'] = ollama_model_manual
        elif ollama_model_select:
            config['OLLAMA_MODEL'] = ollama_model_select
        else:
            config['OLLAMA_MODEL'] = 'llama3.2-vision:11b'  # fallback
            
        config['AI_TIMEOUT'] = request.form.get('ai_timeout', '30')
        config['AI_MAX_TOKENS'] = request.form.get('ai_max_tokens', '1000')
        config['AI_TEMPERATURE'] = request.form.get('ai_temperature', '0.1')
        config['AI_REASONING_MODE'] = (request.form.get('ai_reasoning_mode') or config.get('AI_REASONING_MODE') or 'none').strip().lower() or 'none'
        config['AI_BOOK_EXTRACTION_ENABLED'] = 'true' if request.form.get('ai_book_extraction_enabled') else 'false'
        config['AI_BOOK_EXTRACTION_AUTO_SEARCH'] = 'true' if request.form.get('ai_book_extraction_auto_search') else 'false'

        def _resolve_role_provider(field_name: str) -> str:
            return (request.form.get(field_name) or config['AI_PROVIDER']).strip().lower() or config['AI_PROVIDER']

        def _resolve_role_model(field_name: str, provider_value: str) -> str:
            candidate = (request.form.get(field_name) or '').strip()
            if candidate:
                return candidate
            return config['OPENAI_MODEL'] if provider_value == 'openai' else config['OLLAMA_MODEL']

        vision_provider = _resolve_role_provider('ai_vision_provider')
        config['AI_VISION_PROVIDER'] = vision_provider
        config['AI_VISION_MODEL'] = _resolve_role_model('ai_vision_model', vision_provider)

        genre_provider = _resolve_role_provider('ai_genre_provider')
        config['AI_GENRE_PROVIDER'] = genre_provider
        config['AI_GENRE_MODEL'] = _resolve_role_model('ai_genre_model', genre_provider)

        chat_provider = _resolve_role_provider('ai_chat_provider')
        config['AI_CHAT_PROVIDER'] = chat_provider
        config['AI_CHAT_MODEL'] = _resolve_role_model('ai_chat_model', chat_provider)
        config['AI_CHAT_SYSTEM_PROMPT'] = (request.form.get('ai_chat_system_prompt') or DEFAULT_LIBRARY_CHAT_PROMPT).strip()

        def _resolve_scope_tuning(scope: str, field: str, fallback_key: str, default_value: str) -> str:
            form_key = f'ai_{scope}_{field}'
            raw = (request.form.get(form_key) or '').strip()
            if raw:
                return raw
            return str(config.get(fallback_key, default_value))

        for scope in ('vision', 'genre', 'chat'):
            upper = scope.upper()
            config[f'AI_{upper}_MAX_TOKENS'] = _resolve_scope_tuning(scope, 'max_tokens', 'AI_MAX_TOKENS', config['AI_MAX_TOKENS'])
            config[f'AI_{upper}_TEMPERATURE'] = _resolve_scope_tuning(scope, 'temperature', 'AI_TEMPERATURE', config['AI_TEMPERATURE'])
            config[f'AI_{upper}_REASONING'] = _resolve_scope_tuning(scope, 'reasoning', 'AI_REASONING_MODE', config['AI_REASONING_MODE'])
        
        # Save configuration
        if save_ai_config(config):
            flash('AI settings saved successfully!', 'success')
        else:
            flash('Error saving AI settings. Please try again.', 'danger')
        
        # Inline (AJAX/unified settings) support
        if request.form.get('inline') == '1' or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            from app.admin import get_admin_settings_context, load_ai_config
            ctx = get_admin_settings_context()
            ctx['ai_config'] = load_ai_config()
            # Return just the AI panel partial so caller can replace content
            return render_template('settings/partials/server_ai.html', **ctx)
        ref = request.referrer or ''
        if '/settings' in ref and '/admin/settings' not in ref:
            return redirect(url_for('auth.settings', section='server', panel='ai'))
        return redirect(url_for('admin.settings', section='ai'))
        
    except Exception as e:
        _log('error', f"Error updating AI settings: {e}")
    flash('Error updating AI settings. Please try again.', 'danger')
    return redirect(url_for('admin.settings'))


@admin.route('/update-rag-settings', methods=['POST'])
@login_required
@admin_required
def update_rag_settings():
    """Update vector database + RAG preferences."""
    try:
        config = build_rag_config_payload(request.form)
        if save_rag_config(config):
            try:
                rag_vector_service.reload_configuration()
            except Exception as svc_err:
                _log('warning', f"RAG service reload failed: {svc_err}")
            flash('Vector search settings saved successfully!', 'success')
        else:
            flash('Error saving vector search settings.', 'danger')
    except Exception as exc:
        _log('error', f"Error updating RAG settings: {exc}")
        flash('Error saving vector search settings.', 'danger')
    return redirect(url_for('admin.settings'))

@admin.route('/update-smtp-settings', methods=['POST'])
@login_required
@admin_required
def update_smtp_settings():
    """Update SMTP configuration settings"""
    try:
        config = {}
        
        # Get form data
        config['smtp_server'] = request.form.get('smtp_server', '').strip()
        raw_port = (request.form.get('smtp_port', '') or '').strip()
        try:
            config['smtp_port'] = int(raw_port or 587)
        except (TypeError, ValueError):
            config['smtp_port'] = 587
        config['smtp_username'] = request.form.get('smtp_username', '').strip()
        config['smtp_password'] = request.form.get('smtp_password', '').strip()
        config['smtp_from_email'] = request.form.get('smtp_from_email', '').strip()
        config['smtp_from_name'] = request.form.get('smtp_from_name', 'MyBibliotheca').strip()
        allowed_security_values = {'starttls', 'ssl', 'none'}
        raw_security = (request.form.get('smtp_security', '') or '').strip().lower()
        if raw_security not in allowed_security_values:
            legacy_tls = request.form.get('smtp_use_tls', '')
            raw_security = 'starttls' if legacy_tls in {'on', 'true', '1'} else 'none'
        config['smtp_security'] = raw_security
        config['smtp_use_tls'] = raw_security == 'starttls'
        
        # Save configuration
        if save_smtp_config(config):
            flash('SMTP settings saved successfully!', 'success')
        else:
            flash('Error saving SMTP settings. Please try again.', 'danger')
        
        # Inline (AJAX/unified settings) support
        if request.form.get('inline') == '1' or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            ctx = get_admin_settings_context()
            ctx['smtp_config'] = load_smtp_config()
            # Return just the SMTP panel partial if it exists, otherwise redirect
            try:
                return render_template('settings/partials/server_smtp.html', **ctx)
            except:
                pass
        
        return redirect(url_for('admin.settings'))
        
    except Exception as e:
        _log('error', f"Error updating SMTP settings: {e}", extra_secrets=[request.form.get('smtp_password', '')])
        flash('Error updating SMTP settings. Please try again.', 'danger')
    
    return redirect(url_for('admin.settings'))

@admin.route('/test-smtp-connection', methods=['POST'])
@login_required
@admin_required
def test_smtp_connection():
    """Test SMTP connection with provided settings"""
    import smtplib
    import socket
    import ssl

    # Get settings from form
    smtp_server = request.form.get('smtp_server', '').strip()
    raw_port = (request.form.get('smtp_port', '') or '').strip()
    smtp_username = request.form.get('smtp_username', '').strip()
    smtp_password = request.form.get('smtp_password', '').strip()
    allowed_security_values = {'starttls', 'ssl', 'none'}
    raw_security = (request.form.get('smtp_security', '') or '').strip().lower()
    if raw_security not in allowed_security_values:
        legacy_tls = (request.form.get('smtp_use_tls', '') or '').strip().lower()
        raw_security = 'starttls' if legacy_tls in {'true', '1', 'on'} else 'none'

    try:
        smtp_port = int(raw_port or 587)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'message': 'SMTP port must be a number.'}), 400

    secret_values = [smtp_password] if smtp_password else []

    # Log the connection attempt (sanitize password) with forced visibility
    _log_force('info', f"[SMTP] Testing connection to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
    _log_force('info', f"[SMTP] Configuration - Security: {raw_security.upper()}, Username: {smtp_username if smtp_username else '(none)'}", extra_secrets=secret_values)
    
    if not smtp_server:
        _log_force('warning', "[SMTP] Test aborted - no server specified", extra_secrets=secret_values)
        return jsonify({'success': False, 'message': 'SMTP server is required'}), 400
    
    # Test connection with detailed logging
    server = None
    try:
        # Step 1: DNS Resolution
        _log_force('info', f"[SMTP] Step 1/4: Resolving DNS for {smtp_server}...", extra_secrets=secret_values)
        try:
            resolved_ip = socket.gethostbyname(smtp_server)
            _log_force('info', f"[SMTP] DNS resolved: {smtp_server} -> {resolved_ip}", extra_secrets=secret_values)
        except socket.gaierror as dns_err:
            _log_force('error', f"[SMTP] DNS resolution failed for {smtp_server}: {dns_err}", extra_secrets=secret_values)
            return jsonify({
                'success': False,
                'message': f'DNS resolution failed for {smtp_server}. Please check the server address.'
            }), 500
        
        # Step 2: Create SMTP connection with longer timeout
        if raw_security == 'ssl':
            _log_force('info', f"[SMTP] Step 2/4: Connecting with implicit SSL to {smtp_server}:{smtp_port} (timeout: 30s)...", extra_secrets=secret_values)
            try:
                context = ssl.create_default_context()
                server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30, context=context)
                server.set_debuglevel(0)
                server.ehlo()
                _log_force('info', "[SMTP] Connection established over SSL", extra_secrets=secret_values)
            except socket.timeout:
                _log_force('error', f"[SMTP] Connection timeout after 30s to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'Connection timeout to {smtp_server}:{smtp_port}. Check firewall settings or try a different port.'
                }), 500
            except ConnectionRefusedError:
                _log_force('error', f"[SMTP] Connection refused by {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'Connection refused by {smtp_server}:{smtp_port}. Server may not be accepting connections.'
                }), 500
            except socket.error as sock_err:
                _log_force('error', f"[SMTP] Socket error connecting to {smtp_server}:{smtp_port}: {sock_err}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'Network error: {sock_err}. Check server address and port.'
                }), 500
        else:
            _log_force('info', f"[SMTP] Step 2/4: Connecting to {smtp_server}:{smtp_port} (timeout: 30s)...", extra_secrets=secret_values)
            try:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
                server.set_debuglevel(0)  # Don't expose sensitive data in debug output
                server.ehlo()
                _log_force('info', f"[SMTP] Connection established to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
            except socket.timeout:
                _log_force('error', f"[SMTP] Connection timeout after 30s to {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'Connection timeout to {smtp_server}:{smtp_port}. Check firewall settings or try a different port.'
                }), 500
            except ConnectionRefusedError:
                _log_force('error', f"[SMTP] Connection refused by {smtp_server}:{smtp_port}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'Connection refused by {smtp_server}:{smtp_port}. Server may not be accepting connections.'
                }), 500
            except socket.error as sock_err:
                _log_force('error', f"[SMTP] Socket error connecting to {smtp_server}:{smtp_port}: {sock_err}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'Network error: {sock_err}. Check server address and port.'
                }), 500

        # Step 3: TLS handling
        if raw_security == 'starttls':
            _log_force('info', "[SMTP] Step 3/4: Initiating STARTTLS...", extra_secrets=secret_values)
            try:
                context = ssl.create_default_context()
                server.starttls(context=context)
                server.ehlo()
                _log_force('info', "[SMTP] STARTTLS successful", extra_secrets=secret_values)
            except smtplib.SMTPException as tls_err:
                _log_force('error', f"[SMTP] STARTTLS failed: {tls_err}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'TLS negotiation failed: {tls_err}'
                }), 500
        elif raw_security == 'ssl':
            _log_force('info', "[SMTP] Step 3/4: SSL negotiation completed during connection", extra_secrets=secret_values)
        else:
            _log_force('info', "[SMTP] Step 3/4: No TLS requested", extra_secrets=secret_values)
        
        # Step 4: Authentication
        if smtp_username and smtp_password:
            _log_force('info', f"[SMTP] Step 4/4: Authenticating as {smtp_username}...", extra_secrets=secret_values)
            try:
                server.login(smtp_username, smtp_password)
                _log_force('info', f"[SMTP] Authentication successful for {smtp_username}", extra_secrets=secret_values)
            except smtplib.SMTPAuthenticationError as auth_err:
                _log_force('error', f"[SMTP] Authentication failed for {smtp_username}: {auth_err}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': 'Authentication failed. Please check your username and password.'
                }), 401
            except smtplib.SMTPException as smtp_err:
                _log_force('error', f"[SMTP] SMTP error during authentication: {smtp_err}", extra_secrets=secret_values)
                return jsonify({
                    'success': False,
                    'message': f'SMTP authentication error: {smtp_err}'
                }), 500
        else:
            _log_force('info', "[SMTP] Step 4/4: No authentication (username/password not provided)", extra_secrets=secret_values)
        
        # Success - close connection
        _log_force('info', f"[SMTP] All steps completed successfully. Closing connection...", extra_secrets=secret_values)
        server.quit()
        _log_force('info', f"[SMTP] Test completed successfully for {smtp_server}:{smtp_port}", extra_secrets=secret_values)
        
        return jsonify({
            'success': True,
            'message': f'Successfully connected to {smtp_server}:{smtp_port} using {raw_security.upper()} security'
        })
        
    except smtplib.SMTPException as e:
        _log_force('error', f"[SMTP] SMTP error: {type(e).__name__}: {str(e)}", extra_secrets=secret_values)
        return jsonify({
            'success': False,
            'message': f'SMTP error: {str(e)}'
        }), 500
    except socket.timeout:
        _log_force('error', f"[SMTP] Operation timeout for {smtp_server}:{smtp_port}", extra_secrets=secret_values)
        return jsonify({
            'success': False,
            'message': f'Operation timeout. The server may be slow or unreachable.'
        }), 500
    except Exception as e:
        _log_force('error', f"[SMTP] Unexpected error: {type(e).__name__}: {str(e)}", extra_secrets=secret_values, exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Connection failed: {str(e)}'
        }), 500
    finally:
        # Ensure connection is closed
        if server:
            try:
                server.quit()
            except:
                pass

@admin.route('/update-backup-settings', methods=['POST'])
@login_required
@admin_required
def update_backup_settings():
    """Update backup configuration settings"""
    try:
        config = {}
        
        # Get form data
        config['backup_directory'] = request.form.get('backup_directory', 'data/backups').strip()
        
        # Validate the path
        backup_dir = config['backup_directory']
        if not backup_dir:
            flash('Backup directory cannot be empty.', 'danger')
            return redirect(url_for('admin.settings'))
        
        # Save configuration
        if save_backup_config(config):
            flash('Backup settings saved successfully!', 'success')
        else:
            flash('Error saving backup settings. Please try again.', 'danger')
        
        return redirect(url_for('admin.settings'))
        
    except Exception as e:
        _log('error', f"Error updating backup settings: {e}")
        flash('Error updating backup settings. Please try again.', 'danger')
    
    return redirect(url_for('admin.settings'))

@admin.route('/test-ai-connection', methods=['POST'])
@login_required
@admin_required
def test_ai_connection():
    """Test AI connection with provided settings"""
    try:
        # Build config from form data
        config = {
            'AI_PROVIDER': request.form.get('ai_provider', 'openai'),
            'OPENAI_API_KEY': request.form.get('openai_api_key', ''),
            'OPENAI_BASE_URL': request.form.get('openai_base_url', 'https://api.openai.com/v1'),
            'OPENAI_MODEL': request.form.get('openai_model', 'gpt-4o'),
            'OLLAMA_BASE_URL': request.form.get('ollama_base_url', 'http://localhost:11434'),
            'OLLAMA_MODEL': request.form.get('ollama_model_manual') or request.form.get('ollama_model', 'llama3.2-vision:11b'),
            'AI_TIMEOUT': request.form.get('ai_timeout', '30'),
            'AI_MAX_TOKENS': request.form.get('ai_max_tokens', '1000'),
            'AI_TEMPERATURE': request.form.get('ai_temperature', '0.1'),
        }
        
        # Test connection using AI service
        from app.services.ai_service import AIService
        ai_service = AIService(config, task_scope='vision')
        result = ai_service.test_connection()
        
        return jsonify(result)
            
    except Exception as e:
        _log('error', f"Error testing AI connection: {e}")
        return jsonify({'success': False, 'message': 'Connection test failed. Please check your settings.'})

@admin.route('/test-rag-chat', methods=['POST'])
@login_required
@admin_required
def test_rag_chat():
    """Test RAG retrieval with a chat query"""
    current_app.logger.info("RAG: test_rag_chat called")
    try:
        query = request.form.get('query', '').strip()
        current_app.logger.info(f"RAG: Chat query='{query}'")
        if not query:
            return jsonify({'success': False, 'message': 'Query is required'}), 400
            
        # Use the service to query
        results = rag_vector_service.query(text=query, n_results=5)
        count = len(results.get('documents', [[]])[0])
        current_app.logger.info(f"RAG: Query returned {count} results")
        
        # Format results for display
        formatted_results = []
        if results and results.get('documents'):
            documents = results['documents'][0]
            metadatas = results['metadatas'][0]
            distances = results['distances'][0] if results.get('distances') else [0] * len(documents)
            
            for i in range(len(documents)):
                formatted_results.append({
                    'text': documents[i],
                    'metadata': metadatas[i],
                    'distance': distances[i]
                })
                
        return jsonify({
            'success': True, 
            'results': formatted_results
        })
        
    except Exception as e:
        current_app.logger.exception(f"Error testing RAG chat: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@admin.route('/rag-ingestion-status')
@login_required
@admin_required
def rag_ingestion_status():
    """Get status of RAG ingestion background job"""
    try:
        from app.services.opds_embedding_service import get_opds_embedding_runner
        runner = get_opds_embedding_runner()
        return jsonify(runner.get_status())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@admin.route('/rag-indexed-books')
@login_required
@admin_required
def rag_indexed_books():
    """Get list of books indexed in RAG"""
    try:
        books = rag_vector_service.list_indexed_books()
        return jsonify({'success': True, 'books': books})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

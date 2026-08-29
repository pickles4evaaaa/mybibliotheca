"""Admin server-settings panel handler.

The route facade in app.auth owns the public endpoint registration.  Keeping
the implementation here isolates the large panel dispatcher without changing
its request or persistence behavior.
"""

from __future__ import annotations

from typing import Any, Optional

from flask import (
    current_app,
    flash,
    get_flashed_messages,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required

from app.admin import get_admin_settings_context
from app.domain.models import MediaType
from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
from app.services import user_service
from app.services.kuzu_service_facade import _convert_query_result_to_list
from app.utils.user_settings import get_default_book_format


_MEDIA_TYPE_VALUES = {media_type.value for media_type in MediaType}


@login_required
def settings_server_partial(panel: str):
    # Normalize panel to avoid subtle whitespace/case mismatches
    try:
        panel = (panel or '').strip().lower()
    except Exception:
        panel = str(panel)
    if not current_user.is_admin:
        return '<div class="text-danger small">Not authorized.</div>'
    if panel == 'users':
        # Recreate admin.users view inline (no pagination controls yet)
        search = request.args.get('search', '', type=str)
        from app.services.kuzu_async_helper import run_async
        from app.domain.models import User as DomainUser
        from datetime import datetime, timezone as _tz
        def _now():
            try:
                from app.domain.models import now_utc
                return now_utc()
            except Exception:
                return datetime.now(_tz.utc)
        users_render = []
        try:
            repo = getattr(user_service, 'user_repo', None)
            if repo and hasattr(repo, 'get_all'):
                raw_list = run_async(repo.get_all(limit=2000))  # type: ignore
                for row in raw_list:
                    try:
                        # row may be dict-like
                        uid = row.get('id') if isinstance(row, dict) else getattr(row, 'id', None)
                        if not uid:
                            continue
                        username_val = (row.get('username') if isinstance(row, dict) else getattr(row,'username','')) or ''
                        email_val = (row.get('email') if isinstance(row, dict) else getattr(row,'email','')) or ''
                        user_obj = DomainUser(
                            id=uid,
                            username=str(username_val),
                            email=str(email_val),
                            is_admin=bool(row.get('is_admin', False) if isinstance(row, dict) else getattr(row,'is_admin', False)),
                            is_active=bool(row.get('is_active', True) if isinstance(row, dict) else getattr(row,'is_active', True)),
                            password_hash=(row.get('password_hash','') if isinstance(row, dict) else getattr(row,'password_hash','')) or '',
                        )
                        # created_at may be timestamp or string
                        if isinstance(row, dict) and 'created_at' in row and row['created_at']:
                            try:
                                # If numeric timestamp, convert; else parse iso
                                ca = row['created_at']
                                from datetime import datetime
                                if isinstance(ca, (int,float)):
                                    user_obj.created_at = datetime.fromtimestamp(ca/1000, _tz.utc)
                                elif isinstance(ca, str):
                                    user_obj.created_at = datetime.fromisoformat(ca.replace('Z',''))
                            except Exception:
                                pass
                        users_render.append(user_obj)
                    except Exception as ie:
                        current_app.logger.warning(f"User row build failed: {ie}")
        except Exception as e:
            current_app.logger.error(f"Inline users load error (repo path): {e}")
        if search:
            s = search.lower()
            users_render = [u for u in users_render if s in (u.username or '').lower() or s in (u.email or '').lower()]
        # Sort by created_at desc for consistency
        try:
            users_render.sort(key=lambda u: getattr(u,'created_at', _now()), reverse=True)
        except Exception:
            pass
        return render_template('settings/partials/server_users_full.html', users=users_render, search=search)
    if panel == 'user_edit':
        user_id = request.args.get('user_id') or request.form.get('user_id')
        if not user_id:
            return '<div class="text-danger small">User ID missing.</div>'
        target_user = None
        try:
            target_user = user_service.get_user_by_id_sync(user_id)
        except Exception as e:
            current_app.logger.error(f"User edit load error: {e}")
            return '<div class="text-danger small">Failed to load user.</div>'
        if not target_user:
            return '<div class="text-danger small">User not found.</div>'
        # Handle POST actions
        if request.method == 'POST':
            action = request.form.get('action')
            # --- TEMP DEBUG LOGGING START (use error level so it appears with LOG_LEVEL=error) ---
            try:
                current_app.logger.error(f"[USER_DELETE_DEBUG] POST entry action={action} user_id={user_id} form_keys={list(request.form.keys())} headers={{'HX':request.headers.get('HX-Request'), 'XHR': request.headers.get('X-Requested-With')}}")
            except Exception:
                pass
            from app.services.simple_backup_service import get_simple_backup_service
            backup_service = get_simple_backup_service()
            def _trigger_backup(reason: str):
                try:
                    backup_service.create_backup(reason=reason)
                except Exception as be:
                    current_app.logger.warning(f"Auto-backup failed after user change: {be}")
            if action == 'update_profile':
                current_app.logger.error(f"[USER_DELETE_DEBUG] update_profile path entered for {user_id}")
                new_username = request.form.get('username','').strip()
                new_email = request.form.get('email','').strip()
                if new_username and new_email:
                    target_user.username = new_username  # type: ignore
                    target_user.email = new_email  # type: ignore
                    user_service.update_user_sync(target_user)
                    _trigger_backup('user_profile_update')
                    flash('User profile updated & backup created.', 'success')
                else:
                    flash('Username and email required.', 'error')
            elif action == 'reset_password':
                current_app.logger.error(f"[USER_DELETE_DEBUG] reset_password path entered for {user_id}")
                pwd1 = request.form.get('password','')
                pwd2 = request.form.get('confirm_password','')
                if pwd1 and pwd1 == pwd2:
                    target_user.set_password(pwd1)  # type: ignore
                    user_service.update_user_sync(target_user)
                    _trigger_backup('user_password_reset')
                    flash('Password reset & backup created.', 'success')
                else:
                    flash('Passwords must match.', 'error')
            elif action == 'update_role':
                current_app.logger.error(f"[USER_DELETE_DEBUG] update_role path entered for {user_id}")
                requested_role = (request.form.get('role') or '').strip().lower()
                if requested_role not in ('admin', 'user'):
                    flash('Invalid role selection.', 'error')
                else:
                    desired_admin_state = requested_role == 'admin'
                    current_admin_state = bool(getattr(target_user, 'is_admin', False))  # type: ignore
                    if desired_admin_state == current_admin_state:
                        flash('User role already set to the selected value.', 'info')
                    else:
                        if not desired_admin_state:
                            admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
                            if current_admin_state and admin_count <= 1:
                                flash('Cannot remove admin privileges from the last administrator.', 'error')
                            else:
                                target_user.is_admin = False  # type: ignore
                                user_service.update_user_sync(target_user)
                                _trigger_backup('user_role_change')
                                flash('User role updated & backup created.', 'success')
                        else:
                            target_user.is_admin = True  # type: ignore
                            user_service.update_user_sync(target_user)
                            _trigger_backup('user_role_change')
                            flash('User role updated & backup created.', 'success')
            elif action == 'toggle_active':
                current_app.logger.error(f"[USER_DELETE_DEBUG] toggle_active path entered for {user_id}")
                target_user.is_active = not getattr(target_user,'is_active',True)  # type: ignore
                user_service.update_user_sync(target_user)
                _trigger_backup('user_status_change')
                flash('User status toggled & backup created.', 'success')
            elif action == 'delete_user':
                current_app.logger.error(f"[USER_DELETE_DEBUG] delete_user path entered for {user_id}")
                admin_pwd = request.form.get('admin_password','')
                if current_user.check_password(admin_pwd):  # type: ignore
                    try:
                        # Protect against deleting yourself
                        if target_user.id == current_user.id:  # type: ignore
                            flash('Cannot delete your own account.', 'error')
                            current_app.logger.error(f"[USER_DELETE_DEBUG] Attempt to delete self blocked: current_user={current_user.id} target={target_user.id}")
                        else:
                            # Prevent deleting last admin
                            if getattr(target_user, 'is_admin', False):  # type: ignore
                                admin_count = user_service.get_admin_count_sync() if hasattr(user_service, 'get_admin_count_sync') else 1  # type: ignore
                                if admin_count <= 1:
                                    flash('Cannot delete the last admin user.', 'error')
                                    current_app.logger.error(f"[USER_DELETE_DEBUG] Last admin delete blocked admin_count={admin_count}")
                                else:
                                    deleted = user_service.delete_user_sync(target_user.id)  # type: ignore
                                    current_app.logger.error(f"[USER_DELETE_DEBUG] delete attempt admin user deleted={deleted}")
                                    if deleted:
                                        _trigger_backup('user_deleted')
                                        flash('User deleted & backup created.', 'success')
                                    else:
                                        # Diagnostic: check existence directly
                                        try:
                                            repo = getattr(user_service, 'user_repo', None)
                                            exists_flag = False
                                            if repo and hasattr(repo, 'safe_manager'):
                                                check_q = "MATCH (u:User {id: $uid}) RETURN COUNT(u) as c"
                                                res = repo.safe_manager.execute_query(check_q, {"uid": target_user.id})
                                                from app.services.kuzu_service_facade import _convert_query_result_to_list as _cvt
                                                data = _cvt(res)
                                                if data and int(data[0].get('c',0))>0:
                                                    exists_flag = True
                                            current_app.logger.error(f"[USER_DELETE_DEBUG] delete admin failed exists_flag={exists_flag}")
                                            flash(f'Delete failed (diagnostic: exists={exists_flag}).', 'error')
                                        except Exception as de_diag:
                                            current_app.logger.error(f"[USER_DELETE_DEBUG] diagnostic error admin delete: {de_diag}")
                                            flash(f'Delete failed (diag error: {de_diag}).', 'error')
                                        flash('Delete failed.', 'error')
                            else:
                                deleted = user_service.delete_user_sync(target_user.id)  # type: ignore
                                current_app.logger.error(f"[USER_DELETE_DEBUG] delete attempt non-admin deleted={deleted}")
                                if deleted:
                                    _trigger_backup('user_deleted')
                                    flash('User deleted & backup created.', 'success')
                                else:
                                    try:
                                        repo = getattr(user_service, 'user_repo', None)
                                        exists_flag = False
                                        if repo and hasattr(repo, 'safe_manager'):
                                            check_q = "MATCH (u:User {id: $uid}) RETURN COUNT(u) as c"
                                            res = repo.safe_manager.execute_query(check_q, {"uid": target_user.id})
                                            from app.services.kuzu_service_facade import _convert_query_result_to_list as _cvt
                                            data = _cvt(res)
                                            if data and int(data[0].get('c',0))>0:
                                                exists_flag = True
                                        current_app.logger.error(f"[USER_DELETE_DEBUG] delete non-admin failed exists_flag={exists_flag}")
                                        flash(f'Delete failed (diagnostic: exists={exists_flag}).', 'error')
                                    except Exception as de_diag:
                                        current_app.logger.error(f"[USER_DELETE_DEBUG] diagnostic error non-admin delete: {de_diag}")
                                        flash(f'Delete failed (diag error: {de_diag}).', 'error')
                                    flash('Delete failed.', 'error')
                        # Always refresh users list inline (message shown once)
                        try:
                            current_app.logger.error("[USER_DELETE_DEBUG] Fetching updated user list after delete attempt")
                        except Exception:
                            pass
                        updated_users = [u for u in user_service.get_all_users_sync() if u.id != target_user.id]
                        # Retrieve flashed messages (if any) to embed
                        from flask import get_flashed_messages as _flashed_msgs
                        msgs = _flashed_msgs(with_categories=True)
                        try:
                            current_app.logger.error(f"[USER_DELETE_DEBUG] Messages after delete attempt: {msgs}")
                        except Exception:
                            pass
                        return render_template('settings/partials/server_users_full.html', users=updated_users, search='', inline_messages=msgs)
                    except Exception as de:
                        current_app.logger.error(f"[USER_DELETE_DEBUG] Exception during delete flow: {de}")
                        flash('Delete failed.', 'error')
                else:
                    current_app.logger.error("[USER_DELETE_DEBUG] Admin password incorrect for delete")
                    flash('Admin password incorrect.', 'error')
            # reload updated user
            try:
                current_app.logger.error(f"[USER_DELETE_DEBUG] Reloading user editor for user_id={user_id}")
            except Exception:
                pass
            target_user = user_service.get_user_by_id_sync(user_id)
        return render_template('settings/partials/server_user_edit.html', user=target_user)
    if panel == 'debug':
        try:
            import os  # ensure availability in this scope for env and fs ops
            # Support POST to update .env debug flags
            if request.method == 'POST':
                action = request.form.get('action')
                if action == 'update_debug_env':
                    try:
                        # Persist settings to data/.env (volume-backed) for reliability
                        data_dir = current_app.config.get('DATA_DIR', None)
                        if not data_dir:
                            try:
                                # Fall back to project root /data
                                import pathlib
                                data_dir = str(pathlib.Path(current_app.root_path).parent / 'data')
                            except Exception:
                                data_dir = 'data'
                        env_path = os.path.join(data_dir, '.env')
                        # Keys we manage here
                        manage_keys = [
                            'MYBIBLIOTHECA_DEBUG', 'MYBIBLIOTHECA_DEBUG_AUTH', 'MYBIBLIOTHECA_DEBUG_CSRF',
                            'MYBIBLIOTHECA_DEBUG_SESSION', 'MYBIBLIOTHECA_DEBUG_REQUESTS', 'MYBIBLIOTHECA_VERBOSE_INIT',
                            'ABS_LISTENING_DEBUG', 'KUZU_DEBUG', 'LOG_LEVEL'
                        ]
                        # Build desired values from form (checkbox true/false, plus LOG_LEVEL text)
                        def _to_str_bool(name: str) -> str:
                            v = request.form.get(name)
                            return 'true' if (v in ('on','true','1','yes')) else 'false'
                        updates = {
                            'MYBIBLIOTHECA_DEBUG': _to_str_bool('MYBIBLIOTHECA_DEBUG'),
                            'MYBIBLIOTHECA_DEBUG_AUTH': _to_str_bool('MYBIBLIOTHECA_DEBUG_AUTH'),
                            'MYBIBLIOTHECA_DEBUG_CSRF': _to_str_bool('MYBIBLIOTHECA_DEBUG_CSRF'),
                            'MYBIBLIOTHECA_DEBUG_SESSION': _to_str_bool('MYBIBLIOTHECA_DEBUG_SESSION'),
                            'MYBIBLIOTHECA_DEBUG_REQUESTS': _to_str_bool('MYBIBLIOTHECA_DEBUG_REQUESTS'),
                            'MYBIBLIOTHECA_VERBOSE_INIT': _to_str_bool('MYBIBLIOTHECA_VERBOSE_INIT'),
                            'ABS_LISTENING_DEBUG': _to_str_bool('ABS_LISTENING_DEBUG'),
                            'KUZU_DEBUG': _to_str_bool('KUZU_DEBUG'),
                            'LOG_LEVEL': (request.form.get('LOG_LEVEL') or os.getenv('LOG_LEVEL') or 'INFO').strip().upper()
                        }
                        # Ensure directory exists
                        os.makedirs(os.path.dirname(env_path), exist_ok=True)
                        # Use python-dotenv to safely upsert each key
                        try:
                            from dotenv import set_key, load_dotenv as _load
                            for k in manage_keys:
                                if k in updates:
                                    # Avoid quote_mode arg for compatibility with older python-dotenv
                                    set_key(env_path, k, str(updates[k]))
                            # Reload to reflect file changes; also override current env
                            _load(dotenv_path=env_path, override=True)
                        except Exception:
                            # Fallback simple writer if python-dotenv set_key unavailable
                            existing = {}
                            if os.path.exists(env_path):
                                try:
                                    with open(env_path, 'r') as rf:
                                        for line in rf:
                                            s = line.strip()
                                            if s and not s.startswith('#') and '=' in s:
                                                k, v = s.split('=', 1)
                                                existing[k.strip()] = v.strip()
                                except Exception:
                                    existing = {}
                            existing.update(updates)
                            tmp_path = env_path + '.tmp'
                            with open(tmp_path, 'w') as wf:
                                wf.write('# Debug settings (managed by Admin UI)\n')
                                for k in manage_keys:
                                    wf.write(f"{k}={existing.get(k, updates.get(k, ''))}\n")
                            os.replace(tmp_path, env_path)
                        # Also apply updates to current process so changes take effect without full restart
                        try:
                            import logging as _logging
                            for k, v in updates.items():
                                os.environ[k] = str(v)
                            # Adjust Python logging level dynamically when LOG_LEVEL changes
                            lvl_name = str(updates.get('LOG_LEVEL', os.getenv('LOG_LEVEL', 'ERROR'))).upper()
                            lvl = getattr(_logging, lvl_name, _logging.ERROR)
                            _logging.getLogger().setLevel(lvl)
                            try:
                                current_app.logger.setLevel(lvl)
                            except Exception:
                                pass
                        except Exception:
                            # Best effort; if anything fails, at least .env was updated
                            pass
                        flash('Debug environment settings updated. Restart may be required.', 'success')
                    except Exception as we:
                        current_app.logger.error(f"Failed updating debug env: {we}")
                        flash('Failed to update debug environment settings.', 'error')
                elif action == 'toggle_abs_debug':
                    try:
                        from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
                        toggle_to = (request.form.get('toggle_to') or '').strip().lower()
                        enabled = True if toggle_to == 'enable' else False
                        save_abs_settings({'debug_listening_sync': enabled})
                        flash(f"ABS listening debug {'enabled' if enabled else 'disabled'}.", 'success')
                    except Exception as te:
                        current_app.logger.error(f"ABS debug toggle error: {te}")
                        flash('Failed to toggle ABS listening debug.', 'error')
                elif action == 'run_abs_listening_sync':
                    try:
                        from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
                        page_size = request.form.get('page_size')
                        ps = int(page_size) if page_size else 200
                        runner = get_abs_sync_runner()
                        runner.enqueue_listening_sync(str(current_user.id), page_size=ps)
                        flash('Listening sync started. Check Import Progress for updates.', 'info')
                    except Exception as re:
                        current_app.logger.error(f"ABS run sync now error: {re}")
                        flash('Failed to start listening sync.', 'error')
            # Build context values
            def _env_bool(name: str) -> bool:
                return str(os.getenv(name, 'false')).strip().lower() in ('1','true','yes','on')
            env_flags = {
                'MYBIBLIOTHECA_DEBUG': _env_bool('MYBIBLIOTHECA_DEBUG'),
                'MYBIBLIOTHECA_DEBUG_AUTH': _env_bool('MYBIBLIOTHECA_DEBUG_AUTH'),
                'MYBIBLIOTHECA_DEBUG_CSRF': _env_bool('MYBIBLIOTHECA_DEBUG_CSRF'),
                'MYBIBLIOTHECA_DEBUG_SESSION': _env_bool('MYBIBLIOTHECA_DEBUG_SESSION'),
                'MYBIBLIOTHECA_DEBUG_REQUESTS': _env_bool('MYBIBLIOTHECA_DEBUG_REQUESTS'),
                'MYBIBLIOTHECA_VERBOSE_INIT': _env_bool('MYBIBLIOTHECA_VERBOSE_INIT'),
                'ABS_LISTENING_DEBUG': _env_bool('ABS_LISTENING_DEBUG'),
                'KUZU_DEBUG': _env_bool('KUZU_DEBUG')
            }
            log_level = (os.getenv('LOG_LEVEL') or 'INFO').upper()
            from app.utils.audiobookshelf_settings import load_abs_settings
            abs_debug = False
            try:
                abs_debug = bool(load_abs_settings().get('debug_listening_sync'))
            except Exception:
                pass
            # If this was an HTMX/fetch request, just return the partial; otherwise keep users on full settings page
            tpl = render_template('settings/partials/server_debug.html', env_flags=env_flags, log_level=log_level, abs_debug_listening=abs_debug)
            if request.headers.get('HX-Request') or request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return tpl
            # Non-ajax POST/GET: render the partial (settings.html will normally fetch it). Returning partial avoids redirect loop
            return tpl
        except Exception as e:
            current_app.logger.error(f"Debug panel render error: {e}")
            return '<div class="text-danger small">Error loading debug tools.</div>'
    if panel == 'config':
        # Inline server configuration management (mirrors admin.settings POST logic) without redirect
        import os, uuid
        ctx = {}
        if request.method == 'POST':
            # Gather form fields
            site_name = request.form.get('site_name', os.getenv('SITE_NAME', 'MyBibliotheca'))
            server_timezone = request.form.get('server_timezone', 'UTC')
            terminology_preference = request.form.get('terminology_preference', 'genre')
            background_config = {
                'type': request.form.get('background_type', 'default'),
                'solid_color': request.form.get('solid_color', '#667eea'),
                'gradient_start': request.form.get('gradient_start', '#667eea'),
                'gradient_end': request.form.get('gradient_end', '#764ba2'),
                'gradient_direction': request.form.get('gradient_direction', '135deg'),
                'image_url': request.form.get('background_image_url', ''),
                'image_position': request.form.get('image_position', 'cover')
            }
            # Reading defaults (optional numbers)
            try:
                dp_raw = request.form.get('default_pages_per_log', '').strip()
                dm_raw = request.form.get('default_minutes_per_log', '').strip()
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
            # Handle optional image upload
            if 'background_image_file' in request.files:
                file = request.files['background_image_file']
                if file and file.filename:
                    allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                    if '.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in allowed_extensions:
                        try:
                            file_extension = file.filename.rsplit('.', 1)[1].lower()
                            unique_filename = f"bg_{uuid.uuid4().hex}.{file_extension}"
                            data_dir = getattr(current_app.config, 'DATA_DIR', None)
                            if data_dir:
                                upload_dir = os.path.join(data_dir, 'uploads', 'backgrounds')
                            else:
                                base_dir = Path(current_app.root_path).parent
                                upload_dir = os.path.join(base_dir, 'data', 'uploads', 'backgrounds')
                            os.makedirs(upload_dir, exist_ok=True)
                            upload_path = os.path.join(upload_dir, unique_filename)
                            file.save(upload_path)
                            background_config['image_url'] = f"/uploads/backgrounds/{unique_filename}"
                            background_config['type'] = 'image'
                            flash(f'Background image uploaded successfully: {file.filename}', 'success')
                        except Exception as e:
                            current_app.logger.error(f"Inline background upload error: {e}")
                            flash('Error uploading background image.', 'error')
                    else:
                        flash('Invalid background image type.', 'error')
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
                }
            }
            if save_system_config(config):
                flash('System settings saved.', 'success')
            else:
                flash('Failed to save system settings.', 'error')
        # Always refresh context after (or for GET)
        ctx = get_admin_settings_context()
        return render_template('settings/partials/server_config.html', **ctx)
    if panel == 'smtp':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        ctx = {
            'smtp_config': load_smtp_config(),
            'site_name': (load_system_config() or {}).get('site_name', 'MyBibliotheca'),
        }
        return render_template('settings/partials/server_smtp.html', **ctx)
    if panel == 'backup':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        ctx = {
            'backup_config': load_backup_config(),
        }
        return render_template('settings/partials/server_backup.html', **ctx)
    if panel == 'ai':
        ctx = get_admin_settings_context()
        ctx['ai_config'] = load_ai_config()
        return render_template('settings/partials/server_ai.html', **ctx)
    if panel == 'opds':
        from app.utils.opds_settings import load_opds_settings, save_opds_settings
        from app.utils.opds_mapping import clean_mapping, build_source_options, MB_FIELD_WHITELIST, MB_FIELD_LABELS
        from app.services import opds_probe_service as _opds_probe_service
        from app.services import ensure_opds_sync_runner, get_opds_sync_runner
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        from markupsafe import Markup, escape

        try:
            ensure_opds_sync_runner()
        except Exception as runner_err:
            try:
                current_app.logger.debug(f"OPDS runner init skipped: {runner_err}")
            except Exception:
                pass

        settings = load_opds_settings()
        stored_password = settings.get('password') or ''
        has_password = bool(settings.get('password_present')) or bool(stored_password)
        field_inventory = session.get('opds_field_inventory') or settings.get('last_field_inventory') or {}
        mapping = settings.get('mapping') or {}
        probe_result = None
        sync_result = None
        suggestions = None
        status_message = None
        error_message = None

        last_test_summary = settings.get('last_test_summary')
        last_test_preview = settings.get('last_test_preview') or []
        pending_jobs: list[dict[str, Any]] = []
        pending_job_ids: set[Any] = set()

        def _build_pending_job(kind: str, status_value: Any, task_id: Any, api_url: Any, progress_url: Any) -> dict[str, Any] | None:
            if not task_id or not api_url:
                return None
            status_norm = str(status_value or '').strip().lower() or 'queued'
            allowed_prefixes = ('queued', 'running', 'in-progress', 'in progress')
            if not any(status_norm.startswith(prefix) for prefix in allowed_prefixes):
                return None
            return {
                'task_id': task_id,
                'api_progress_url': api_url,
                'progress_url': progress_url,
                'kind': kind,
                'status': status_norm,
            }

        def _append_pending_job(job: dict[str, Any]) -> None:
            task_id = job.get('task_id')
            if not task_id or task_id in pending_job_ids:
                return
            pending_job_ids.add(task_id)
            pending_jobs.append(job)

        if request.method == 'POST':
            form = request.form
            action = (form.get('action') or '').strip().lower()
            base_url = (form.get('base_url') or settings.get('base_url') or '').strip()
            username = (form.get('username') or settings.get('username') or '').strip()
            user_agent = (form.get('user_agent') or settings.get('user_agent') or '').strip()
            password_input = form.get('password')
            clear_password = form.get('clear_password')
            mapping_json = form.get('mapping_json') or '{}'
            try:
                incoming_mapping = _json.loads(mapping_json) if mapping_json else {}
            except Exception as parse_err:
                current_app.logger.warning(f"OPDS mapping parse failed: {parse_err}")
                incoming_mapping = {}
                error_message = 'Mapping JSON malformed – ignoring submitted mapping.'

            inventory_for_clean = field_inventory or settings.get('last_field_inventory') or {}
            cleaned_mapping = clean_mapping(incoming_mapping, inventory_for_clean)

            update_payload: dict[str, Any] = {}
            if base_url:
                update_payload['base_url'] = base_url
            if username or settings.get('username'):
                update_payload['username'] = username
            if user_agent or settings.get('user_agent'):
                update_payload['user_agent'] = user_agent
            if cleaned_mapping is not None:
                update_payload['mapping'] = cleaned_mapping

            auto_sync_flag = form.get('auto_sync_enabled')
            if auto_sync_flag is not None:
                update_payload['auto_sync_enabled'] = str(auto_sync_flag).strip().lower() in ('1', 'true', 'yes', 'on')
            interval_raw = form.get('auto_sync_every_hours')
            if interval_raw is not None:
                update_payload['auto_sync_every_hours'] = interval_raw
            auto_user_id = form.get('auto_sync_user_id')
            if auto_user_id is not None:
                update_payload['auto_sync_user_id'] = auto_user_id.strip()

            password_for_request = stored_password
            if clear_password:
                update_payload['password'] = ''
                password_for_request = ''
                has_password = False
            elif password_input:
                update_payload['password'] = password_input
                password_for_request = password_input
                has_password = True

            def _refresh_settings() -> None:
                nonlocal settings, stored_password, mapping, last_test_summary, last_test_preview
                settings = load_opds_settings()
                stored_password = settings.get('password') or ''
                mapping = settings.get('mapping') or {}
                last_test_summary = settings.get('last_test_summary')
                last_test_preview = settings.get('last_test_preview') or []

            if action == 'save-settings':
                try:
                    if (
                        update_payload
                        and 'password' not in update_payload
                        and not base_url
                        and not username
                        and not user_agent
                        and incoming_mapping == {}
                        and auto_sync_flag is None
                        and interval_raw is None
                        and auto_user_id is None
                    ):
                        status_message = 'No changes detected.'
                    else:
                        save_ok = save_opds_settings(update_payload)
                        if save_ok:
                            status_message = 'OPDS settings saved.'
                            _refresh_settings()
                            field_inventory = settings.get('last_field_inventory') or field_inventory
                            session['opds_field_inventory'] = field_inventory
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        else:
                            error_message = 'Failed to save OPDS settings.'
                except Exception as err:
                    current_app.logger.error(f"OPDS settings save error: {err}")
                    error_message = 'Unexpected error saving settings.'
            elif action == 'probe':
                if not base_url:
                    error_message = 'Base URL is required for probe.'
                else:
                    try:
                        if update_payload:
                            save_opds_settings(update_payload)
                            _refresh_settings()
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        probe_result = _opds_probe_service.probe_sync(
                            base_url,
                            username=username or None,
                            password=password_for_request or None,
                            user_agent=user_agent or None,
                        )
                        field_inventory = probe_result.get('field_inventory') or {}
                        session['opds_field_inventory'] = field_inventory
                        suggestions = probe_result.get('mapping_suggestions') or {}
                        status_message = f"Probe complete: {len(probe_result.get('samples', []))} sample entries detected."
                        save_opds_settings({
                            'last_field_inventory': field_inventory,
                            'mapping': cleaned_mapping,
                            'last_probe_summary': probe_result,
                        })
                        _refresh_settings()
                        has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                    except Exception as err:
                        current_app.logger.error(f"OPDS probe failed: {err}")
                        error_message = f"Probe failed: {err}"
            elif action == 'sync-now':
                if not base_url:
                    error_message = 'Base URL is required before syncing.'
                elif not cleaned_mapping:
                    error_message = 'At least one field mapping is required before syncing.'
                else:
                    try:
                        if update_payload:
                            save_opds_settings(update_payload)
                            _refresh_settings()
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        ensure_opds_sync_runner()
                        runner = get_opds_sync_runner()
                        limit_raw = form.get('sync_limit')
                        limit_value = None
                        if limit_raw:
                            try:
                                limit_value = max(1, int(limit_raw))
                            except Exception:
                                limit_value = None
                        job_info = runner.enqueue_sync(str(current_user.id), limit=limit_value)
                        now_iso = _dt.now(_tz.utc).isoformat()
                        message_text = 'Sync job queued.'
                        sync_task_id = None
                        sync_progress_url = None
                        sync_api_url = None
                        if isinstance(job_info, dict):
                            sync_task_id = job_info.get('task_id')
                            sync_progress_url = job_info.get('progress_url')
                            sync_api_url = job_info.get('api_progress_url')
                            task_id_text = escape(str(sync_task_id or ''))
                            message_text = f"Sync job queued as task <code>{task_id_text}</code>."
                            if sync_progress_url:
                                message_text += f' <a href="{escape(sync_progress_url)}" class="link-primary">View progress</a>'
                        status_message = Markup(message_text)
                        save_payload = {
                            'last_sync_status': 'queued',
                            'last_sync_at': now_iso,
                            'last_sync_task_id': sync_task_id,
                            'last_sync_task_progress_url': sync_progress_url,
                            'last_sync_task_api_url': sync_api_url,
                        }
                        if limit_value:
                            save_payload['last_sync_summary'] = {'status': 'queued', 'limit': limit_value, 'timestamp': now_iso}
                        save_opds_settings(save_payload)
                        _refresh_settings()
                        has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        queued_job = _build_pending_job('sync', 'queued', sync_task_id, sync_api_url, sync_progress_url)
                        if queued_job:
                            _append_pending_job(queued_job)
                    except Exception as err:
                        current_app.logger.error(f"OPDS sync enqueue failed: {err}")
                        error_message = f"Sync failed: {err}"
            elif action == 'test-sync':
                if not base_url:
                    error_message = 'Base URL is required before running a test sync.'
                elif not cleaned_mapping:
                    error_message = 'At least one field mapping is required before testing the sync.'
                else:
                    try:
                        if update_payload:
                            save_opds_settings(update_payload)
                            _refresh_settings()
                            has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        ensure_opds_sync_runner()
                        runner = get_opds_sync_runner()
                        limit_raw = form.get('test_limit') or '10'
                        try:
                            limit_value = max(1, min(50, int(limit_raw)))
                        except Exception:
                            limit_value = 10
                        job_info = runner.enqueue_test_sync(str(current_user.id), limit=limit_value)
                        message_text = f"Test sync queued (limit {limit_value})."
                        test_task_id = None
                        test_progress_url = None
                        test_api_url = None
                        if isinstance(job_info, dict):
                            test_task_id = job_info.get('task_id')
                            test_progress_url = job_info.get('progress_url')
                            test_api_url = job_info.get('api_progress_url')
                            task_id_text = escape(str(test_task_id or ''))
                            if task_id_text:
                                message_text += f" Task <code>{task_id_text}</code>."
                            if test_progress_url:
                                message_text += f' <a href="{escape(test_progress_url)}" class="link-primary">Track progress</a>'
                        status_message = Markup(message_text)
                        now_iso = _dt.now(_tz.utc).isoformat()
                        save_opds_settings({
                            'last_test_summary': {'status': 'queued', 'limit': limit_value, 'timestamp': now_iso},
                            'last_test_preview': [],
                            'last_test_task_id': test_task_id,
                            'last_test_task_progress_url': test_progress_url,
                            'last_test_task_api_url': test_api_url,
                        })
                        _refresh_settings()
                        has_password = bool(settings.get('password_present')) or bool(settings.get('password'))
                        queued_job = _build_pending_job('test', 'queued', test_task_id, test_api_url, test_progress_url)
                        if queued_job:
                            _append_pending_job(queued_job)
                    except Exception as err:
                        current_app.logger.error(f"OPDS test sync enqueue failed: {err}")
                        error_message = f"Test sync failed: {err}"
            else:
                error_message = 'Unknown action.'

        if probe_result is None:
            probe_result = settings.get('last_probe_summary')
        if sync_result is None:
            sync_result = settings.get('last_sync_summary')

        # Prepare view context (avoid leaking password)
        settings_view = dict(settings)
        password_value = stored_password if has_password else ''
        settings_view.pop('password', None)
        settings_view['password_present'] = has_password
        settings_view['password_value'] = password_value

        source_options = build_source_options(field_inventory)
        if suggestions is None:
            suggestions = settings.get('mapping_suggestions') or {}

        def _register_pending_job(kind: str, status_value: Any, task_id: Any, api_url: Any, progress_url: Any) -> None:
            job = _build_pending_job(kind, status_value, task_id, api_url, progress_url)
            if job:
                _append_pending_job(job)

        _register_pending_job(
            'test',
            (last_test_summary or {}).get('status'),
            settings.get('last_test_task_id'),
            settings.get('last_test_task_api_url'),
            settings.get('last_test_task_progress_url'),
        )
        _register_pending_job(
            'sync',
            settings.get('last_sync_status'),
            settings.get('last_sync_task_id'),
            settings.get('last_sync_task_api_url'),
            settings.get('last_sync_task_progress_url'),
        )

        preview_columns = list(mapping.keys()) if mapping else []
        preview_rows: list[dict[str, Any]] = []

        if not preview_columns and last_test_preview:
            fallback_priority = [
                'title',
                'subtitle',
                'authors',
                'opds_source_id',
                'entry_id',
                'publisher',
                'average_rating',
                'language',
                'categories',
                'tags',
                'series',
                'series_order',
                'page_count',
                'published_date',
                'cover_url',
                'media_type',
            ]
            discovered_fields: list[str] = []
            for entry in last_test_preview:
                if not isinstance(entry, dict):
                    continue
                for key in entry.keys():
                    if key in ('action', 'reason', 'recent_activity', 'summary', 'raw_links'):
                        continue
                    if key not in discovered_fields:
                        discovered_fields.append(key)
            prioritized = [field for field in fallback_priority if field in discovered_fields]
            remaining = [field for field in discovered_fields if field not in prioritized]
            preview_columns = (prioritized + remaining)[:10]

        def _stringify_preview_value(value: Any) -> str:
            if value is None or value == '':
                return ''
            if isinstance(value, list):
                return ', '.join(str(v) for v in value if v not in (None, ''))
            if isinstance(value, dict):
                try:
                    return _json.dumps(value, ensure_ascii=False)
                except Exception:
                    return str(value)
            return str(value)

        def _build_preview_cell(entry: dict[str, Any], field_name: str) -> dict[str, Any]:
            entry_payload = entry.get('entry') if isinstance(entry.get('entry'), dict) else {}
            raw_value: Any
            if field_name.startswith('contributors.'):
                role = field_name.split('.', 1)[1].upper() if '.' in field_name else ''
                contributors = entry.get('contributors') or []
                if not contributors and isinstance(entry_payload, dict):
                    contributors = entry_payload.get('contributors') or []
                names = []
                for contributor in contributors:
                    if not isinstance(contributor, dict):
                        continue
                    c_role = str(contributor.get('role') or '').upper()
                    if c_role == role:
                        name_val = contributor.get('name')
                        if name_val:
                            names.append(str(name_val))
                raw_value = names
            elif field_name == 'opds_source_id':
                raw_value = entry.get('opds_source_id') or entry.get('entry_id') or entry.get('id')
                if raw_value is None and isinstance(entry_payload, dict):
                    raw_value = entry_payload.get('opds_source_id') or entry_payload.get('id')
            else:
                raw_value = entry.get(field_name)
                if raw_value is None and isinstance(entry_payload, dict):
                    raw_value = entry_payload.get(field_name)
            display_text = _stringify_preview_value(raw_value)
            cell_url = None
            if isinstance(raw_value, str):
                lower_value = raw_value.lower()
                if lower_value.startswith(('http://', 'https://')):
                    cell_url = raw_value
                elif field_name == 'cover_url' and raw_value.startswith('/'):
                    cell_url = raw_value
            return {
                'text': display_text or '—',
                'url': cell_url,
            }

        def _resolve_entry_link(payload: Optional[dict[str, Any]]) -> Optional[str]:
            if not isinstance(payload, dict):
                return None
            links = payload.get('raw_links')
            if isinstance(links, list):
                for link in links:
                    if not isinstance(link, dict):
                        continue
                    rel = str(link.get('rel') or '').lower()
                    href = link.get('href')
                    if not isinstance(href, str) or not href:
                        continue
                    if rel in {'self', 'alternate'} or rel.endswith('/self'):
                        return href
                for link in links:
                    if not isinstance(link, dict):
                        continue
                    href = link.get('href')
                    if isinstance(href, str) and href:
                        return href
            return None

        if preview_columns and last_test_preview:
            for entry in last_test_preview:
                if not isinstance(entry, dict):
                    continue
                row_values = {field: _build_preview_cell(entry, field) for field in preview_columns}
                raw_inspect_payload = entry.get('entry')
                inspect_payload: dict[str, Any] = raw_inspect_payload if isinstance(raw_inspect_payload, dict) else {}
                entry_link = _resolve_entry_link(inspect_payload)
                opds_identifier = inspect_payload.get('opds_source_id') if inspect_payload else None
                if not opds_identifier:
                    opds_identifier = entry.get('opds_source_id')
                preview_rows.append({
                    'action': entry.get('action'),
                    'reason': entry.get('reason'),
                    'columns': row_values,
                    'inspect_payload': inspect_payload,
                    'inspect_entry_link': entry_link,
                    'opds_source_id': opds_identifier,
                })

        return render_template(
            'settings/partials/server_opds.html',
            settings=settings_view,
            mapping=mapping,
            mapping_fields=MB_FIELD_WHITELIST,
            mapping_labels=MB_FIELD_LABELS,
            source_options=source_options,
            field_inventory=field_inventory,
            probe_result=probe_result,
            sync_result=sync_result,
            status_message=status_message,
            error_message=error_message,
            suggestions=suggestions,
            last_test_summary=last_test_summary,
            last_test_preview=last_test_preview,
            preview_columns=preview_columns,
            preview_rows=preview_rows,
            pending_jobs=pending_jobs,
        )
    if panel == 'audiobookshelf':
        # Admin-only ABS settings management
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
        from app.services.audiobookshelf_service import get_client_from_settings
        import json as _json
        settings = load_abs_settings()
        connection_test = None
        expects_partial = bool(
            request.headers.get('HX-Request')
            or (request.headers.get('X-Requested-With') or '').lower() in {'xmlhttprequest', 'fetch'}
        )
        # Handle POST to save settings
        if request.method == 'POST':
            payload: dict[str, Any] = {}
            # Allow form or JSON
            if request.content_type and 'application/json' in request.content_type.lower():
                try:
                    payload = (request.get_json(silent=True) or {})
                except Exception:
                    payload = {}
            else:
                payload = {}
                if 'base_url' in request.form:
                    payload['base_url'] = (request.form.get('base_url') or '').strip()
                # Global toggles (treat absence as False)
                payload['enabled'] = True if request.form.get('enabled') else False
                libs_raw = (request.form.get('library_ids') or '').strip()
                if libs_raw:
                    try:
                        # support comma-separated or JSON array -> store as comma string; utils will normalize
                        if libs_raw.startswith('['):
                            arr = _json.loads(libs_raw)
                            if isinstance(arr, list):
                                payload['library_ids'] = ','.join([str(s).strip() for s in arr if str(s).strip()])
                            else:
                                payload['library_ids'] = libs_raw
                        else:
                            payload['library_ids'] = ','.join([s.strip() for s in libs_raw.split(',') if s.strip()])
                    except Exception:
                        payload['library_ids'] = libs_raw
                # Scheduler fields (from form)
                if 'auto_sync_enabled' in request.form or 'library_sync_every_hours' in request.form or 'listening_sync_every_hours' in request.form:
                    payload['auto_sync_enabled'] = True if request.form.get('auto_sync_enabled') else False
                    try:
                        payload['library_sync_every_hours'] = int(request.form.get('library_sync_every_hours') or 24)
                    except Exception:
                        payload['library_sync_every_hours'] = 24
                    try:
                        payload['listening_sync_every_hours'] = int(request.form.get('listening_sync_every_hours') or 12)
                    except Exception:
                        payload['listening_sync_every_hours'] = 12
                # Enforce order field (treat absence as False)
                payload['enforce_book_first'] = True if request.form.get('enforce_book_first') else False
            ok = save_abs_settings(payload)
            if ok:
                settings = load_abs_settings()
            if not expects_partial and not request.is_json:
                flash(
                    'Audiobookshelf settings saved.' if ok else 'Failed to save Audiobookshelf settings.',
                    'success' if ok else 'error',
                )
                return redirect(url_for('auth.settings', section='server', panel='audiobookshelf'))
        # Optionally test connection if query flag present
        try:
            if request.args.get('test') == '1':
                client = get_client_from_settings(settings)
                # Prefer current user's ABS API key if set
                try:
                    from app.utils.user_settings import load_user_settings
                    us = load_user_settings(getattr(current_user, 'id', None))
                    base_url = settings.get('base_url') or ''
                    user_api_key = (us.get('abs_api_key') or '').strip() if isinstance(us, dict) else ''
                    if base_url and user_api_key:
                        from app.services.audiobookshelf_service import AudiobookShelfClient
                        client = AudiobookShelfClient(base_url, user_api_key)
                except Exception:
                    pass
                connection_test = client.test_connection() if client else { 'ok': False, 'message': 'Missing base_url or api_key' }
        except Exception:
            connection_test = { 'ok': False, 'message': 'Connection test failed' }
        return render_template('settings/partials/server_audiobookshelf.html', settings=settings, connection_test=connection_test)
    if panel == 'metadata':
        from app.utils.metadata_settings import get_metadata_settings, save_metadata_settings
        if request.method == 'POST':
            data = {}
            try:
                current_app.logger.error('[METADATA_SAVE][BEGIN] ct=%s is_json=%s content_length=%s', request.content_type, request.is_json, request.content_length)
                # IMPORTANT: use cache=True so we can read body and still parse
                raw_body = request.get_data(cache=True, as_text=True)
                current_app.logger.error('[METADATA_SAVE][RAW_BODY]%s', (raw_body or '')[:2000])
                import json as _json
                if request.content_type and 'application/json' in request.content_type.lower():
                    try:
                        data = _json.loads(raw_body or '{}')
                    except Exception as je:
                        current_app.logger.error(f'[METADATA_SAVE][JSON_DECODE_ERR] {je}')
                        data = {}
                else:
                    raw = request.form.get('data')
                    if raw:
                        try:
                            data = _json.loads(raw)
                        except Exception as fe:
                            current_app.logger.error(f'[METADATA_SAVE][FORM_JSON_ERR] {fe}')
                    # fallback attempt if still empty and raw body looks like JSON
                    if not data and raw_body and raw_body.strip().startswith('{'):
                        try:
                            data = _json.loads(raw_body)
                            current_app.logger.error('[METADATA_SAVE][FALLBACK_PARSE_OK]')
                        except Exception as fe2:
                            current_app.logger.error(f'[METADATA_SAVE][FALLBACK_PARSE_ERR] {fe2}')
                has_books = isinstance(data.get('books'), dict)
                has_people = isinstance(data.get('people'), dict)
                current_app.logger.error('[METADATA_SAVE][PARSED_KEYS] keys=%s books=%s people=%s', list(data.keys())[:8], has_books, has_people)
                if not (has_books or has_people):
                    current_app.logger.error('[METADATA_SAVE][WARN] No valid books/people objects found in payload; aborting save.')
                    return jsonify({'ok': False, 'error': 'no_valid_payload'}), 400
                # sanity: ensure payload not unexpectedly huge
                if len(str(data)) > 20000:
                    current_app.logger.error('[METADATA_SAVE][WARN] Payload unusually large size=%s', len(str(data)))
                ok = save_metadata_settings(data)
                current_app.logger.error('[METADATA_SAVE][RESULT] ok=%s', ok)
                if not ok:
                    return jsonify({'ok': False, 'error': 'save_returned_false', 'received_keys': list(data.keys())}), 400
                return jsonify({'ok': True, 'metadata_settings': get_metadata_settings()})
            except Exception as e:
                current_app.logger.error(f"Metadata settings save failed: {e}")
                import traceback, sys
                traceback.print_exc(file=sys.stderr)
                return jsonify({'ok': False, 'error': 'save_failed'}), 400
        metadata_settings = get_metadata_settings()
        return render_template('settings/partials/server_metadata.html', metadata_settings=metadata_settings)
    if panel == 'repairs':
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'

        def _run_query(rows_query: str, params: dict[str, Any] | None = None, op: str = "repairs") -> list[dict[str, Any]]:
            try:
                result = safe_execute_kuzu_query(rows_query, params or {}, operation=op)
                rows = _convert_query_result_to_list(result)
                # Normalize to plain dicts with string keys
                normalized: list[dict[str, Any]] = []
                for row in rows or []:
                    if isinstance(row, dict):
                        norm_row = {str(k): row[k] for k in row.keys()}
                        # Provide col_0 fallback for single-column 'result' rows
                        if 'result' in norm_row and 'col_0' not in norm_row:
                            norm_row['col_0'] = norm_row['result']
                        normalized.append(norm_row)
                return normalized
            except Exception as err:
                try:
                    current_app.logger.warning(f"Repairs query failed ({op}): {err}")
                except Exception:
                    pass
                return []

        action_taken = (request.form.get('action') or '').strip().lower() if request.method == 'POST' else ''
        default_media = get_default_book_format()

        if request.method == 'POST':
            current_app.logger.error(
                '[REPAIRS][REQUEST] action=%s user=%s ajax=%s',
                action_taken or '<missing>', getattr(current_user, 'id', '<unknown>'),
                bool(request.headers.get('X-Requested-With') or request.headers.get('HX-Request')),
            )

        if request.method == 'POST' and action_taken:
            if action_taken == 'backfill_media_type':
                try:
                    updated_rows = _run_query(
                        """
                        MATCH (b:Book)
                        WHERE b.media_type IS NULL OR b.media_type = ''
                        SET b.media_type = $media_type
                        RETURN COUNT(b) AS updated
                        """,
                        {'media_type': default_media},
                        op='repairs_backfill_media_type'
                    )
                    updated = 0
                    if updated_rows:
                        row = updated_rows[0]
                        updated = int(row.get('updated') or row.get('col_0') or 0)
                    if updated:
                        flash(f'Updated media type for {updated} book(s).', 'success')
                    else:
                        flash('No books were missing a media type.', 'info')
                except Exception as err:
                    current_app.logger.error(f"Repair action backfill_media_type failed: {err}")
                    flash('Failed to backfill media types. Check logs for details.', 'error')
            elif action_taken == 'assign_default_location':
                try:
                    from app.location_service import LocationService
                    location_service = LocationService()
                    default_location = location_service.get_default_location()
                    if not default_location:
                        defaults = location_service.setup_default_locations()
                        default_location = defaults[0] if defaults else None
                    if not default_location or not getattr(default_location, 'id', None):
                        flash('No default location is available. Create a location first.', 'warning')
                    else:
                        updated_rows = _run_query(
                            """
                            MATCH (loc:Location {id: $loc_id})
                            WITH loc
                            MATCH (b:Book)
                            WHERE NOT (b)-[:STORED_AT]->(:Location)
                            MERGE (b)-[:STORED_AT]->(loc)
                            RETURN COUNT(b) AS updated
                            """,
                            {'loc_id': default_location.id},
                            op='repairs_assign_default_location'
                        )
                        updated = 0
                        if updated_rows:
                            row = updated_rows[0]
                            updated = int(row.get('updated') or row.get('col_0') or 0)
                        if updated:
                            flash(f'Default location assigned to {updated} book(s).', 'success')
                        else:
                            flash('All books already have a location assigned.', 'info')
                except Exception as err:
                    current_app.logger.error(f"Repair action assign_default_location failed: {err}")
                    flash('Failed to assign default locations. Check logs for details.', 'error')
            elif action_taken in {'assign_missing_isbns', 'fetch_missing_covers'}:
                try:
                    from app.services.book_repair_service import start_repair_job
                    job = start_repair_job(action_taken)
                    current_app.logger.error('[REPAIRS][QUEUED] action=%s job=%s', action_taken, job.get('job_id'))
                    flash('Repair started. Progress will update in this panel.', 'info')
                except Exception as err:
                    current_app.logger.error(f"Repair action {action_taken} failed: {err}", exc_info=True)
                    flash(f'Could not start the {"ISBN" if action_taken == "assign_missing_isbns" else "cover"} repair. Check logs for details.', 'error')
            else:
                flash('Unknown repair action.', 'warning')

        issue_stats: dict[str, int | None] = {}
        issue_samples: dict[str, list[dict[str, Any]]] = {}

        total_books_rows = _run_query(
            "MATCH (b:Book) RETURN COUNT(b) AS total",
            op='repairs_total_books'
        )
        total_books = 0
        if total_books_rows:
            total_books = int(
                total_books_rows[0].get('total')
                or total_books_rows[0].get('count')
                or total_books_rows[0].get('result')
                or total_books_rows[0].get('col_0')
                or 0
            )

        stats_config = [
            (
                'missing_media_type',
                "MATCH (b:Book) WHERE b.media_type IS NULL OR b.media_type = '' RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE b.media_type IS NULL OR b.media_type = '' RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
            (
                'missing_authors',
                "MATCH (b:Book) WHERE NOT (b)-[:AUTHORED]->(:Person) AND NOT (b)-[:WRITTEN_BY]->(:Person) RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE NOT (b)-[:AUTHORED]->(:Person) AND NOT (b)-[:WRITTEN_BY]->(:Person) RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
            (
                'missing_locations',
                "MATCH (b:Book) WHERE NOT (b)-[:STORED_AT]->(:Location) RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE NOT (b)-[:STORED_AT]->(:Location) RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
            (
                'missing_isbn',
                "MATCH (b:Book) WHERE (b.isbn13 IS NULL OR b.isbn13 = '') AND (b.isbn10 IS NULL OR b.isbn10 = '') RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE (b.isbn13 IS NULL OR b.isbn13 = '') AND (b.isbn10 IS NULL OR b.isbn10 = '') RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
            (
                'missing_covers',
                "MATCH (b:Book) WHERE b.cover_url IS NULL OR b.cover_url = '' RETURN COUNT(b) AS total",
                "MATCH (b:Book) WHERE b.cover_url IS NULL OR b.cover_url = '' RETURN b.id AS id, b.title AS title, b.updated_at AS updated_at ORDER BY b.updated_at DESC LIMIT 6"
            ),
        ]

        for key, count_query, sample_query in stats_config:
            count_rows = _run_query(count_query, op=f'repairs_{key}_count')
            count_val = 0
            if count_rows:
                row = count_rows[0]
                try:
                    count_val = int(
                        row.get('total')
                        or row.get('count')
                        or row.get('result')
                        or row.get('col_0')
                        or 0
                    )
                except Exception:
                    count_val = 0
            issue_stats[key] = count_val
            sample_rows = _run_query(sample_query, op=f'repairs_{key}_sample')
            normalized_samples: list[dict[str, Any]] = []
            for item in sample_rows:
                normalized_samples.append({
                    'id': item.get('id') or item.get('book_id') or item.get('col_0'),
                    'title': item.get('title') or item.get('name') or item.get('col_1'),
                    'updated_at': item.get('updated_at') or item.get('col_2') or item.get('result')
                })
            issue_samples[key] = normalized_samples

        try:
            total_issues = sum(int(issue_stats.get(k) or 0) for k in issue_stats.keys())
        except Exception:
            total_issues = 0

        inline_messages = get_flashed_messages(with_categories=True)

        try:
            from app.services.book_repair_service import get_active_repair_job, get_repair_job
            repair_job = get_repair_job(request.args.get('repair_job')) or get_active_repair_job()
        except Exception:
            repair_job = None

        return render_template(
            'settings/partials/server_repairs.html',
            issue_stats=issue_stats,
            issue_samples=issue_samples,
            total_books=total_books,
            total_issues=total_issues,
            default_media=default_media,
            inline_messages=inline_messages,
            repair_job=repair_job,
        )
    if panel == 'jobs':
        # Admin view of all import/sync jobs across users
        if not current_user.is_admin:
            return '<div class="text-danger small">Not authorized.</div>'
        try:
            from app.utils.safe_import_manager import safe_import_manager, safe_get_import_job
            # ABS runner health (best effort)
            runner_alive = False
            try:
                from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
                _runner = get_abs_sync_runner()
                try:
                    _runner.ensure_started()
                except Exception:
                    pass
                th = getattr(_runner, '_thread', None)
                runner_alive = bool(th and hasattr(th, 'is_alive') and th.is_alive())
            except Exception:
                runner_alive = False
            # High-level debug map to discover all user_ids and task_ids
            debug_map = safe_import_manager.get_jobs_for_admin_debug(current_user.id, include_user_data=True)
            # Collect detailed records for rendering
            jobs = []
            jobs_by_user = debug_map.get('jobs_by_user') or {}
            for uid, tasks in jobs_by_user.items():
                try:
                    for tid in tasks.keys():
                        try:
                            job = safe_get_import_job(uid, tid) or {}
                        except Exception:
                            job = {'task_id': tid, 'user_id': uid, 'status': 'unknown'}
                        # Normalize fields
                        entry = {
                            'task_id': job.get('task_id', tid),
                            'user_id': job.get('user_id', uid),
                            'type': job.get('type', job.get('import_type', 'unknown')),
                            'status': job.get('status', 'unknown'),
                            'created_at': job.get('created_at', ''),
                            'updated_at': job.get('updated_at', ''),
                            'processed': job.get('processed', 0),
                            'total': job.get('total', job.get('total_books', 0)),
                            'message': job.get('message') or (job.get('error_messages', [''])[0] if job.get('error_messages') else ''),
                        }
                        jobs.append(entry)
                except Exception:
                    continue
            # Sort newest first (fallback to unsorted on parse error)
            def _ts(j):
                ts = j.get('updated_at') or j.get('created_at') or ''
                return str(ts)
            try:
                jobs.sort(key=_ts, reverse=True)
            except Exception:
                pass
            # Manager stats for header
            stats = safe_import_manager.get_statistics()
            stats['abs_runner_alive'] = runner_alive
        except Exception as e:
            current_app.logger.error(f"Jobs panel error: {e}")
            jobs = []
            stats = {'total_active_jobs': 0, 'total_users_with_jobs': 0, 'operation_stats': {}, 'abs_runner_alive': False}
        return render_template('settings/partials/server_jobs.html', jobs=jobs, manager_stats=stats)
    # 'system' panel removed; info moved to overview section
    return '<div class="text-danger small">Unknown panel.</div>'


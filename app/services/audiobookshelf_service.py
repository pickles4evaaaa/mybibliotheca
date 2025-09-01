"""
Audiobookshelf (ABS) client and thin service wrapper.

Responsibilities:
- Load ABS settings (base_url, api_key, library_ids)
- Provide API methods: test_connection, list_libraries, list_library_items, get_item
- Designed to be used by ABS import services

Notes:
- Uses requests with short timeouts
- Does not persist settings; rely on utils.audiobookshelf_settings for that
"""

from typing import Dict, Any, List, Optional, Tuple

import requests  # type: ignore


DEFAULT_TIMEOUT = 8


class AudiobookShelfClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = DEFAULT_TIMEOUT):
        self.base_url = (base_url or '').rstrip('/')
        self.api_key = api_key or ''
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _url(self, path: str) -> str:
        if not path.startswith('/'):
            path = '/' + path
        return f"{self.base_url}{path}"

    def test_connection(self) -> Dict[str, Any]:
        """Ping a simple endpoint to validate connectivity and auth.

        Returns: { ok: bool, message: str, libraries?: list }
        """
        if not self.base_url:
            return {"ok": False, "message": "Missing base URL"}
        try:
            # First, validate token with /api/me (works for non-admin/user tokens)
            me = requests.get(self._url('/api/me'), headers=self._headers(), timeout=self.timeout)
            if me.status_code == 401:
                return {"ok": False, "message": "Unauthorized: API key invalid"}
            me.raise_for_status()
            me_data = me.json() if me.content else {}
            # Best-effort libraries fetch (may require higher privileges; ignore 401)
            libs_norm = []
            try:
                resp = requests.get(self._url('/api/libraries'), headers=self._headers(), timeout=self.timeout)
                if resp.status_code != 401:
                    resp.raise_for_status()
                    data = resp.json() if resp.content else {}
                    libraries = data if isinstance(data, list) else data.get('libraries') or []
                    for lib in libraries:
                        if isinstance(lib, dict):
                            libs_norm.append({
                                'id': lib.get('id') or lib.get('_id') or lib.get('libraryId') or '',
                                'name': lib.get('name') or lib.get('title') or 'Unnamed',
                                'mediaType': lib.get('mediaType') or lib.get('type') or ''
                            })
            except Exception:
                pass
            return {"ok": True, "message": "Connected", "libraries": libs_norm}
        except Exception as e:
            return {"ok": False, "message": f"Connection failed: {e}"}

    def list_libraries(self) -> Tuple[bool, List[Dict[str, Any]], str]:
        """Return (ok, libraries, message)."""
        res = self.test_connection()
        return res.get('ok', False), list(res.get('libraries') or []), res.get('message', '')

    def list_library_items(self, library_id: str, page: int = 1, size: int = 50) -> Dict[str, Any]:
        """List items in a library. Returns dict with ok, items, total, message.

        Uses ABS endpoint: /api/libraries/{library_id}/items?page=&size=
        """
        if not library_id:
            # Continue with global fallback below
            library_id = ''
        try:
            def _parse_items(data: Any) -> tuple[list, int]:
                if isinstance(data, list):
                    return data, len(data)
                if isinstance(data, dict):
                    items = data.get('results') or data.get('items') or data.get('libraryItems') or []
                    total = data.get('total') or data.get('totalItems') or len(items)
                    return items, int(total) if isinstance(total, (int, float)) else len(items)
                return [], 0

            attempts = []
            # Attempt 1: documented library items endpoint with sort by updatedAt desc and minified objects
            if library_id:
                attempts.append((self._url(f"/api/libraries/{library_id}/items"), {"limit": size, "page": max(0, page-1), "sort": "updatedAt", "desc": 1, "minified": 1}))
            # Attempt 1b: same but using size param name
            if library_id:
                attempts.append((self._url(f"/api/libraries/{library_id}/items"), {"size": size, "page": page, "sort": "updatedAt", "desc": 1, "minified": 1}))
            # Attempt 2: without sort fallback
            if library_id:
                attempts.append((self._url(f"/api/libraries/{library_id}/items"), {"limit": size, "page": max(0, page-1), "minified": 1}))
            # Attempt 3: global items endpoint filtered by library
            if library_id:
                attempts.append((self._url("/api/items"), {"library": library_id, "limit": size, "offset": (page-1)*size, "sort": "updatedAt", "desc": 1}))
                attempts.append((self._url("/api/items"), {"libraryId": library_id, "limit": size, "offset": (page-1)*size, "sort": "updatedAt", "desc": 1}))
            # Final fallback: global items (no filter) for limited tokens
            attempts.append((self._url("/api/items"), {"limit": size, "offset": (page-1)*size, "sort": "updatedAt", "desc": 1}))

            last_err: Optional[str] = None
            for url, params in attempts:
                try:
                    resp = requests.get(url, headers=self._headers(), timeout=self.timeout, params=params)
                    if resp.status_code == 401:
                        return {"ok": False, "message": "Unauthorized", "items": []}
                    resp.raise_for_status()
                    data = resp.json() if resp.content else {}
                    items, total = _parse_items(data)
                    if items or total:
                        return {"ok": True, "items": items, "total": total}
                except Exception as e:  # continue to next attempt
                    last_err = str(e)
                    continue
            # If all attempts failed or yielded no items
            return {"ok": True, "items": [], "total": 0, "message": last_err or "No items returned"}
        except Exception as e:
            return {"ok": False, "message": f"Failed to list items: {e}", "items": []}

    def get_item(self, item_id: str, expanded: bool = True) -> Dict[str, Any]:
        """Get a single item. Always returns shape { ok, item, message? }.

        Tries a few known variants and normalizes the response so callers can
        reliably access 'item'.
        """
        if not item_id:
            return {"ok": False, "message": "Missing item_id"}
        attempts: List[Tuple[str, Optional[Dict[str, Any]]]] = []
        # Primary
        attempts.append((self._url(f"/api/items/{item_id}"), {"expanded": 1} if expanded else None))
        # Include variants used by some ABS builds
        if expanded:
            attempts.append((self._url(f"/api/items/{item_id}"), {"include": "media,metadata"}))
            attempts.append((self._url(f"/api/items/{item_id}"), {"full": 1}))
        # Some deployments expose libraryItems path
        attempts.append((self._url(f"/api/library-items/{item_id}"), None))
        last_err: Optional[str] = None
        try:
            for url, params in attempts:
                try:
                    resp = requests.get(url, headers=self._headers(), timeout=self.timeout, params=params)
                    if resp.status_code == 401:
                        return {"ok": False, "message": "Unauthorized"}
                    resp.raise_for_status()
                    data = resp.json() if resp.content else {}
                    # Normalize item payload
                    item_obj = None
                    if isinstance(data, dict):
                        if 'item' in data and isinstance(data['item'], dict):
                            item_obj = data['item']
                        elif 'libraryItem' in data and isinstance(data['libraryItem'], dict):
                            item_obj = data['libraryItem']
                        elif data.get('id') or data.get('_id'):
                            # Response is the item itself
                            item_obj = data
                    if item_obj is not None:
                        return {"ok": True, "item": item_obj}
                except Exception as e:
                    last_err = str(e)
                    continue
            return {"ok": False, "message": last_err or "Item not found"}
        except Exception as e:
            return {"ok": False, "message": f"Failed to get item: {e}"}

    def build_cover_url(self, cover_path: Optional[str]) -> Optional[str]:
        """Build absolute URL for a given coverPath from ABS item."""
        if not cover_path:
            return None
        if cover_path.startswith('http://') or cover_path.startswith('https://'):
            return cover_path
        if not cover_path.startswith('/'):
            cover_path = '/' + cover_path
        return f"{self.base_url}{cover_path}"

    def get_me(self) -> Dict[str, Any]:
        """Fetch authenticated user info (/api/me). Returns { ok, user?, message? }."""
        try:
            resp = requests.get(self._url("/api/me"), headers=self._headers(), timeout=self.timeout)
            if resp.status_code == 401:
                return {"ok": False, "message": "Unauthorized"}
            resp.raise_for_status()
            data = resp.json() if resp.content else {}
            if isinstance(data, dict):
                return {"ok": True, "user": data}
            return {"ok": False, "message": "Unexpected response"}
        except Exception as e:
            return {"ok": False, "message": str(e)}

    # --- Listening sessions/progress helpers ---
    def list_user_sessions(self, user_id: Optional[str] = None, updated_after: Optional[str] = None, limit: int = 100, page: int = 0) -> Dict[str, Any]:
        """Fetch listening sessions or play progress for a user.

        Tries multiple ABS endpoints since API variants differ. Returns { ok, sessions, total }.
        updated_after: ISO timestamp string if supported by server for incremental syncs.
        """
        try:
            headers = self._headers()
            base_attempts: List[Tuple[str, Dict[str, Any]]] = []
            # Prefer "me" endpoints if user_id not specified
            if not user_id:
                # Official endpoints
                base_attempts.append((self._url("/api/me/listening-sessions"), {"itemsPerPage": limit, "page": page}))
                # Fallbacks (older variants)
                base_attempts.append((self._url("/api/me/sessions"), {"itemsPerPage": limit, "page": page}))
            else:
                base_attempts.append((self._url(f"/api/users/{user_id}/listening-sessions"), {"itemsPerPage": limit, "page": page}))
                # Admin fallback: global sessions filtered by user
                base_attempts.append((self._url("/api/sessions"), {"user": user_id, "itemsPerPage": limit, "page": page}))

            # Add updatedAfter filters if provided
            attempts: List[Tuple[str, Dict[str, Any]]] = []
            for url, params in base_attempts:
                attempts.append((url, params))
                if updated_after:
                    params2 = params.copy()
                    # Try various param names
                    for k in ("updatedAfter", "updated_since", "since"):
                        params2[k] = updated_after
                    attempts.append((url, params2))

            last_err: Optional[str] = None
            def _parse_sessions(data: Any) -> tuple[list, int]:
                if isinstance(data, list):
                    return data, len(data)
                if isinstance(data, dict):
                    items = data.get('results') or data.get('items') or data.get('sessions') or data.get('progress') or []
                    total = data.get('total') or data.get('totalItems') or len(items)
                    return items, int(total) if isinstance(total, (int, float)) else len(items)
                return [], 0

            for url, params in attempts:
                try:
                    resp = requests.get(url, headers=headers, timeout=self.timeout, params=params)
                    if resp.status_code == 401:
                        return {"ok": False, "message": "Unauthorized", "sessions": []}
                    resp.raise_for_status()
                    data = resp.json() if resp.content else {}
                    sessions, total = _parse_sessions(data)
                    if sessions or total:
                        return {"ok": True, "sessions": sessions, "total": total}
                except Exception as e:
                    last_err = str(e)
                    continue
            return {"ok": True, "sessions": [], "total": 0, "message": last_err or "No sessions returned"}
        except Exception as e:
            return {"ok": False, "message": f"Failed to list sessions: {e}", "sessions": []}

    def get_user_progress_for_item(self, item_id: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Fetch a user's progress/bookmark for a specific item."""
        try:
            attempts: List[Tuple[str, Optional[Dict[str, Any]]]] = []
            # Primary according to docs
            attempts.append((self._url(f"/api/me/progress/{item_id}"), None))
            # Fallbacks seen in some clients
            attempts.append((self._url(f"/api/me/item/{item_id}/progress"), None))
            # Admin/user-scoped variants if a user_id is available (some deployments require this)
            if user_id:
                attempts.append((self._url(f"/api/users/{user_id}/progress/{item_id}"), None))
                attempts.append((self._url(f"/api/users/{user_id}/items/{item_id}/progress"), None))
            # Additional generic fallback sometimes exposed
            attempts.append((self._url(f"/api/progress/{item_id}"), None))
            last_err: Optional[str] = None
            for url, params in attempts:
                try:
                    resp = requests.get(url, headers=self._headers(), timeout=self.timeout, params=params)
                    if resp.status_code == 401:
                        return {"ok": False, "message": "Unauthorized"}
                    resp.raise_for_status()
                    data = resp.json() if resp.content else {}
                    if isinstance(data, dict) and data:
                        return {"ok": True, "progress": data}
                except Exception as e:
                    last_err = str(e)
                    continue
            return {"ok": False, "message": last_err or "No progress found"}
        except Exception as e:
            return {"ok": False, "message": f"Failed to get progress: {e}"}


def get_client_from_settings(settings: Dict[str, Any]) -> Optional[AudiobookShelfClient]:
    base_url = (settings or {}).get('base_url') or ''
    api_key = (settings or {}).get('api_key') or ''
    if not base_url:
        return None
    # Return a client even if api_key is missing; callers may override with per-user keys.
    return AudiobookShelfClient(base_url=base_url, api_key=api_key)


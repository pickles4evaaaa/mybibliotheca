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
            # Try libraries endpoint (common and safe)
            resp = requests.get(self._url('/api/libraries'), headers=self._headers(), timeout=self.timeout)
            if resp.status_code == 401:
                return {"ok": False, "message": "Unauthorized: API key invalid"}
            resp.raise_for_status()
            data = resp.json() if resp.content else {}
            libraries = data if isinstance(data, list) else data.get('libraries') or []
            # Normalize items
            libs_norm = []
            for lib in libraries:
                if isinstance(lib, dict):
                    libs_norm.append({
                        'id': lib.get('id') or lib.get('_id') or lib.get('libraryId') or '',
                        'name': lib.get('name') or lib.get('title') or 'Unnamed',
                        'mediaType': lib.get('mediaType') or lib.get('type') or ''
                    })
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
            return {"ok": False, "message": "Missing library_id", "items": []}
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
            # Attempt 1: documented library items endpoint
            attempts.append((self._url(f"/api/libraries/{library_id}/items"), {"page": page, "size": size}))
            # Try limit/offset variant
            attempts.append((self._url(f"/api/libraries/{library_id}/items"), {"limit": size, "offset": (page-1)*size}))
            # Attempt 2: global items endpoint filtered by library
            attempts.append((self._url("/api/items"), {"library": library_id, "page": page, "size": size}))
            attempts.append((self._url("/api/items"), {"library": library_id, "limit": size, "offset": (page-1)*size}))
            # Attempt 3: alternative param name
            attempts.append((self._url("/api/items"), {"libraryId": library_id, "page": page, "size": size}))
            attempts.append((self._url("/api/items"), {"libraryId": library_id, "limit": size, "offset": (page-1)*size}))

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


def get_client_from_settings(settings: Dict[str, Any]) -> Optional[AudiobookShelfClient]:
    base_url = (settings or {}).get('base_url') or ''
    api_key = (settings or {}).get('api_key') or ''
    if not base_url or not api_key:
        return None
    return AudiobookShelfClient(base_url=base_url, api_key=api_key)


#!/usr/bin/env python3
"""
Probe Audiobookshelf listening sessions using local JSON settings.
- Reads data/audiobookshelf_settings.json for base_url
- Uses the first user in data/user_settings/*.json for API key
- Calls /api/me and fetches up to N sessions, printing a compact summary

Run:
  python scripts/probe_abs_listening.py [--limit 100] [--pages 2]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'


def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding='utf-8'))


def iso_from_epoch(val: Any) -> str:
    try:
        x = float(val)
        if x > 10_000_000_000:  # ms
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc).isoformat()
    except Exception:
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace('Z', '+00:00')).isoformat()
            except Exception:
                return val
        return str(val)


def _headers(api_key: str) -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


def _get_json(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Tuple[bool, Any, int, str]:
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        code = r.status_code
        if code == 401:
            return False, None, code, "Unauthorized"
        r.raise_for_status()
        data = r.json() if r.content else None
        return True, data, code, "OK"
    except Exception as e:
        return False, None, 0, str(e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=100)
    ap.add_argument('--pages', type=int, default=2)
    args = ap.parse_args()

    abs_settings = read_json(DATA / 'audiobookshelf_settings.json')
    user_dir = DATA / 'user_settings'
    user_files = sorted(user_dir.glob('*.json'))
    if not user_files:
        print('No user settings in', user_dir)
        return 1
    user = read_json(user_files[0])
    base_url = abs_settings.get('base_url')
    api_key = user.get('abs_api_key') or abs_settings.get('api_key') or ''

    headers = _headers(api_key)
    ok, data, code, msg = _get_json(f"{base_url.rstrip('/')}/api/me", headers)
    user_id = (data or {}).get('id') if isinstance(data, dict) else None
    print('GET /api/me ->', 'ok' if ok else 'FAIL', user_id, msg)

    total_collected = 0
    for page in range(max(1, args.pages)):
        # Try a few endpoints to fetch listening sessions
        sessions = []
        total = 0
        attempts = [
            (f"{base_url.rstrip('/')}/api/me/listening-sessions", {"itemsPerPage": args.limit, "page": page}),
            (f"{base_url.rstrip('/')}/api/me/sessions", {"itemsPerPage": args.limit, "page": page}),
            (f"{base_url.rstrip('/')}/api/sessions", {"itemsPerPage": args.limit, "page": page}),
        ]
        last_msg = ''
        for url, params in attempts:
            ok, payload, _code, _msg = _get_json(url, headers, params=params)
            last_msg = _msg
            if not ok or payload is None:
                continue
            # Normalize
            if isinstance(payload, list):
                sessions = payload
                total = len(sessions)
                break
            if isinstance(payload, dict):
                sessions = payload.get('results') or payload.get('items') or payload.get('sessions') or []
                total = payload.get('total') or payload.get('totalItems') or len(sessions)
                try:
                    total = int(total)
                except Exception:
                    total = len(sessions)
                break
        print(f'Page {page}: ok={(len(sessions)>0)} total={total} returned={len(sessions)} msg={last_msg}')
        if not sessions:
            break
        total_collected += len(sessions)
        for s in sessions[:5]:
            dur = s.get('duration') or s.get('msPlayed') or s.get('secondsListened') or 0
            mins = int(float(dur) / 60) if isinstance(dur, (int, float)) else 0
            print({
                'id': s.get('id'),
                'libraryItemId': s.get('libraryItemId') or s.get('itemId'),
                'bookId': s.get('bookId'),
                'startedAt': iso_from_epoch(s.get('startedAt') or s.get('startTime') or s.get('createdAt')),
                'updatedAt': iso_from_epoch(s.get('updatedAt') or s.get('lastUpdate')),
                'positionMs': s.get('positionMs') or s.get('currentTimeMs') or s.get('position'),
                'msPlayed': s.get('msPlayed') or s.get('timeListenedMs') or s.get('playedMs'),
                'secondsListened': s.get('secondsListened') or s.get('timeListened') or s.get('playedSeconds'),
                'isFinished?': s.get('isFinished') or s.get('finished') or s.get('completed') or s.get('isCompleted') or s.get('isComplete'),
                '~minutesFromDuration': mins,
            })
    print('Total sessions collected (capped by pages*limit):', total_collected)


if __name__ == '__main__':
    raise SystemExit(main())

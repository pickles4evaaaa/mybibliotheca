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
from typing import Any

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

    from app.services.audiobookshelf_service import AudiobookShelfClient
    client = AudiobookShelfClient(base_url, api_key)

    me = client.get_me()
    print('GET /api/me ->', 'ok' if me.get('ok') else 'FAIL', (me.get('user') or {}).get('id'))

    total_collected = 0
    for page in range(max(1, args.pages)):
        res = client.list_user_sessions(limit=args.limit, page=page)
        sessions = res.get('sessions') or []
        print(f'Page {page}: ok={res.get("ok")} total={res.get("total")} returned={len(sessions)} msg={res.get("message")}')
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
                '~minutesFromDuration': mins,
            })
    print('Total sessions collected (capped by pages*limit):', total_collected)


if __name__ == '__main__':
    raise SystemExit(main())

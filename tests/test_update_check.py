import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from flask import Flask, render_template_string
from flask_login import LoginManager

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.admin as admin_mod
from app.utils import update_check as uc


def _rel(tag, pre=False, draft=False, url=None):
    return {"tag_name": tag, "prerelease": pre, "draft": draft,
            "html_url": url or f"https://github.com/pickles4evaaaa/mybibliotheca/releases/tag/{tag}"}


def _resp(status=200, payload=None, etag='"abc"', raises=None):
    r = MagicMock()
    r.status_code = status
    r.headers = {"ETag": etag} if etag else {}
    if raises:
        r.json.side_effect = raises
    else:
        r.json.return_value = payload
    return r


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    uc.reset_cache()
    monkeypatch.delenv("UPDATE_CHECK_ENABLED", raising=False)
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setattr(uc, "get_current_version", lambda: "3.0.0")
    yield
    uc.reset_cache()


@pytest.mark.parametrize("latest,current,expected", [
    ("v3.0.1", "3.0.0", True),
    ("3.0.0", "v3.0.0", False),
    ("2.9.9", "3.0.0", False),
    ("3.1", "3.0.9", True),
    ("3.0", "3.0.0", False),
    ("2.2.1", "2.2.1-beta", True),
    ("2.2.1-beta", "2.2.1", False),
    ("2.2.1-beta2", "2.2.1-beta", True),
    ("2.2.1-rc1", "2.2.1-beta3", True),
    ("v10.0.0", "9.9.9", True),
    ("garbage", "3.0.0", False),
    ("3.0.1", "unknown", False),
])
def test_is_newer(latest, current, expected):
    assert uc.is_newer(latest, current) is expected


def test_prerelease_ignored_for_stable_install(monkeypatch):
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: _resp(payload=[
        _rel("v3.1.0-beta", pre=True), _rel("v3.0.5"), _rel("v9.9.9", draft=True)]))
    s = uc.get_status(background=False)
    assert s["latest"] == "v3.0.5" and s["update_available"]


def test_prerelease_counts_for_prerelease_install(monkeypatch):
    monkeypatch.setattr(uc, "get_current_version", lambda: "3.0.0-beta")
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: _resp(payload=[
        _rel("v3.0.0-beta2", pre=True), _rel("v3.0.0-beta", pre=True)]))
    s = uc.get_status(background=False)
    assert s["latest"] == "v3.0.0-beta2" and s["update_available"]


def test_caching_ttl_and_etag(monkeypatch):
    calls = []

    def fake_get(url, headers=None, **kw):
        calls.append((headers, kw))
        if len(calls) == 1:
            return _resp(payload=[_rel("v3.0.1")])
        return _resp(status=304, etag=None)

    monkeypatch.setattr(uc.requests, "get", fake_get)
    uc.get_status(background=False)
    uc.get_status(background=False)
    assert len(calls) == 1  # served from cache
    assert calls[0][1]["timeout"] <= 5
    assert calls[0][0]["User-Agent"] == "MyBibliotheca/3.0.0"
    assert "If-None-Match" not in calls[0][0]
    # expire TTL -> conditional request, 304 keeps cached result
    s = uc.get_status(background=False, ttl=0)
    assert len(calls) == 2
    assert calls[1][0]["If-None-Match"] == '"abc"'
    assert s["latest"] == "v3.0.1" and s["update_available"]


@pytest.mark.parametrize("resp", [
    _resp(status=403, payload={"message": "rate limit"}),
    _resp(status=429),
    _resp(status=500),
    _resp(raises=ValueError("bad json")),
    _resp(payload={"not": "a list"}),
    _resp(payload=["x", 1, None]),
])
def test_bad_responses_fail_silently(monkeypatch, resp):
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: resp)
    s = uc.get_status(background=False)
    assert s["update_available"] is False and s["latest"] is None


@pytest.mark.parametrize("exc", [requests.Timeout(), requests.ConnectionError(), RuntimeError("x")])
def test_network_errors_fail_silently_and_back_off(monkeypatch, exc):
    calls = []

    def boom(*a, **k):
        calls.append(1)
        raise exc

    monkeypatch.setattr(uc.requests, "get", boom)
    assert uc.get_status(background=False)["update_available"] is False
    uc.get_status(background=False)
    assert len(calls) == 1  # backoff, no hammering


def test_disabled_flag_makes_no_request(monkeypatch):
    monkeypatch.setenv("UPDATE_CHECK_ENABLED", "false")
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: pytest.fail("network used"))
    s = uc.get_status(background=False)
    assert s["update_available"] is False and s["enabled"] is False


def test_background_mode_does_not_block(monkeypatch):
    started = []
    monkeypatch.setattr(uc.threading, "Thread", lambda **kw: started.append(kw) or MagicMock())
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: pytest.fail("blocking fetch"))
    s = uc.get_status(background=True)
    assert started and s["latest"] is None


def test_persisted_cache_survives_restart(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: _resp(payload=[_rel("v3.2.0")]))
    uc.get_status(background=False)
    uc.reset_cache()
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: pytest.fail("should use persisted cache"))
    s = uc.get_status(background=False)
    assert s["latest"] == "v3.2.0"


class _User:
    is_active = True
    is_authenticated = True
    is_anonymous = False

    def __init__(self, admin):
        self.is_admin = admin
        self.id = "admin" if admin else "user"

    def get_id(self):
        return self.id


def _client(user_id):
    app = Flask(__name__)
    app.secret_key = "t"
    lm = LoginManager(app)
    lm.user_loader(lambda uid: _User(uid == "admin"))
    app.add_url_rule("/admin/update-check.json", "admin.update_check_status",
                     admin_mod.update_check_status)
    c = app.test_client()
    if user_id:
        with c.session_transaction() as s:
            s["_user_id"] = user_id
            s["_fresh"] = True
    return app, c


def test_endpoint_admin_gets_payload_with_link(monkeypatch):
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: _resp(payload=[_rel("v3.0.1")]))
    uc.get_status(background=False)  # warm cache
    _, c = _client("admin")
    data = c.get("/admin/update-check.json").get_json()
    assert data["current"] == "3.0.0" and data["latest"] == "v3.0.1"
    assert data["update_available"] is True
    assert data["url"].endswith("/releases/tag/v3.0.1") and data["checked_at"]


def test_endpoint_forbidden_for_non_admin_and_anon(monkeypatch):
    monkeypatch.setattr(uc.requests, "get", lambda *a, **k: pytest.fail("network used"))
    assert _client("user")[1].get("/admin/update-check.json").status_code == 403
    assert _client(None)[1].get("/admin/update-check.json").status_code in (302, 401)


def test_base_template_banner_admin_only():
    tpl = (Path(__file__).resolve().parent.parent / "app/templates/base.html").read_text()
    start = tpl.index("{% if current_user.is_authenticated and current_user.is_admin %}\n    <div class=\"container\">\n      <div id=\"update-banner\"")
    block = tpl[start:tpl.index("{% endif %}", start)]
    assert 'rel="noopener noreferrer"' in block and 'target="_blank"' in block
    assert "updateDismissed:" in block  # per-version dismissal key

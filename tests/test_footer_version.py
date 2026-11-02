import re
from pathlib import Path

from app.utils.update_check import get_current_version

ROOT = Path(__file__).resolve().parents[1]


def test_footer_uses_app_version_not_hardcoded():
    html = (ROOT / "app/templates/base.html").read_text()
    footer = [l for l in html.splitlines() if "footer-version-text\">" in l]
    assert footer, "footer version line missing"
    line = footer[0]
    assert "app_version" in line
    assert not re.search(r"v\d+\.\d+\.\d+", line)


def test_app_version_matches_pyproject():
    import tomllib
    with open(ROOT / "pyproject.toml", "rb") as f:
        expected = tomllib.load(f)["project"]["version"]
    assert get_current_version() == expected
    assert re.fullmatch(r"\d+\.\d+\.\d+", expected)

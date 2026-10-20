#!/usr/bin/env python3
"""Create or complete the local .env file needed by Docker Compose."""

from __future__ import annotations

import secrets
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"
ENV_EXAMPLE = ROOT / ".env.example"
REQUIRED_VALUES = ("SECRET_KEY", "SECURITY_PASSWORD_SALT", "POSTGRES_PASSWORD")


def main() -> int:
    if not ENV_FILE.exists():
        if not ENV_EXAMPLE.exists():
            print("Could not find .env.example. Run this script from a complete MyBibliotheca checkout.")
            return 1
        shutil.copyfile(ENV_EXAMPLE, ENV_FILE)
        print("Created .env from .env.example.")

    lines = ENV_FILE.read_text(encoding="utf-8").splitlines()
    updated: list[str] = []
    found: set[str] = set()
    generated: list[str] = []

    for line in lines:
        key, separator, value = line.partition("=")
        if separator and key in REQUIRED_VALUES:
            found.add(key)
            if not value.strip():
                value = secrets.token_urlsafe(32)
                line = f"{key}={value}"
                generated.append(key)
        updated.append(line)

    for key in REQUIRED_VALUES:
        if key not in found:
            updated.append(f"{key}={secrets.token_urlsafe(32)}")
            generated.append(key)

    ENV_FILE.write_text("\n".join(updated) + "\n", encoding="utf-8")
    try:
        ENV_FILE.chmod(0o600)
    except OSError:
        pass

    if generated:
        print(f"Generated secure values for: {', '.join(generated)}.")
    else:
        print("The required settings in .env already have values; kept them unchanged.")
    print("Next, run: docker compose up -d")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

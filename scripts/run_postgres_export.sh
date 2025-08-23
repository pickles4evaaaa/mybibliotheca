#!/usr/bin/env bash
# Ad-hoc runner for export_postgres_single_user_to_sqlite.py without altering project dependencies.
#
# Strategy:
#   1. If 'uv' is available, use it to run the script with a transient psycopg dependency.
#   2. Else, create (or reuse) a local throwaway virtualenv at .export_venv strictly for this script.
#   3. Install psycopg (binary) only inside that isolated environment.
#   4. Run the export script, forwarding all CLI arguments.
#
# Usage:
#   ./scripts/run_postgres_export.sh --output legacy_snapshot.sqlite
#   (Ensure DATABASE_URL is set in your environment or .env file.)
#
# Cleanup:
#   You can remove the .export_venv directory at any time; it is never required by the main app.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORT_SCRIPT="$SCRIPT_DIR/export_postgres_single_user_to_sqlite.py"

if [ ! -f "$EXPORT_SCRIPT" ]; then
  echo "[ERROR] Cannot locate export script at $EXPORT_SCRIPT" >&2
  exit 1
fi

echo "[INFO] Running ad-hoc Postgres export (no permanent dependency changes)."

if command -v uv >/dev/null 2>&1; then
  echo "[INFO] Detected 'uv'; running with transient dependency injection."
  # --with ensures psycopg is available just-in-time.
  uv run --with psycopg[binary] "$EXPORT_SCRIPT" "$@"
  exit 0
fi

VENV_DIR="${SCRIPT_DIR}/../.export_venv"

if [ ! -d "$VENV_DIR" ]; then
  echo "[INFO] Creating ephemeral virtualenv at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"
  python -m pip install --upgrade pip >/dev/null
  echo "[INFO] Installing psycopg (binary) inside ephemeral venv"
  python -m pip install 'psycopg[binary]>=3.2' >/dev/null
else
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"
  if ! python - <<'PY' >/dev/null 2>&1; then
import importlib, sys
sys.exit(0 if importlib.util.find_spec('psycopg') else 1)
PY
  then
    echo "[INFO] Updating ephemeral venv with psycopg dependency"
    python -m pip install 'psycopg[binary]>=3.2' >/dev/null
  fi
fi

echo "[INFO] Executing export script"
python "$EXPORT_SCRIPT" "$@"

echo "[INFO] Done. Generated file (if successful) is listed above."

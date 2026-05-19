#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if ! ./.venv/bin/python -c "import browser_cookie3" >/dev/null 2>&1; then
  echo "Installing browser-cookie3 into project venv..."
  ./.venv/bin/python -m pip install browser-cookie3
fi

./.venv/bin/python - <<'PY'
from pathlib import Path

from sync.sync_akshare import (
    get_eastmoney_cookie_path,
    refresh_eastmoney_cookie_from_chrome,
)

cookie = refresh_eastmoney_cookie_from_chrome()
cookie_path = get_eastmoney_cookie_path()

print("COOKIE_LEN", len(cookie))
print("COOKIE_PREFIX", cookie[:200])
print("COOKIE_PATH", cookie_path)
print("COOKIE_FILE_EXISTS", cookie_path.exists())

if cookie_path.exists():
    saved = cookie_path.read_text(encoding="utf-8").strip()
    print("SAVED_MATCH", saved == cookie)
    print("SAVED_LEN", len(saved))
PY

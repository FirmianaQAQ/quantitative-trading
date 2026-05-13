#!/bin/zsh

set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "未找到 .venv，请先执行 ./bootstrap.sh"
  exit 1
fi

mkdir -p data logs

exec ./.venv/bin/python run_batch_backtest.py

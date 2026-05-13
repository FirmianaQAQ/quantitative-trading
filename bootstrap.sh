#!/bin/zsh

set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

mkdir -p data logs

./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

echo "环境初始化完成"

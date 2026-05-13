#!/bin/zsh

set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "未找到 .venv，请先执行 ./bootstrap.sh"
  exit 1
fi

mkdir -p data logs

if (( $# > 0 )); then
  exec ./.venv/bin/python sync/sync_akshare.py "$@"
fi

echo ""
echo "请选择同步方式："
echo "1. 输入固定股票代码同步"
echo "2. 拉取全部上证主板普通账户可买股票"
echo ""

read "sync_mode?请输入选项 [1/2]: "

case "$sync_mode" in
  1)
    echo ""
    echo "支持输入一个或多个股票代码，用空格或逗号分隔。"
    echo "示例：600580 或 sh.600580, sz.000725"
    read "raw_codes?请输入股票代码: "

    normalized_codes="${raw_codes//,/ }"
    code_args=(${=normalized_codes})
    if (( ${#code_args[@]} == 0 )); then
      echo "未输入任何股票代码，已取消。"
      exit 1
    fi

    exec ./.venv/bin/python sync/sync_akshare.py "${code_args[@]}"
    ;;
  2)
    exec ./.venv/bin/python sync/sync_akshare.py --all-sh-main
    ;;
  *)
    echo "无效选项：$sync_mode"
    exit 1
    ;;
esac

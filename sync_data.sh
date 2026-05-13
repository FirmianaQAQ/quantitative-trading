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

choose_source_arg() {
  echo "" >&2
  echo "请选择拉取方式：" >&2
  echo "1. 东方财富直连" >&2
  echo "2. Baostock" >&2
  echo "3. Akshare" >&2
  echo "4. Tushare" >&2
  echo "5. 自动（东方财富直连 -> Baostock -> Akshare -> Tushare）" >&2
  echo "" >&2

  read "source_mode?请输入选项 [1/2/3/4/5，默认5]: "
  case "$source_mode" in
    ""|5)
      echo "--source=auto"
      ;;
    1)
      echo "--source=eastmoney"
      ;;
    2)
      echo "--source=baostock"
      ;;
    3)
      echo "--source=akshare"
      ;;
    4)
      echo "--source=tushare"
      ;;
    *)
      echo "无效拉取方式：$source_mode" >&2
      return 1
      ;;
  esac
}

echo ""
echo "请选择同步方式："
echo "1. 输入固定股票代码同步"
echo "2. 拉取默认设置的数据"
echo "3. 拉取全部上证主板普通账户可买股票"
echo ""

read "sync_mode?请输入选项 [1/2/3]: "
source_arg="$(choose_source_arg)" || exit 1

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

    exec ./.venv/bin/python sync/sync_akshare.py "$source_arg" "${code_args[@]}"
    ;;
  2)
    exec ./.venv/bin/python sync/sync_akshare.py "$source_arg"
    ;;
  3)
    exec ./.venv/bin/python sync/sync_akshare.py "$source_arg" --all-sh-main
    ;;
  *)
    echo "无效选项：$sync_mode"
    exit 1
    ;;
esac

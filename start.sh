#!/bin/zsh

set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "未找到 .venv，请先执行 ./bootstrap.sh"
  exit 1
fi

while true; do
  echo
  echo "请选择启动方式："
  echo "  1. GUI 回测"
  echo "  2. 终端批量回测"
  echo "  q. 退出"
  printf "请输入编号: "
  if ! read -r choice; then
    echo
    exit 0
  fi

  case "$choice" in
    1)
      ./start_backtest_gui.sh
      echo
      echo "已返回主菜单"
      ;;
    2)
      ./start_backtest.sh
      echo
      echo "已返回主菜单"
      ;;
    q|Q)
      exit 0
      ;;
    *)
      echo "输入无效，请重新输入"
      ;;
  esac
done

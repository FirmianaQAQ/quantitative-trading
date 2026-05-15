#!/bin/zsh

set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "未找到 .venv，请先执行 ./bootstrap.sh"
  exit 1
fi

while true; do
  echo
  echo "请选择启动方式（直接回车默认 1）："
  echo "  1. GUI 回测 + AI 分析"
  echo "  2. GUI 回测（不启用 AI）"
  echo "  3. 终端批量回测"
  echo "  4. 拉取数据"
  echo "  5. 合并历史回测报告为分享版"
  echo "  q. 退出"
  printf "请输入编号或快捷键 [1/2/3/4/5, ga, g, b, s, m]: "
  if ! read -r choice; then
    echo
    exit 0
  fi

  case "$choice" in
    ""|1|ga|GA)
      if ./start_backtest_gui.sh "--ai=on"; then
        echo
        echo "已返回主菜单"
      else
        child_exit_code=$?
        if [[ $child_exit_code -eq 86 ]]; then
          exit 0
        fi
        exit "$child_exit_code"
      fi
      ;;
    2|g|G)
      if ./start_backtest_gui.sh "--ai=off"; then
        echo
        echo "已返回主菜单"
      else
        child_exit_code=$?
        if [[ $child_exit_code -eq 86 ]]; then
          exit 0
        fi
        exit "$child_exit_code"
      fi
      ;;
    3|b|B)
      if ./start_backtest.sh; then
        echo
        echo "已返回主菜单"
      else
        child_exit_code=$?
        if [[ $child_exit_code -eq 86 ]]; then
          exit 0
        fi
        exit "$child_exit_code"
      fi
      ;;
    4|s|S)
      if ./sync_data.sh; then
        echo
        echo "已返回主菜单"
      else
        child_exit_code=$?
        if [[ $child_exit_code -eq 86 ]]; then
          exit 0
        fi
        exit "$child_exit_code"
      fi
      ;;
    5|m|M)
      echo
      echo "请输入回测 HTML 路径，例如：logs/backtest/simple_ma_backtest_v2-sz.000725.html"
      printf "回测报告路径: "
      if ! read -r backtest_report_path; then
        echo
        exit 0
      fi
      if [[ -z "$backtest_report_path" ]]; then
        echo "未输入回测报告路径，已返回主菜单"
        continue
      fi

      echo
      echo "AI 报告路径可留空，留空则自动去 logs/llm_analysis/ 查找同名文件"
      printf "AI 报告路径（可留空）: "
      if ! read -r ai_report_path; then
        echo
        exit 0
      fi

      echo
      echo "输出路径可留空，留空则默认生成 *-share.html"
      printf "输出路径（可留空）: "
      if ! read -r share_output_path; then
        echo
        exit 0
      fi

      merge_args=("$backtest_report_path")
      if [[ -n "$ai_report_path" ]]; then
        merge_args+=("--ai-report" "$ai_report_path")
      fi
      if [[ -n "$share_output_path" ]]; then
        merge_args+=("--output" "$share_output_path")
      fi

      if ./.venv/bin/python merge_backtest_ai_html.py "${merge_args[@]}"; then
        echo
        echo "已返回主菜单"
      else
        child_exit_code=$?
        if [[ $child_exit_code -eq 86 ]]; then
          exit 0
        fi
        exit "$child_exit_code"
      fi
      ;;
    q|Q)
      exit 0
      ;;
    *)
      echo "输入无效，请重新输入"
      ;;
  esac
done

from __future__ import annotations

import argparse
from pathlib import Path

from utils.backtest_report import merge_backtest_html_with_ai_report


PROJECT_ROOT = Path(__file__).resolve().parent


def _guess_ai_report_path(backtest_report_path: Path) -> Path:
    llm_report_dir = PROJECT_ROOT / "logs" / "llm_analysis"
    primary = llm_report_dir / backtest_report_path.name
    if primary.exists():
        return primary

    failed = primary.with_suffix(".failed.html")
    if failed.exists():
        return failed

    raise FileNotFoundError(
        "未找到同名 AI 报告，请显式传入 --ai-report。"
        f" 已尝试: {primary} / {failed}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="把已有回测 HTML 和 AI 分析 HTML 合并成单文件分享版报告。"
    )
    parser.add_argument(
        "backtest_report",
        help="回测 HTML 路径，例如 logs/backtest/simple_ma_backtest_v2-sz.000725.html",
    )
    parser.add_argument(
        "--ai-report",
        dest="ai_report",
        help="AI 分析 HTML 路径；不传则自动按同名文件去 logs/llm_analysis/ 下查找。",
    )
    parser.add_argument(
        "--output",
        help="输出路径；不传则默认生成到原目录，文件名加 -share 后缀。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    backtest_report_path = Path(args.backtest_report).expanduser().resolve()
    ai_report_path = (
        Path(args.ai_report).expanduser().resolve()
        if args.ai_report
        else _guess_ai_report_path(backtest_report_path)
    )
    merged_path = merge_backtest_html_with_ai_report(
        backtest_html_path=str(backtest_report_path),
        ai_report_path=str(ai_report_path),
        output_path=args.output,
    )
    print(f"合并完成: {merged_path}")


if __name__ == "__main__":
    main()

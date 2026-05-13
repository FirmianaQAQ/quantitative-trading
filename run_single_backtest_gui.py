from __future__ import annotations

import io
import os
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path

from backtest.strategy_registry import (
    StrategySpec,
    get_required_codes,
    get_selection_label,
    get_strategy_spec,
    group_strategy_specs,
    list_strategy_specs,
    supports_manual_code_input,
)
from utils.project_utils import get_daily_csv_path, load_daily_data


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STOCK_NAMES = {
    "sz.000100": "TCL科技",
    "sz.000725": "京东方A",
    "sz.001308": "康冠科技",
    "sz.002594": "比亚迪",
    "sh.000001": "上证指数",
    "sz.000001": "平安银行/深市代码样例",
}
EXIT_MENU_VALUE = "__exit__"
MANUAL_MENU_VALUE = "__manual__"
RECOMMEND_MENU_VALUE = "__recommend__"


def normalize_code(raw_code: str) -> str:
    code = raw_code.strip().lower()
    if not code:
        raise ValueError("股票代码不能为空")
    if "." in code:
        return code
    if len(code) != 6 or not code.isdigit():
        raise ValueError("股票代码格式错误，应为 sz.000725 或 000725")
    prefix = "sh" if code.startswith("6") else "sz"
    return f"{prefix}.{code}"


def get_stock_label(code: str) -> str:
    return DEFAULT_STOCK_NAMES.get(code, code)


def collect_stock_candidates(spec: StrategySpec) -> list[str]:
    codes: set[str] = set()
    code = spec.config.get("code")
    if code:
        codes.add(code)

    for item in spec.test_cases:
        candidate_code = item.get("code")
        if candidate_code:
            codes.add(candidate_code)

    if not supports_manual_code_input(spec):
        return sorted(codes)

    daily_dir = PROJECT_ROOT / "data" / "daily"
    if daily_dir.exists():
        for path in daily_dir.glob("*.csv"):
            stem = path.stem
            if "_" not in stem:
                continue
            code_part, _adjust_part = stem.rsplit("_", 1)
            if "." in code_part:
                codes.add(code_part)

    return sorted(codes)


def prompt_strategy_menu() -> str:
    grouped_specs = group_strategy_specs()
    while True:
        print()
        print("请选择策略大类：")
        for index, (family_name, _family_specs) in enumerate(grouped_specs, start=1):
            print(f"  {index}. {family_name}")
        print("  q. 退出")

        choice = input("请输入编号: ").strip()
        if choice.lower() == "q":
            return EXIT_MENU_VALUE
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue

        selected_index = int(choice)
        if not 1 <= selected_index <= len(grouped_specs):
            print("编号超出范围，请重新输入")
            continue

        family_name, family_specs = grouped_specs[selected_index - 1]
        while True:
            print()
            print(f"已选择大类：{family_name}")
            print("请选择具体策略版本：")
            for index, spec in enumerate(family_specs, start=1):
                print(f"  {index}. {spec.display_name} ({spec.strategy_id})")
            print("  b. 返回上一级")
            print("  q. 退出")

            sub_choice = input("请输入编号: ").strip()
            lower_sub_choice = sub_choice.lower()
            if lower_sub_choice == "q":
                return EXIT_MENU_VALUE
            if lower_sub_choice == "b":
                break
            if not sub_choice.isdigit():
                print("输入无效，请输入数字编号")
                continue

            sub_index = int(sub_choice)
            if 1 <= sub_index <= len(family_specs):
                return family_specs[sub_index - 1].strategy_id
            print("编号超出范围，请重新输入")


def prompt_stock_menu(spec: StrategySpec) -> str:
    codes = collect_stock_candidates(spec)
    allow_manual = supports_manual_code_input(spec)
    while True:
        print()
        print("请选择股票：")
        for index, code in enumerate(codes, start=1):
            print(f"  {index}. {code}  {get_selection_label(spec, code)}")
        print("  r. 系统推荐前5")
        if allow_manual:
            print("  0. 手动输入")
        print("  q. 退出")

        choice = input("请输入编号: ").strip()
        lower_choice = choice.lower()
        if lower_choice == "q":
            return EXIT_MENU_VALUE
        if lower_choice == "r":
            return RECOMMEND_MENU_VALUE
        if allow_manual and choice == "0":
            return MANUAL_MENU_VALUE
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue

        selected_index = int(choice)
        if 1 <= selected_index <= len(codes):
            return codes[selected_index - 1]
        print("编号超出范围，请重新输入")


def choose_stock_interactively(spec: StrategySpec) -> str:
    while True:
        selected = prompt_stock_menu(spec)
        if selected == EXIT_MENU_VALUE:
            raise SystemExit(0)
        if selected == MANUAL_MENU_VALUE:
            while True:
                raw_code = input("请输入股票代码，例如 sz.000725 或 000725: ").strip()
                if raw_code.lower() == "q":
                    raise SystemExit(0)
                try:
                    return normalize_code(raw_code)
                except ValueError as exc:
                    print(str(exc))
        if selected == RECOMMEND_MENU_VALUE:
            recommended_code = choose_recommended_stock(spec)
            if recommended_code is not None:
                return recommended_code
            continue
        return selected


def parse_cli_args() -> tuple[str | None, str | None]:
    if len(sys.argv) <= 1:
        return None, None

    strategy_ids = {spec.strategy_id for spec in list_strategy_specs()}
    first_arg = sys.argv[1].strip()
    if first_arg in strategy_ids:
        strategy_id = first_arg
        stock_code = sys.argv[2].strip() if len(sys.argv) >= 3 else None
        return strategy_id, stock_code

    return None, normalize_code(first_arg)


def choose_strategy_spec(cli_strategy_id: str | None) -> StrategySpec:
    if cli_strategy_id:
        return get_strategy_spec(cli_strategy_id)

    selected = prompt_strategy_menu()
    if selected == EXIT_MENU_VALUE:
        raise SystemExit(0)
    return get_strategy_spec(selected)


def resolve_config(spec: StrategySpec, stock_code: str | None) -> dict:
    config = dict(spec.config)
    if stock_code:
        config["code"] = stock_code

    config["plot"] = True
    config["print_log"] = False

    benchmark_code = config.get("benchmark_code", "")
    if benchmark_code:
        benchmark_csv = get_daily_csv_path(benchmark_code, config["adjust_flag"])
        if not benchmark_csv.exists():
            print(f"未找到基准数据 {benchmark_csv}，本次 GUI 报告将不展示基准曲线")
            config["benchmark_code"] = ""

    return config


def sync_single_stock_data(code: str) -> bool:
    print(f"未找到 {code} 的本地数据，正在尝试自动同步...")
    command = [
        str(PROJECT_ROOT / ".venv" / "bin" / "python"),
        str(PROJECT_ROOT / "sync" / "sync_akshare.py"),
        code,
    ]
    result = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    if result.returncode == 0:
        print(f"{code} 数据同步完成")
        return True
    print(f"{code} 数据同步失败，退出码={result.returncode}")
    return False


def build_report_path(config: dict) -> Path:
    report_dir = PROJECT_ROOT / config["report_dir"]
    filename = f"{config['report_name']}-{config['code']}.html"
    return report_dir / filename


def try_open_report(report_path: Path) -> None:
    if os.getenv("OPEN_GUI", "1") != "1":
        return
    if sys.platform != "darwin":
        return
    if not report_path.exists():
        return

    open_commands = [
        ["open", "-a", "Google Chrome", str(report_path)],
        ["open", "-a", "Safari", str(report_path)],
        ["open", str(report_path)],
    ]
    for command in open_commands:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return

    print(f"自动打开 GUI 失败，请手动打开报告: {report_path}")


def build_recommendation_candidates(spec: StrategySpec) -> list[str]:
    available_codes: list[str] = []
    for code in collect_stock_candidates(spec):
        required_codes = get_required_codes(spec, code)
        if all(
            get_daily_csv_path(required_code, spec.config["adjust_flag"]).exists()
            for required_code in required_codes
        ):
            available_codes.append(code)
    return available_codes


def evaluate_stock_for_recommendation(spec: StrategySpec, code: str) -> dict | None:
    config = dict(spec.config)
    config.update(
        {
            "code": code,
            "plot": False,
            "print_log": False,
            "benchmark_code": "",
        }
    )
    try:
        spec.validate_config(config)
        df = (
            load_daily_data(config["code"], config["adjust_flag"])
            if get_required_codes(spec, code) == [code]
            else None
        )
        with redirect_stdout(io.StringIO()):
            summary = spec.run_backtest(config, df)
        return summary
    except Exception:
        return None


def choose_recommended_stock(spec: StrategySpec) -> str | None:
    candidate_codes = build_recommendation_candidates(spec)
    if not candidate_codes:
        print("当前没有可用于推荐的本地股票数据，请先同步数据")
        return None

    print()
    print(f"系统正在基于 {spec.display_name} 评估本地股票，请稍等...")
    ranked_results: list[tuple[str, dict]] = []
    for code in candidate_codes:
        summary = evaluate_stock_for_recommendation(spec, code)
        if summary is None or summary.get("annual_return_pct") is None:
            continue
        ranked_results.append((code, summary))

    if not ranked_results:
        print("系统推荐失败，当前没有可用的回测结果")
        return None

    ranked_results.sort(
        key=lambda item: (
            item[1]["annual_return_pct"],
            item[1]["sharpe_ratio"] if item[1]["sharpe_ratio"] is not None else float("-inf"),
            -(item[1]["max_drawdown_pct"] if item[1]["max_drawdown_pct"] is not None else float("inf")),
        ),
        reverse=True,
    )
    top_results = ranked_results[:5]

    while True:
        print()
        print("系统推荐前5：")
        for index, (code, summary) in enumerate(top_results, start=1):
            annual_text = f"{summary['annual_return_pct']:.2f}%"
            drawdown_text = (
                f"{summary['max_drawdown_pct']:.2f}%"
                if summary["max_drawdown_pct"] is not None
                else "N/A"
            )
            sharpe_text = (
                f"{summary['sharpe_ratio']:.2f}"
                if summary["sharpe_ratio"] is not None
                else "N/A"
            )
            print(
                f"  {index}. {code}  {get_selection_label(spec, code)}  "
                f"年化={annual_text}  回撤={drawdown_text}  夏普={sharpe_text}"
            )
        print("  q. 返回上一级")

        choice = input("请输入编号: ").strip()
        if choice.lower() == "q":
            return None
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue
        selected_index = int(choice)
        if 1 <= selected_index <= len(top_results):
            return top_results[selected_index - 1][0]
        print("编号超出范围，请重新输入")


def validate_required_data_files(spec: StrategySpec, config: dict) -> None:
    missing_files: list[Path] = []
    for code in get_required_codes(spec, config["code"]):
        csv_path = get_daily_csv_path(code, config["adjust_flag"])
        if csv_path.exists():
            continue
        sync_single_stock_data(code)
        if not csv_path.exists():
            missing_files.append(csv_path)

    if not missing_files:
        return

    missing_text = "\n".join(str(path) for path in missing_files)
    raise FileNotFoundError(
        "缺少 GUI 回测所需日线数据文件，且自动同步失败。请手动执行 ./sync_data.sh 同步数据：\n"
        f"{missing_text}"
    )


def main() -> None:
    cli_strategy_id, cli_stock_code = parse_cli_args()
    spec = choose_strategy_spec(cli_strategy_id)
    stock_code = (
        normalize_code(cli_stock_code)
        if cli_stock_code and supports_manual_code_input(spec)
        else cli_stock_code
    ) or choose_stock_interactively(spec)
    config = resolve_config(spec, stock_code)

    print(f"已选择策略: {spec.display_name} ({spec.strategy_id})")
    print(f"已选择股票: {config['code']} {get_selection_label(spec, config['code'])}")

    validate_required_data_files(spec, config)
    spec.validate_config(config)
    df = (
        load_daily_data(config["code"], config["adjust_flag"])
        if get_required_codes(spec, config["code"]) == [config["code"]]
        else None
    )
    spec.run_backtest(config, df)

    report_path = build_report_path(config)
    print(f"GUI 回测报告: {report_path}")
    try_open_report(report_path)


if __name__ == "__main__":
    main()

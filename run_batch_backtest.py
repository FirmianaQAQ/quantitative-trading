from __future__ import annotations

import sys
from pathlib import Path

from batch_backtest import batch_backtest
from backtest.strategy_registry import (
    StrategySpec,
    get_default_strategy_spec,
    get_required_codes,
    get_strategy_spec,
    group_strategy_specs,
    list_strategy_specs,
)
from utils.project_utils import get_daily_csv_path


BACK_MENU_VALUE = "__back__"
EXIT_ALL_MENU_VALUE = "__exit_all__"
FULL_EXIT_CODE = 86


def prompt_strategy_menu() -> str:
    strategy_specs = list_strategy_specs()
    grouped_specs = group_strategy_specs()
    while True:
        print()
        print("请选择批量回测策略：")
        index = 1
        for family_name, family_specs in grouped_specs:
            print(f"  [{family_name}]")
            for spec in family_specs:
                print(f"  {index}. {spec.display_name} ({spec.brief_description})")
                index += 1
        print("  b. 返回上一级")
        print("  q. 退出")
        choice = input("请输入编号: ").strip()

        lower_choice = choice.lower()
        if lower_choice == "b":
            return BACK_MENU_VALUE
        if lower_choice == "q":
            return EXIT_ALL_MENU_VALUE
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue

        selected_index = int(choice)
        if 1 <= selected_index <= len(strategy_specs):
            return strategy_specs[selected_index - 1].strategy_id
        print("编号超出范围，请重新输入")


def parse_strategy_id() -> str:
    if len(sys.argv) >= 2:
        return sys.argv[1].strip()

    selected = prompt_strategy_menu()
    if selected == BACK_MENU_VALUE:
        raise SystemExit(0)
    if selected == EXIT_ALL_MENU_VALUE:
        raise SystemExit(FULL_EXIT_CODE)
    return selected


def validate_required_data_files(spec: StrategySpec) -> None:
    required_codes: set[str] = set()
    for item in spec.test_cases:
        code = item.get("code")
        if not code:
            continue
        required_codes.update(get_required_codes(spec, str(code)))
    missing_files: list[Path] = []
    for code in sorted(required_codes):
        csv_path = get_daily_csv_path(code, spec.config["adjust_flag"])
        if not csv_path.exists():
            missing_files.append(csv_path)

    if not missing_files:
        return

    missing_text = "\n".join(str(path) for path in missing_files)
    raise FileNotFoundError(
        "缺少回测所需日线数据文件，请先执行 ./sync_data.sh 同步数据：\n"
        f"{missing_text}"
    )


def main() -> None:
    strategy_id = parse_strategy_id()
    spec = (
        get_default_strategy_spec()
        if not strategy_id
        else get_strategy_spec(strategy_id)
    )
    validate_required_data_files(spec)
    batch_backtest(spec.strategy_id)


if __name__ == "__main__":
    main()

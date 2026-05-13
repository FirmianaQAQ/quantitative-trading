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
    list_family_strategy_specs,
    list_strategy_specs,
)
from utils.project_utils import get_daily_csv_path


BACK_MENU_VALUE = "__back__"
EXIT_ALL_MENU_VALUE = "__exit_all__"
FULL_EXIT_CODE = 86
DEFAULT_CASH = 100000.0
MULTI_VERSION_FAMILY_IDS = {"simple_ma_backtest"}


def parse_cash_input(raw_value: str) -> float:
    text = raw_value.strip().lower()
    if not text:
        return DEFAULT_CASH

    normalized = text.replace(",", "").replace(" ", "")
    if normalized.endswith("万"):
        normalized = normalized[:-1] + "w"
    if normalized.endswith("w"):
        value = float(normalized[:-1]) * 10000
    else:
        value = float(normalized)

    if value <= 0:
        raise ValueError("初始资金必须大于 0")
    return round(value, 2)


def is_multi_version_family(family_specs: list[StrategySpec]) -> bool:
    return bool(family_specs) and family_specs[0].family_id in MULTI_VERSION_FAMILY_IDS


def prompt_strategy_menu() -> str:
    grouped_specs = group_strategy_specs()
    menu_items: list[str] = []
    while True:
        print()
        print("请选择批量回测策略：")
        index = 1
        for family_name, family_specs in grouped_specs:
            print(f"  [{family_name}]")
            if is_multi_version_family(family_specs):
                print(f"  {index}. 联跑全部版本（生成全部普通双均线版本结果）")
                menu_items.append(family_specs[0].family_id)
                index += 1
                continue
            for spec in family_specs:
                print(f"  {index}. {spec.display_name} ({spec.brief_description})")
                menu_items.append(spec.strategy_id)
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
        if 1 <= selected_index <= len(menu_items):
            return menu_items[selected_index - 1]
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


def validate_strategy_selection(spec: StrategySpec) -> None:
    if spec.family_id not in MULTI_VERSION_FAMILY_IDS:
        validate_required_data_files(spec)
        return

    for family_spec in list_family_strategy_specs(spec.family_id):
        validate_required_data_files(family_spec)


def prompt_initial_cash() -> float:
    while True:
        raw_value = input("请输入初始资金，直接回车默认 10w: ").strip()
        if raw_value.lower() == "q":
            raise SystemExit(FULL_EXIT_CODE)
        try:
            return parse_cash_input(raw_value)
        except ValueError as exc:
            print(f"输入无效: {exc}")


def main() -> None:
    strategy_id = parse_strategy_id()
    spec = (
        get_default_strategy_spec()
        if not strategy_id
        else get_strategy_spec(strategy_id)
    )
    cash = prompt_initial_cash()
    validate_strategy_selection(spec)
    batch_backtest(spec.strategy_id, cash)


if __name__ == "__main__":
    main()

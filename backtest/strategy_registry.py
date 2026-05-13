from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Callable
import re


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    module_name: str
    display_name: str
    family_id: str
    family_display_name: str
    version_number: int
    config: dict[str, Any]
    test_cases: list[dict[str, Any]]
    run_backtest: Callable[[dict[str, Any], Any], dict[str, Any]]
    validate_config: Callable[[dict[str, Any]], None]


STRATEGY_FAMILY_DISPLAY_NAMES = {
    "simple_ma_backtest": "普通双均线",
    "pair_trade_backtest": "套利配对交易",
}
STRATEGY_FAMILY_ORDER = {
    "simple_ma_backtest": 0,
    "pair_trade_backtest": 1,
}


def _iter_candidate_strategy_ids() -> list[str]:
    backtest_dir = Path(__file__).resolve().parent
    return sorted(
        path.stem
        for path in backtest_dir.glob("*_backtest*.py")
        if path.is_file()
        and not path.stem.startswith("_")
        and path.stem != "strategy_registry"
    )


def _parse_strategy_family(strategy_id: str) -> tuple[str, int]:
    match = re.match(r"^(.*)_v(\d+)$", strategy_id)
    if match is None:
        return strategy_id, 0
    return match.group(1), int(match.group(2))


def _build_strategy_spec(module: ModuleType, strategy_id: str) -> StrategySpec | None:
    required_attrs = ("CONFIG", "TEST_CASES", "run_backtest", "validate_config")
    if any(not hasattr(module, attr) for attr in required_attrs):
        return None

    config = dict(getattr(module, "CONFIG"))
    test_cases = list(getattr(module, "TEST_CASES"))
    family_id, version_number = _parse_strategy_family(strategy_id)
    return StrategySpec(
        strategy_id=strategy_id,
        module_name=module.__name__,
        display_name=str(config.get("strategy_name", strategy_id)),
        family_id=family_id,
        family_display_name=STRATEGY_FAMILY_DISPLAY_NAMES.get(
            family_id,
            str(config.get("strategy_name", family_id)),
        ),
        version_number=version_number,
        config=config,
        test_cases=test_cases,
        run_backtest=getattr(module, "run_backtest"),
        validate_config=getattr(module, "validate_config"),
    )


@lru_cache(maxsize=1)
def list_strategy_specs() -> tuple[StrategySpec, ...]:
    specs: list[StrategySpec] = []
    for strategy_id in _iter_candidate_strategy_ids():
        module = import_module(f"backtest.{strategy_id}")
        spec = _build_strategy_spec(module, strategy_id)
        if spec is not None:
            specs.append(spec)

    if not specs:
        raise RuntimeError("未找到可用的回测策略")
    specs.sort(
        key=lambda spec: (
            STRATEGY_FAMILY_ORDER.get(spec.family_id, 999),
            spec.family_display_name,
            spec.version_number,
            spec.strategy_id,
        )
    )
    return tuple(specs)


def get_strategy_spec(strategy_id: str) -> StrategySpec:
    for spec in list_strategy_specs():
        if spec.strategy_id == strategy_id:
            return spec
    available = ", ".join(spec.strategy_id for spec in list_strategy_specs())
    raise ValueError(f"未知策略版本: {strategy_id}，可选值: {available}")


def get_default_strategy_spec() -> StrategySpec:
    specs = list_strategy_specs()
    for spec in specs:
        if spec.strategy_id == "simple_ma_backtest":
            return spec
    return specs[0]


def group_strategy_specs() -> list[tuple[str, list[StrategySpec]]]:
    grouped: list[tuple[str, list[StrategySpec]]] = []
    for spec in list_strategy_specs():
        if grouped and grouped[-1][0] == spec.family_display_name:
            grouped[-1][1].append(spec)
            continue
        grouped.append((spec.family_display_name, [spec]))
    return grouped


def find_test_case(spec: StrategySpec, code: str) -> dict[str, Any] | None:
    for item in spec.test_cases:
        if item.get("code") == code:
            return item
    return None


def get_selection_label(spec: StrategySpec, code: str) -> str:
    item = find_test_case(spec, code)
    if item is not None and item.get("label"):
        return str(item["label"])
    return code


def get_required_codes(spec: StrategySpec, code: str) -> list[str]:
    item = find_test_case(spec, code)
    if item is None:
        return [code]

    required_codes = item.get("required_codes")
    if not required_codes:
        return [code]
    return [str(item_code) for item_code in required_codes]


def supports_manual_code_input(spec: StrategySpec) -> bool:
    stock_pattern = re.compile(r"^(sh|sz)\.\d{6}$")
    candidate_codes = [item.get("code") for item in spec.test_cases if item.get("code")]
    if not candidate_codes:
        return True

    for code in candidate_codes:
        if not stock_pattern.match(str(code)):
            return False
        if get_required_codes(spec, str(code)) != [str(code)]:
            return False
    return True

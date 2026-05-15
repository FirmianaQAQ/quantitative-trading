from __future__ import annotations

from typing import Any

from analysis.config import is_llm_analysis_requested
from analysis.service import maybe_generate_batch_analysis
from backtest.strategy_registry import (
    get_default_strategy_spec,
    get_required_codes,
    list_family_strategy_specs,
    get_strategy_spec,
)
from utils.project_utils import load_daily_data


MULTI_VERSION_FAMILY_IDS = {"simple_ma_backtest"}


def _run_single_batch_strategy(
    strategy_id: str,
    cash: float | None = None,
) -> list[dict[str, Any]]:
    spec = get_strategy_spec(strategy_id)
    if not spec.test_cases:
        raise ValueError(f"策略 {spec.strategy_id} 没有配置 TEST_CASES，无法批量回测")

    base_config = dict(spec.config)
    base_config["plot"] = False
    base_config["print_log"] = False
    base_config["enable_llm_analysis"] = is_llm_analysis_requested()
    if cash is not None:
        base_config["cash"] = cash

    spec.validate_config(
        {
            **base_config,
            "code": spec.test_cases[0]["code"],
        }
    )
    print(f"已选择策略: {spec.display_name} ({spec.strategy_id})")
    print(f"初始资金: {base_config['cash']:.2f}")

    batch_results: list[dict[str, Any]] = []
    for test_case in spec.test_cases:
        config = dict(base_config)
        config["code"] = test_case["code"]
        df = (
            load_daily_data(config["code"], config["adjust_flag"])
            if get_required_codes(spec, config["code"]) == [config["code"]]
            else None
        )
        summary = spec.run_backtest(config, df)
        batch_results.append(
            {
                **summary,
                "code": config["code"],
                "strategy_id": spec.strategy_id,
                "strategy_name": spec.display_name,
                "enable_llm_analysis": bool(config.get("enable_llm_analysis")),
            }
        )
    maybe_generate_batch_analysis(
        strategy_id=spec.strategy_id,
        strategy_name=spec.display_name,
        batch_results=batch_results,
    )
    return batch_results


def _run_multi_version_batch(spec_family_id: str, cash: float | None = None) -> None:
    family_specs = list_family_strategy_specs(spec_family_id)
    if not family_specs:
        raise ValueError(f"策略家族 {spec_family_id} 没有可用版本")

    print(f"已选择策略家族: {family_specs[0].family_display_name}（联跑全部版本）")
    for index, family_spec in enumerate(family_specs, start=1):
        print()
        print(f"===== 批量联跑版本 {index}/{len(family_specs)}：{family_spec.display_name} =====")
        _run_single_batch_strategy(family_spec.strategy_id, cash)


def batch_backtest(strategy_id: str | None = None, cash: float | None = None) -> None:
    """
    批量测试多只股票，每只股票单独跑一次回测。
    strategy_id 为空时，默认跑基础版 simple_ma_backtest。
    """
    spec = (
        get_strategy_spec(strategy_id)
        if strategy_id is not None
        else get_default_strategy_spec()
    )
    if spec.family_id in MULTI_VERSION_FAMILY_IDS:
        _run_multi_version_batch(spec.family_id, cash)
        return
    _run_single_batch_strategy(spec.strategy_id, cash)


if __name__ == "__main__":
    batch_backtest()

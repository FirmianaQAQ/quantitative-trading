from __future__ import annotations

from backtest.strategy_registry import (
    get_default_strategy_spec,
    get_required_codes,
    get_strategy_spec,
)
from utils.project_utils import load_daily_data


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
    if not spec.test_cases:
        raise ValueError(f"策略 {spec.strategy_id} 没有配置 TEST_CASES，无法批量回测")

    base_config = dict(spec.config)
    base_config["plot"] = False
    base_config["print_log"] = False
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

    for test_case in spec.test_cases:
        config = dict(base_config)
        config["code"] = test_case["code"]
        df = (
            load_daily_data(config["code"], config["adjust_flag"])
            if get_required_codes(spec, config["code"]) == [config["code"]]
            else None
        )
        spec.run_backtest(config, df)


if __name__ == "__main__":
    batch_backtest()

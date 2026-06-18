from __future__ import annotations

import copy
from typing import Any

from backtest.backtest_v1 import (
    CONFIG as BASE_CONFIG,
    run_backtest,
    run_optimization,
    validate_config,
)
from utils.default_stocks import build_default_stock_test_cases
from utils.project_utils import load_daily_data


STRATEGY_ID = "MAS"
STRATEGY_FAMILY_ID = "MAS"

CONFIG: dict[str, Any] = copy.deepcopy(BASE_CONFIG)
CONFIG.update(
    {
        "strategy_name": "MAS",
        "strategy_brief": "均线策略",
        "report_name": "mas_backtest",
    }
)

TEST_CASES = build_default_stock_test_cases()


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])

    if config.get("optimize"):
        run_optimization(config, df)
        return

    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

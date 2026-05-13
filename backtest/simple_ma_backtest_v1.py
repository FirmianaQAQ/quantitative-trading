from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest import (
    CONFIG as BASE_CONFIG,
    TEST_CASES,
    run_backtest,
    validate_config,
)
from utils.project_utils import load_daily_data


CONFIG: dict[str, Any] = dict(BASE_CONFIG)
CONFIG.update(
    {
        # V1 直接基于当前默认标的做参数强化，目标是优先提升年化收益率。
        "buy_cash_ratio": 0.99,
        "buy_trigger_multiplier": 1.06,
        "buy_trigger_window": 8,
        "buy_rise_window": 4,
        "buy_rise_days_required": 1,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.08,
        "fast": 4,
        "slow": 238,
        # 当前仓库默认没有上证指数基准数据，留空避免生成报告时直接失败。
        "benchmark_code": "",
        "report_name": "simple_ma_backtest_v1",
        "strategy_name": "普通双均线V1",
        "strategy_brief": "强化收益版",
    }
)


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

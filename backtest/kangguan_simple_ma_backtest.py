from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest_v2 import CONFIG as V2_BASE_CONFIG, validate_config as validate_v2_config
from backtest.specialized_ma_support import run_specialized_backtest, validate_specialized_code
from utils.project_utils import load_daily_data


KANGGUAN_CODE = "sz.001308"
STRATEGY_FAMILY_ID = "specialized_ma_backtest"

CONFIG: dict[str, Any] = dict(V2_BASE_CONFIG)
CONFIG.update(
    {
        "code": KANGGUAN_CODE,
        "fast": 10,
        "slow": 238,
        "buy_trigger_multiplier": 1.03,
        "buy_trigger_window": 8,
        "buy_rise_window": 5,
        "buy_rise_days_required": 1,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.08,
        "breakout_power_threshold": 0.55,
        "breakout_buy_limit_multiplier": 1.0,
        "benchmark_code": "",
        "report_name": "kangguan_simple_ma_backtest",
        "strategy_name": "康冠科技双均线专版",
        "strategy_brief": "sz.001308历史节奏增强版",
    }
)

TEST_CASES = [{"code": KANGGUAN_CODE, "label": "康冠科技（专版）", "required_codes": [KANGGUAN_CODE]}]


def validate_config(config: dict[str, Any]) -> None:
    validate_v2_config(config)
    validate_specialized_code(config, KANGGUAN_CODE, "康冠科技")


def run_backtest(config: dict[str, Any], df):
    return run_specialized_backtest(config, df)


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

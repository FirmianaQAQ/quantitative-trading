from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.extended_strategies._simple_ma_core import CONFIG as V2_BASE_CONFIG, validate_config as validate_v2_config
from backtest.extended_strategies.specialized_ma_support import run_specialized_backtest, validate_specialized_code
from utils.project_utils import load_daily_data


CMB_CODE = "sh.600036"
STRATEGY_FAMILY_ID = "specialized_ma_backtest"

CONFIG: dict[str, Any] = dict(V2_BASE_CONFIG)
CONFIG.update(
    {
        "code": CMB_CODE,
        "fast": 8,
        "slow": 250,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 8,
        "buy_rise_window": 5,
        "buy_rise_days_required": 1,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.08,
        "breakout_power_threshold": 0.55,
        "breakout_buy_limit_multiplier": 1.0,
        "benchmark_code": "",
        "report_name": "cmb_simple_ma_backtest",
        "strategy_name": "招商银行双均线专版",
        "strategy_brief": "sh.600036历史节奏增强版",
    }
)

TEST_CASES = [{"code": CMB_CODE, "label": "招商银行（专版）", "required_codes": [CMB_CODE]}]


def validate_config(config: dict[str, Any]) -> None:
    validate_v2_config(config)
    validate_specialized_code(config, CMB_CODE, "招商银行")


def run_backtest(config: dict[str, Any], df):
    return run_specialized_backtest(config, df)


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

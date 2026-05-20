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


GUIGUAN_CODE = "sh.600236"
STRATEGY_FAMILY_ID = "specialized_ma_backtest"

CONFIG: dict[str, Any] = dict(V2_BASE_CONFIG)
CONFIG.update(
    {
        "code": GUIGUAN_CODE,
        "fast": 8,
        "slow": 220,
        "buy_trigger_multiplier": 1.03,
        "buy_trigger_window": 8,
        "buy_rise_window": 5,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.08,
        "breakout_power_threshold": 0.55,
        "breakout_buy_limit_multiplier": 1.0,
        "benchmark_code": "",
        "report_name": "guiguan_simple_ma_backtest",
        "strategy_name": "桂冠电力双均线专版",
        "strategy_brief": "sh.600236历史节奏增强版",
    }
)

TEST_CASES = [{"code": GUIGUAN_CODE, "label": "桂冠电力（专版）", "required_codes": [GUIGUAN_CODE]}]


def validate_config(config: dict[str, Any]) -> None:
    validate_v2_config(config)
    validate_specialized_code(config, GUIGUAN_CODE, "桂冠电力")


def run_backtest(config: dict[str, Any], df):
    return run_specialized_backtest(config, df)


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

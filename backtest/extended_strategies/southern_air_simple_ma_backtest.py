from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.extended_strategies._simple_ma_core import CONFIG as V2_BASE_CONFIG, validate_config as validate_v2_config
from backtest.extended_strategies.specialized_ma_support import (
    build_specialized_config,
    run_specialized_backtest,
    validate_specialized_code,
)
from utils.project_utils import load_daily_data


SOUTHERN_AIR_CODE = "sh.600029"
STRATEGY_FAMILY_ID = "specialized_ma_backtest"

CONFIG: dict[str, Any] = build_specialized_config(
    V2_BASE_CONFIG,
    code=SOUTHERN_AIR_CODE,
    report_name="southern_air_simple_ma_backtest",
    strategy_name="南方航空双均线专版",
    strategy_brief=SOUTHERN_AIR_CODE,
)

TEST_CASES = [{"code": SOUTHERN_AIR_CODE, "label": "南方航空（专版）", "required_codes": [SOUTHERN_AIR_CODE]}]


def validate_config(config: dict[str, Any]) -> None:
    validate_v2_config(config)
    validate_specialized_code(config, SOUTHERN_AIR_CODE, "南方航空")


def run_backtest(config: dict[str, Any], df):
    return run_specialized_backtest(config, df)


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.service import maybe_generate_single_stock_analysis
from backtest.simple_ma_backtest import (
    add_analyzers,
    build_data_feed,
    create_cerebro,
    generate_html_report,
    print_summary,
)
from backtest.simple_ma_backtest_v2 import (
    CONFIG as V2_BASE_CONFIG,
    SimpleMovingAverageStrategyV2,
    validate_config as validate_v2_config,
)
from utils.backtest_report_builder import (
    build_backtest_report_data,
    summarize_result,
)
from utils.project_utils import load_daily_data


BOE_CODE = "sz.000725"

CONFIG: dict[str, Any] = dict(V2_BASE_CONFIG)
CONFIG.update(
    {
        "code": BOE_CODE,
        "fast": 8,
        "slow": 250,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 8,
        "buy_rise_window": 4,
        "buy_rise_days_required": 1,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.1,
        "breakout_power_threshold": 0.6,
        "breakout_buy_limit_multiplier": 1.0,
        "benchmark_code": "",
        "report_name": "boe_simple_ma_backtest",
        "strategy_name": "京东方双均线专版",
        "strategy_brief": "sz.000725稳健增强版",
    }
)

TEST_CASES = [
    {
        "code": BOE_CODE,
        "label": "京东方A（专版）",
        "required_codes": [BOE_CODE],
    }
]


def validate_config(config: dict[str, Any]) -> None:
    validate_v2_config(config)
    if str(config.get("code", "")).strip().lower() != BOE_CODE:
        raise ValueError(f"京东方专版仅支持 {BOE_CODE}")


def run_backtest(config: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    cerebro = create_cerebro(config)
    cerebro.addstrategy(
        SimpleMovingAverageStrategyV2,
        fast_period=config["fast"],
        slow_period=config["slow"],
        printlog=config["print_log"],
        p=config,
        df=df,
    )
    cerebro.adddata(build_data_feed(df, config["data_from_date"], config["to_date"]))
    add_analyzers(cerebro)

    initial_value = cerebro.broker.getvalue()
    print(f"开始回测: 股票={config['code']}，初始资金={initial_value:.2f}")
    strategies = cerebro.run()
    strategy = strategies[0]
    summary = summarize_result(strategy, initial_value)
    summary.update(
        {
            "fast_period": strategy.params.fast_period,
            "slow_period": strategy.params.slow_period,
        }
    )
    print_summary(summary)

    ai_report_path = maybe_generate_single_stock_analysis(config, summary, df)

    if config["plot"]:
        report_data = build_backtest_report_data(
            strategy,
            config,
            [config["fast"], config["slow"]],
        )
        generate_html_report(
            report_data,
            config,
            getattr(strategy, "log_messages", []),
            ai_report_path=ai_report_path,
        )

    return summary


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

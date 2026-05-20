from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.service import maybe_generate_single_stock_analysis
from backtest.base_backtest import (
    add_analyzers,
    build_data_feed,
    create_cerebro,
    generate_html_report,
    print_summary,
)
from backtest.extended_strategies._simple_ma_core import (
    CONFIG as V2_BASE_CONFIG,
    SimpleMovingAverageStrategyV2,
    validate_config as validate_v2_config,
)
from utils.backtest_report_builder import (
    build_backtest_report_data,
    build_empty_entry_timing_plan,
    build_next_trade_plan,
    build_next_trade_plan_by_position,
    summarize_result,
)
from utils.project_utils import load_daily_data


DATANG_CODE = "sh.601991"
STRATEGY_FAMILY_ID = "specialized_ma_backtest"

CONFIG: dict[str, Any] = dict(V2_BASE_CONFIG)
CONFIG.update(
    {
        "code": DATANG_CODE,
        # 基于本地 2018-01-02 到 2026-05-20 历史扫描，
        # 8/238 在收益、回撤、交易次数之间更均衡。
        "fast": 8,
        "slow": 238,
        # 大唐发电整体波动不算大，但阶段拉升明显，
        # 因此把触发阈值略收紧，确认窗口拉到 5 日 2 涨。
        "buy_trigger_multiplier": 1.03,
        "buy_trigger_window": 8,
        "buy_rise_window": 5,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.08,
        "breakout_power_threshold": 0.60,
        "breakout_buy_limit_multiplier": 1.0,
        "benchmark_code": "",
        "report_name": "datang_simple_ma_backtest",
        "strategy_name": "大唐发电双均线专版",
        "strategy_brief": "sh.601991历史节奏增强版",
    }
)

TEST_CASES = [
    {
        "code": DATANG_CODE,
        "label": "大唐发电（专版）",
        "required_codes": [DATANG_CODE],
    }
]


def validate_config(config: dict[str, Any]) -> None:
    validate_v2_config(config)
    if str(config.get("code", "")).strip().lower() != DATANG_CODE:
        raise ValueError(f"大唐发电专版仅支持 {DATANG_CODE}")


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
    next_trade_plan_by_position = build_next_trade_plan_by_position(
        source_df=df,
        config=config,
        ma_periods=[config["fast"], config["slow"]],
    )
    if next_trade_plan_by_position.get("empty"):
        next_trade_plan_by_position["empty"]["entry_timing"] = build_empty_entry_timing_plan(
            source_df=df,
            config=config,
            ma_periods=[config["fast"], config["slow"]],
        )
    summary.update(
        {
            "fast_period": strategy.params.fast_period,
            "slow_period": strategy.params.slow_period,
            "next_trade_plan": build_next_trade_plan(
                source_df=df,
                config=config,
                ma_periods=[config["fast"], config["slow"]],
            ),
            "next_trade_plan_by_position": next_trade_plan_by_position,
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

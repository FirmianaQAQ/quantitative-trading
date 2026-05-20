from __future__ import annotations

from typing import Any

import pandas as pd

from analysis.service import maybe_generate_single_stock_analysis
from backtest.simple_ma_backtest import (
    add_analyzers,
    build_data_feed,
    create_cerebro,
    generate_html_report,
    print_summary,
)
from backtest.simple_ma_backtest_v2 import SimpleMovingAverageStrategyV2
from utils.backtest_report_builder import (
    build_backtest_report_data,
    build_empty_entry_timing_plan,
    build_next_trade_plan,
    build_next_trade_plan_by_position,
    summarize_result,
)


def validate_specialized_code(
    config: dict[str, Any],
    expected_code: str,
    display_name: str,
) -> None:
    if str(config.get("code", "")).strip().lower() != expected_code:
        raise ValueError(f"{display_name}专版仅支持 {expected_code}")


def run_specialized_backtest(config: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
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

from __future__ import annotations

from typing import Any

import pandas as pd

from analysis.service import maybe_generate_single_stock_analysis
from backtest.base_backtest import (
    add_analyzers,
    build_data_feed,
    create_cerebro,
    generate_html_report,
    print_summary,
)
from backtest.extended_strategies._simple_ma_core import SimpleMovingAverageStrategyV2
from utils.backtest_report_builder import (
    build_backtest_report_data,
    build_empty_entry_timing_plan,
    build_next_trade_plan,
    build_next_trade_plan_by_position,
    summarize_result,
)

SPECIALIZED_BASE_TEMPLATE: dict[str, Any] = {
    "fast": 10,
    "slow": 250,
    "buy_trigger_multiplier": 1.03,
    "buy_trigger_window": 8,
    "buy_rise_window": 4,
    "buy_rise_days_required": 1,
    "sell_trigger_multiplier": 0.93,
    "stop_loss_pct": 0.08,
    "breakout_power_threshold": 0.55,
    "breakout_buy_limit_multiplier": 1.0,
    "benchmark_code": "",
}

SPECIALIZED_PROFILE_OVERRIDES: dict[str, dict[str, Any]] = {
    # 面板双雄：低价大盘、趋势持续性一般，优先低吸，不追强突破。
    "sz.000100": {
        "fast": 10,
        "slow": 250,
        "buy_trigger_multiplier": 1.03,
        "buy_trigger_window": 8,
        "sell_trigger_multiplier": 0.93,
        "stop_loss_pct": 0.08,
        "breakout_power_threshold": 0.55,
        "breakout_buy_limit_multiplier": 1.00,
    },
    "sz.000725": {
        "fast": 9,
        "slow": 233,
        "buy_trigger_multiplier": 1.025,
        "buy_trigger_window": 9,
        "sell_trigger_multiplier": 0.94,
        "stop_loss_pct": 0.075,
        "breakout_power_threshold": 0.52,
        "breakout_buy_limit_multiplier": 0.995,
    },
    # 高波动成长：允许更强趋势确认，但不放松风险控制。
    "sz.002594": {
        "fast": 12,
        "slow": 220,
        "buy_trigger_multiplier": 1.045,
        "buy_trigger_window": 7,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.96,
        "stop_loss_pct": 0.09,
        "breakout_power_threshold": 0.68,
        "breakout_buy_limit_multiplier": 1.03,
    },
    "sz.001308": {
        "fast": 11,
        "slow": 210,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 7,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.085,
        "breakout_power_threshold": 0.64,
        "breakout_buy_limit_multiplier": 1.02,
    },
    "sz.002624": {
        "fast": 12,
        "slow": 205,
        "buy_trigger_multiplier": 1.05,
        "buy_trigger_window": 6,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.96,
        "stop_loss_pct": 0.10,
        "breakout_power_threshold": 0.70,
        "breakout_buy_limit_multiplier": 1.035,
    },
    # 稳健权重/公用事业：更看重低位触发与回撤约束。
    "sh.600036": {
        "fast": 8,
        "slow": 260,
        "buy_trigger_multiplier": 1.02,
        "buy_trigger_window": 10,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.06,
        "breakout_power_threshold": 0.48,
        "breakout_buy_limit_multiplier": 0.99,
    },
    "sh.600236": {
        "fast": 8,
        "slow": 245,
        "buy_trigger_multiplier": 1.02,
        "buy_trigger_window": 10,
        "sell_trigger_multiplier": 0.955,
        "stop_loss_pct": 0.06,
        "breakout_power_threshold": 0.46,
        "breakout_buy_limit_multiplier": 0.99,
    },
    "sh.600406": {
        "fast": 9,
        "slow": 240,
        "buy_trigger_multiplier": 1.025,
        "buy_trigger_window": 9,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.065,
        "breakout_power_threshold": 0.50,
        "breakout_buy_limit_multiplier": 0.995,
    },
    "sh.600690": {
        "fast": 9,
        "slow": 238,
        "buy_trigger_multiplier": 1.03,
        "buy_trigger_window": 8,
        "sell_trigger_multiplier": 0.945,
        "stop_loss_pct": 0.07,
        "breakout_power_threshold": 0.54,
        "breakout_buy_limit_multiplier": 1.00,
    },
    # 强周期/高弹性：给更宽止损和更强突破确认。
    "sh.601991": {
        "fast": 11,
        "slow": 225,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 7,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.965,
        "stop_loss_pct": 0.095,
        "breakout_power_threshold": 0.66,
        "breakout_buy_limit_multiplier": 1.03,
    },
    "sh.600029": {
        "fast": 11,
        "slow": 230,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 7,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.965,
        "stop_loss_pct": 0.09,
        "breakout_power_threshold": 0.64,
        "breakout_buy_limit_multiplier": 1.025,
    },
    "sh.600580": {
        "fast": 10,
        "slow": 220,
        "buy_trigger_multiplier": 1.035,
        "buy_trigger_window": 8,
        "sell_trigger_multiplier": 0.955,
        "stop_loss_pct": 0.08,
        "breakout_power_threshold": 0.60,
        "breakout_buy_limit_multiplier": 1.015,
    },
    "sh.605006": {
        "fast": 11,
        "slow": 215,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 7,
        "buy_rise_days_required": 2,
        "sell_trigger_multiplier": 0.96,
        "stop_loss_pct": 0.09,
        "breakout_power_threshold": 0.63,
        "breakout_buy_limit_multiplier": 1.02,
    },
}


def resolve_specialized_profile(code: str) -> dict[str, Any]:
    normalized_code = str(code or "").strip().lower()
    return dict(SPECIALIZED_PROFILE_OVERRIDES.get(normalized_code, {}))


def build_specialized_config(
    base_config: dict[str, Any],
    *,
    code: str,
    report_name: str,
    strategy_name: str,
    strategy_brief: str,
) -> dict[str, Any]:
    config = dict(base_config)
    config.update(SPECIALIZED_BASE_TEMPLATE)
    config.update(resolve_specialized_profile(code))
    config.update(
        {
            "code": code,
            "report_name": report_name,
            "strategy_name": strategy_name,
            "strategy_brief": strategy_brief,
        }
    )
    return config


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

from __future__ import annotations

"""布林线补丁：用上轨突破确认买点，用下轨跌破触发风险退出。"""

from typing import Any

import backtrader as bt

from backtest.patches._indicator_patch_utils import (
    cfg_float,
    cfg_int,
    has_history,
    is_rising,
    line_value,
    signal_price,
    submit_patch_exit,
)


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    period = cfg_int(strategy, "boll_period", 20)
    devfactor = cfg_float(strategy, "boll_devfactor", 2.0)
    boll = bt.indicators.BollingerBands(
        strategy.data.close,
        period=period,
        devfactor=devfactor,
    )
    strategy.boll_mid = boll.mid
    strategy.boll_top = boll.top
    strategy.boll_bot = boll.bot


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    decision = evaluate_sell_signal(strategy)
    if decision["should_sell"]:
        submit_patch_exit(
            strategy,
            patch_name="boll",
            reason=decision["reason"],
        )


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    decision = evaluate_buy_signal(strategy)
    return {
        "allow": decision["allow"],
        "reason": decision["reason"],
    }


def evaluate_buy_signal(strategy: Any) -> dict[str, Any]:
    period = cfg_int(strategy, "boll_period", 20)
    if not has_history(strategy, period + 2):
        return {"allow": False, "reason": "布林线预热未完成"}

    close_price = signal_price(strategy, "close")
    upper_band_prev = line_value(strategy.boll_top, ago=-1)
    middle_band = line_value(strategy.boll_mid)
    upper_slope_up = is_rising(strategy.boll_top, lookback=1)
    width_pct = _bandwidth_pct(strategy)
    min_width_pct = cfg_float(strategy, "boll_bandwidth_min_pct", 4.0)
    breakout_buffer_pct = cfg_float(strategy, "boll_breakout_buffer_pct", 0.0)
    breakout_price = upper_band_prev * (1 + breakout_buffer_pct)

    if close_price <= breakout_price:
        return {
            "allow": False,
            "reason": (
                f"未突破布林线上轨 close={close_price:.2f}"
                f" breakout={breakout_price:.2f}"
            ),
        }
    if close_price <= middle_band:
        return {
            "allow": False,
            "reason": (
                f"价格仍在布林中轨下方 close={close_price:.2f}"
                f" mid={middle_band:.2f}"
            ),
        }
    if width_pct < min_width_pct:
        return {
            "allow": False,
            "reason": (
                f"布林带宽过窄 width={width_pct:.2f}%"
                f" threshold={min_width_pct:.2f}%"
            ),
        }
    if not upper_slope_up:
        return {
            "allow": False,
            "reason": "布林线上轨未上行，突破质量不足",
        }
    return {
        "allow": True,
        "reason": (
            f"布林线突破成立 close={close_price:.2f}"
            f" upper={upper_band_prev:.2f}"
            f" width={width_pct:.2f}%"
        ),
    }


def evaluate_sell_signal(strategy: Any) -> dict[str, Any]:
    period = cfg_int(strategy, "boll_period", 20)
    if not strategy.position or not has_history(strategy, period + 2):
        return {"should_sell": False, "reason": None}

    close_price = signal_price(strategy, "close")
    lower_band_prev = line_value(strategy.boll_bot, ago=-1)
    middle_band = line_value(strategy.boll_mid)
    exit_buffer_pct = cfg_float(strategy, "boll_sell_break_buffer_pct", 0.0)
    lower_trigger = lower_band_prev * (1 - exit_buffer_pct)
    if close_price < lower_trigger:
        return {
            "should_sell": True,
            "reason": (
                f"跌破布林线下轨 close={close_price:.2f}"
                f" lower={lower_trigger:.2f}"
            ),
        }
    if close_price < middle_band and _bandwidth_pct(strategy) < cfg_float(
        strategy,
        "boll_exit_bandwidth_floor_pct",
        3.0,
    ):
        return {
            "should_sell": True,
            "reason": (
                f"跌回中轨且波动收缩 close={close_price:.2f}"
                f" mid={middle_band:.2f}"
            ),
        }
    return {"should_sell": False, "reason": None}


def _bandwidth_pct(strategy: Any) -> float:
    middle_band = max(line_value(strategy.boll_mid), 1e-9)
    return (line_value(strategy.boll_top) - line_value(strategy.boll_bot)) / middle_band * 100

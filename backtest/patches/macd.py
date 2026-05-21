from __future__ import annotations

"""MACD补丁：用金叉和柱体扩张确认动量，用死叉和顶背离处理退出。"""

from typing import Any

import backtrader as bt

from backtest.patches._indicator_patch_utils import (
    cfg_float,
    cfg_int,
    crosses_down,
    crosses_up,
    detect_bearish_divergence,
    detect_bullish_divergence,
    has_history,
    is_falling,
    is_rising,
    line_value,
    submit_patch_exit,
)


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    fast_period = cfg_int(strategy, "macd_fast_period", 12)
    slow_period = cfg_int(strategy, "macd_slow_period", 26)
    signal_period = cfg_int(strategy, "macd_signal_period", 9)
    macd_indicator = bt.indicators.MACD(
        strategy.data.close,
        period_me1=fast_period,
        period_me2=slow_period,
        period_signal=signal_period,
    )
    strategy.macd_line = macd_indicator.macd
    strategy.macd_signal = macd_indicator.signal
    strategy.macd_hist = macd_indicator.macd - macd_indicator.signal


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    decision = evaluate_sell_signal(strategy)
    if decision["should_sell"]:
        submit_patch_exit(
            strategy,
            patch_name="macd",
            reason=decision["reason"],
        )


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    decision = evaluate_buy_signal(strategy)
    return {
        "allow": decision["allow"],
        "reason": decision["reason"],
    }


def evaluate_buy_signal(strategy: Any) -> dict[str, Any]:
    required = cfg_int(strategy, "macd_slow_period", 26) + cfg_int(
        strategy,
        "macd_signal_period",
        9,
    ) + 3
    if not has_history(strategy, required):
        return {"allow": False, "reason": "MACD预热未完成"}

    macd_value = line_value(strategy.macd_line)
    signal_value = line_value(strategy.macd_signal)
    hist_value = line_value(strategy.macd_hist)
    hist_floor = cfg_float(strategy, "macd_hist_floor", 0.0)
    cross_up = crosses_up(strategy.macd_line, strategy.macd_signal)
    bullish_divergence = detect_bullish_divergence(
        strategy.data.close,
        strategy.macd_line,
        window=cfg_int(strategy, "macd_divergence_window", 6),
    )
    if macd_value <= signal_value:
        return {
            "allow": False,
            "reason": (
                f"MACD未金叉 macd={macd_value:.4f}"
                f" signal={signal_value:.4f}"
            ),
        }
    if hist_value <= hist_floor:
        return {
            "allow": False,
            "reason": (
                f"MACD柱体未翻红 hist={hist_value:.4f}"
                f" floor={hist_floor:.4f}"
            ),
        }
    if not is_rising(strategy.macd_hist, lookback=1) and not bullish_divergence:
        return {
            "allow": False,
            "reason": "MACD柱体未扩张，动量确认不足",
        }
    reason_parts = []
    if cross_up:
        reason_parts.append("MACD金叉")
    if bullish_divergence:
        reason_parts.append("底背离修复")
    if not reason_parts:
        reason_parts.append("MACD位于多头区间")
    reason_parts.append(f"hist={hist_value:.4f}")
    return {"allow": True, "reason": "，".join(reason_parts)}


def evaluate_sell_signal(strategy: Any) -> dict[str, Any]:
    required = cfg_int(strategy, "macd_slow_period", 26) + cfg_int(
        strategy,
        "macd_signal_period",
        9,
    ) + 3
    if not strategy.position or not has_history(strategy, required):
        return {"should_sell": False, "reason": None}

    macd_value = line_value(strategy.macd_line)
    signal_value = line_value(strategy.macd_signal)
    hist_value = line_value(strategy.macd_hist)
    cross_down = crosses_down(strategy.macd_line, strategy.macd_signal)
    bearish_divergence = detect_bearish_divergence(
        strategy.data.close,
        strategy.macd_line,
        window=cfg_int(strategy, "macd_divergence_window", 6),
    )
    if cross_down:
        return {
            "should_sell": True,
            "reason": (
                f"MACD死叉 macd={macd_value:.4f}"
                f" signal={signal_value:.4f}"
            ),
        }
    if hist_value < 0 and is_falling(strategy.macd_hist, lookback=1):
        return {
            "should_sell": True,
            "reason": f"MACD绿柱伸长 hist={hist_value:.4f}",
        }
    if bearish_divergence:
        return {
            "should_sell": True,
            "reason": "MACD顶背离，趋势转弱",
        }
    return {"should_sell": False, "reason": None}

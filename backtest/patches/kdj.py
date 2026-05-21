from __future__ import annotations

"""KDJ补丁：用低位金叉和J值修复确认短线反弹，用高位死叉和背离处理退出。"""

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
    line_value,
    signal_price,
    submit_patch_exit,
)


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    period = cfg_int(strategy, "kdj_period", 9)
    k_period = cfg_int(strategy, "kdj_k_period", 3)
    d_period = cfg_int(strategy, "kdj_d_period", 3)
    stochastic = bt.indicators.StochasticFull(
        strategy.data,
        period=period,
        period_dfast=k_period,
        period_dslow=d_period,
    )
    strategy.kdj_k = stochastic.percK
    strategy.kdj_d = stochastic.percD
    strategy.kdj_j = stochastic.percK * 3.0 - stochastic.percD * 2.0


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    decision = evaluate_sell_signal(strategy)
    if decision["should_sell"]:
        submit_patch_exit(
            strategy,
            patch_name="kdj",
            reason=decision["reason"],
        )


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    decision = evaluate_buy_signal(strategy)
    return {
        "allow": decision["allow"],
        "reason": decision["reason"],
    }


def evaluate_buy_signal(strategy: Any) -> dict[str, Any]:
    required = cfg_int(strategy, "kdj_period", 9) + cfg_int(
        strategy,
        "kdj_d_period",
        3,
    ) + 3
    if not has_history(strategy, required):
        return {"allow": False, "reason": "KDJ预热未完成"}

    k_value = line_value(strategy.kdj_k)
    d_value = line_value(strategy.kdj_d)
    j_value = line_value(strategy.kdj_j)
    oversold = cfg_float(strategy, "kdj_oversold", 20.0)
    j_rebound_floor = cfg_float(strategy, "kdj_j_rebound_floor", 0.0)
    cross_up = crosses_up(strategy.kdj_k, strategy.kdj_d)
    bullish_divergence = detect_bullish_divergence(
        strategy.data.close,
        strategy.kdj_k,
        window=cfg_int(strategy, "kdj_divergence_window", 5),
    )
    if not cross_up and not bullish_divergence:
        return {
            "allow": False,
            "reason": (
                f"KDJ未金叉 k={k_value:.2f}"
                f" d={d_value:.2f}"
            ),
        }
    if max(k_value, d_value) > oversold and j_value > j_rebound_floor:
        return {
            "allow": False,
            "reason": (
                f"KDJ不在低位区 k={k_value:.2f}"
                f" d={d_value:.2f}"
                f" j={j_value:.2f}"
            ),
        }
    return {
        "allow": True,
        "reason": (
            ("KDJ低位金叉" if cross_up else "KDJ底背离")
            + f"，j={j_value:.2f}"
        ),
    }


def evaluate_sell_signal(strategy: Any) -> dict[str, Any]:
    required = cfg_int(strategy, "kdj_period", 9) + cfg_int(
        strategy,
        "kdj_d_period",
        3,
    ) + 3
    if not strategy.position or not has_history(strategy, required):
        return {"should_sell": False, "reason": None}

    k_value = line_value(strategy.kdj_k)
    d_value = line_value(strategy.kdj_d)
    j_value = line_value(strategy.kdj_j)
    overbought = cfg_float(strategy, "kdj_overbought", 80.0)
    j_overbought = cfg_float(strategy, "kdj_j_overbought", 100.0)
    cross_down = crosses_down(strategy.kdj_k, strategy.kdj_d)
    bearish_divergence = detect_bearish_divergence(
        strategy.data.close,
        strategy.kdj_k,
        window=cfg_int(strategy, "kdj_divergence_window", 5),
    )
    if cross_down and min(k_value, d_value) >= overbought:
        return {
            "should_sell": True,
            "reason": f"KDJ高位死叉 k={k_value:.2f} d={d_value:.2f}",
        }
    if j_value >= j_overbought and signal_price(strategy, "close") < signal_price(
        strategy,
        "close",
        ago=-1,
    ):
        return {
            "should_sell": True,
            "reason": f"J值超买回落 j={j_value:.2f}",
        }
    if bearish_divergence:
        return {
            "should_sell": True,
            "reason": "KDJ顶背离，短线转弱",
        }
    return {"should_sell": False, "reason": None}

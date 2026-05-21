from __future__ import annotations

"""BMK组合补丁：按 BOLL -> MACD -> KDJ 顺序做共振确认，只在多指标一致时放大信号。"""

from typing import Any

from backtest.patches import boll, kdj, macd
from backtest.patches._indicator_patch_utils import submit_patch_exit


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    boll.setup_patch(strategy, context)
    macd.setup_patch(strategy, context)
    kdj.setup_patch(strategy, context)


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    sell_signals = _collect_sell_signals(strategy)
    strong_signal = next(
        (
            signal for signal in sell_signals
            if signal["should_sell"] and signal["name"] == "boll"
        ),
        None,
    )
    if strong_signal is not None:
        submit_patch_exit(
            strategy,
            patch_name="bmk",
            reason=f"组合补丁强卖出 | {strong_signal['name']}={strong_signal['reason']}",
        )
        return

    active_signals = [signal for signal in sell_signals if signal["should_sell"]]
    if len(active_signals) >= 2:
        reason = "；".join(
            f"{signal['name']}={signal['reason']}" for signal in active_signals
        )
        submit_patch_exit(
            strategy,
            patch_name="bmk",
            reason=f"组合补丁双重转弱 | {reason}",
        )


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    buy_signals = [
        ("boll", boll.evaluate_buy_signal(strategy)),
        ("macd", macd.evaluate_buy_signal(strategy)),
        ("kdj", kdj.evaluate_buy_signal(strategy)),
    ]
    failed = [
        f"{name}={signal.get('reason') or '未通过'}"
        for name, signal in buy_signals
        if not signal.get("allow")
    ]
    if failed:
        return {
            "allow": False,
            "reason": "；".join(failed),
        }
    passed = [
        f"{name}={signal.get('reason') or '通过'}"
        for name, signal in buy_signals
    ]
    return {
        "allow": True,
        "reason": "；".join(passed),
    }


def _collect_sell_signals(strategy: Any) -> list[dict[str, Any]]:
    return [
        {"name": "boll", **boll.evaluate_sell_signal(strategy)},
        {"name": "macd", **macd.evaluate_sell_signal(strategy)},
        {"name": "kdj", **kdj.evaluate_sell_signal(strategy)},
    ]

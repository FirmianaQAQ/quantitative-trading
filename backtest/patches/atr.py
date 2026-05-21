from __future__ import annotations

"""ATR补丁：在基础策略上叠加突破过滤、ATR仓位、分层加仓与趋势失效退出。"""

import math
from types import MethodType
from typing import Any

import backtrader as bt

from backtest.patches._indicator_patch_utils import (
    cfg_float,
    cfg_int,
    has_history,
    line_value,
    signal_price,
    submit_patch_exit,
)


STATE_KEY = "_atr_patch_state"


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    state = {
        "patch_name": context.get("patch_name", "atr"),
        "observed_position_size": 0.0,
        "unit_size": 0,
        "units": 0,
        "entry_base_price": None,
        "last_add_price": None,
        "next_add_price": None,
        "stop_price": None,
    }
    setattr(strategy, STATE_KEY, state)

    atr_period = cfg_int(strategy, "atr_period", 14)
    breakout_period = cfg_int(strategy, "atr_breakout_period", 20)
    exit_period = cfg_int(strategy, "atr_exit_period", 10)

    strategy.atr_patch_atr = bt.indicators.AverageTrueRange(
        strategy.data,
        period=atr_period,
    )
    strategy.atr_patch_breakout_high = bt.indicators.Highest(
        strategy.data.high,
        period=breakout_period,
    )
    strategy.atr_patch_exit_low = bt.indicators.Lowest(
        strategy.data.low,
        period=exit_period,
    )

    original_calculate_buy_size = strategy.calculate_buy_size
    state["original_calculate_buy_size"] = original_calculate_buy_size

    def _patched_calculate_buy_size(self: Any) -> int:
        return _calculate_buy_size_with_atr(self)

    strategy.calculate_buy_size = MethodType(_patched_calculate_buy_size, strategy)


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    if not _has_min_history(strategy):
        return

    _sync_holding_state(strategy)

    if getattr(strategy, "order", None) is not None or not _has_effective_position(
        strategy
    ):
        return

    exit_reason = _build_exit_reason(strategy)
    if exit_reason is not None:
        submit_patch_exit(strategy, patch_name="atr", reason=exit_reason)
        return

    state = _get_state(strategy)
    next_add_price = state.get("next_add_price")
    if next_add_price is None:
        return

    current_close = _current_close(strategy)
    max_units = cfg_int(strategy, "atr_max_units", 3)
    if current_close < float(next_add_price) or int(state.get("units", 0)) >= max_units:
        return

    size = _calculate_add_on_size(strategy)
    if size <= 0:
        strategy.log("ATR补丁放弃加仓 | 原因=ATR风控后可买数量为0")
        return

    strategy.log(
        "ATR补丁触发加仓"
        f" | 当前价={current_close:.2f}"
        f" | 下一加仓价={float(next_add_price):.2f}"
        f" | 数量={size}"
    )
    strategy.order = strategy.buy(size=size)


def after_next(strategy: Any, context: dict[str, Any]) -> None:
    if not _has_min_history(strategy):
        return
    _sync_holding_state(strategy)


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    if not _has_min_history(strategy):
        return {"allow": False, "reason": "ATR补丁预热未完成"}

    breakout_level = _breakout_level(strategy)
    current_close = _current_close(strategy)
    confirm_pct = cfg_float(strategy, "atr_breakout_confirm_pct", 0.0)
    if current_close <= breakout_level * (1 + confirm_pct):
        return {
            "allow": False,
            "reason": (
                f"未突破{cfg_int(strategy, 'atr_breakout_period', 20)}日高点"
                f" close={current_close:.2f}"
                f" level={breakout_level:.2f}"
            ),
        }

    atr_value = _current_atr(strategy)
    if atr_value <= 0:
        return {"allow": False, "reason": "ATR无效，无法计算风险仓位"}

    atr_size = _calculate_buy_size_with_atr(strategy)
    if atr_size <= 0:
        return {"allow": False, "reason": "ATR风控后可买数量为0"}
    return {"allow": True, "reason": f"突破确认通过 atr={atr_value:.4f}"}


def allow_sell(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    return {"allow": True}


def _calculate_buy_size_with_atr(strategy: Any) -> int:
    state = _get_state(strategy)
    original_method = state.get("original_calculate_buy_size")
    original_size = int(original_method()) if callable(original_method) else 0
    if original_size <= 0:
        return 0

    current_price = max(
        _current_close(strategy),
        _trade_price(strategy, "open"),
        _trade_price(strategy, "high"),
    )
    atr_value = _current_atr(strategy)
    if current_price <= 0 or atr_value <= 0:
        return 0

    account_value = float(strategy.broker.getvalue())
    risk_pct = cfg_float(strategy, "atr_risk_pct", 0.02)
    risk_budget = account_value * risk_pct
    risk_per_share = _risk_per_share(strategy, current_price, atr_value)
    if risk_budget <= 0 or risk_per_share <= 0:
        return 0

    lot_size = max(cfg_int(strategy, "lot_size", 100), 1)
    atr_size = _round_down_lot(risk_budget / risk_per_share, lot_size)
    return min(original_size, atr_size)


def _calculate_add_on_size(strategy: Any) -> int:
    state = _get_state(strategy)
    unit_size = int(state.get("unit_size") or 0)
    if unit_size <= 0:
        unit_size = _calculate_buy_size_with_atr(strategy)
    if unit_size <= 0:
        return 0
    return min(unit_size, _calculate_buy_size_with_atr(strategy))


def _has_min_history(strategy: Any) -> bool:
    required = max(
        cfg_int(strategy, "atr_period", 14),
        cfg_int(strategy, "atr_breakout_period", 20),
        cfg_int(strategy, "atr_exit_period", 10),
        21,
    )
    return has_history(strategy, required)


def _breakout_level(strategy: Any) -> float:
    return line_value(strategy.atr_patch_breakout_high, ago=-1)


def _build_exit_reason(strategy: Any) -> str | None:
    state = _get_state(strategy)
    close_price = _current_close(strategy)
    stop_price = state.get("stop_price")
    if stop_price is not None and close_price <= float(stop_price):
        return (
            "ATR止损触发"
            f" close={close_price:.2f}"
            f" stop={float(stop_price):.2f}"
        )

    exit_floor = line_value(strategy.atr_patch_exit_low, ago=-1)
    if close_price <= exit_floor:
        return (
            f"跌破{cfg_int(strategy, 'atr_exit_period', 10)}日低点"
            f" close={close_price:.2f}"
            f" floor={exit_floor:.2f}"
        )
    return None


def _sync_holding_state(strategy: Any) -> None:
    state = _get_state(strategy)
    position_size = abs(float(getattr(strategy.position, "size", 0.0) or 0.0))
    previous_size = float(state.get("observed_position_size", 0.0) or 0.0)
    if position_size <= 0 or not _has_effective_position(strategy):
        _reset_holding_state(state)
        return

    current_atr = _current_atr(strategy)
    last_fill_price = (
        float(strategy.last_buy_price)
        if getattr(strategy, "last_buy_price", None) is not None
        else float(strategy.position.price)
    )

    if previous_size <= 0:
        state["observed_position_size"] = position_size
        state["unit_size"] = max(int(round(position_size)), 1)
        state["units"] = 1
        state["entry_base_price"] = float(strategy.position.price)
        state["last_add_price"] = last_fill_price
        state["next_add_price"] = last_fill_price + current_atr * cfg_float(
            strategy,
            "atr_add_unit_atr",
            1.0,
        )
        state["stop_price"] = _compute_stop_price(
            last_fill_price,
            current_atr,
            strategy.param,
        )
        return

    state["observed_position_size"] = position_size
    if position_size > previous_size + 1e-9:
        unit_size = int(state.get("unit_size") or round(previous_size) or 1)
        state["units"] = max(int(round(position_size / unit_size)), 1)
        state["last_add_price"] = last_fill_price
        state["next_add_price"] = last_fill_price + current_atr * cfg_float(
            strategy,
            "atr_add_unit_atr",
            1.0,
        )
        state["stop_price"] = max(
            float(state.get("stop_price") or 0.0),
            _compute_stop_price(last_fill_price, current_atr, strategy.param),
        )


def _reset_holding_state(state: dict[str, Any]) -> None:
    state["observed_position_size"] = 0.0
    state["unit_size"] = 0
    state["units"] = 0
    state["entry_base_price"] = None
    state["last_add_price"] = None
    state["next_add_price"] = None
    state["stop_price"] = None


def _get_state(strategy: Any) -> dict[str, Any]:
    state = getattr(strategy, STATE_KEY, None)
    if not isinstance(state, dict):
        raise RuntimeError("atr 补丁未初始化")
    return state


def _current_close(strategy: Any) -> float:
    return signal_price(strategy, "close")


def _current_atr(strategy: Any) -> float:
    return max(line_value(strategy.atr_patch_atr), 0.0)


def _trade_price(strategy: Any, field: str, ago: int = 0) -> float:
    getter = getattr(strategy, "_get_trade_price", None)
    if callable(getter):
        return float(getter(field, ago=ago))
    return float(getattr(strategy.data, field)[ago])


def _has_effective_position(strategy: Any) -> bool:
    checker = getattr(strategy, "has_effective_position", None)
    if callable(checker):
        return bool(checker())
    return bool(getattr(strategy, "position", None))


def _risk_per_share(strategy: Any, entry_price: float, atr_value: float) -> float:
    stop_price = _compute_stop_price(entry_price, atr_value, strategy.param)
    return max(entry_price - stop_price, 0.0)


def _compute_stop_price(
    entry_price: float,
    atr_value: float,
    config: dict[str, Any],
) -> float:
    stop_loss_pct = float(config.get("atr_stop_loss_pct", config.get("stop_loss_pct", 0.1)))
    atr_multiplier = float(config.get("atr_stop_atr_multiplier", 2.0))
    atr_stop = entry_price - atr_value * atr_multiplier
    fixed_stop = entry_price * (1 - stop_loss_pct)
    return max(atr_stop, fixed_stop)


def _round_down_lot(size: float, lot_size: int) -> int:
    if size <= 0 or lot_size <= 0:
        return 0
    return int(math.floor(size / lot_size) * lot_size)

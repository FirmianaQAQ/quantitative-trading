from __future__ import annotations

"""海龟交易补丁：在基础策略上叠加突破过滤、ATR仓位、加仓与趋势失效退出。"""

import math
from types import MethodType
from typing import Any

import backtrader as bt


STATE_KEY = "_sea_turtle_patch_state"


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    state = {
        "patch_name": context.get("patch_name", "sea_turtle"),
        "observed_position_size": 0.0,
        "unit_size": 0,
        "units": 0,
        "entry_window": 20,
        "entry_base_price": None,
        "last_add_price": None,
        "next_add_price": None,
        "stop_price": None,
        "pending_entry_window": None,
        "pending_breakout_label": None,
    }
    setattr(strategy, STATE_KEY, state)

    atr_period = _cfg_int(strategy, "sea_turtle_atr_period", 20)
    volume_window = _cfg_int(strategy, "sea_turtle_volume_window", 20)
    fast_period = _cfg_int(strategy, "sea_turtle_short_fish_period", 10)
    mid_period = _cfg_int(strategy, "sea_turtle_belt_fish_period", 20)
    slow_period = _cfg_int(strategy, "sea_turtle_filter_period", 55)

    strategy.sea_turtle_atr = bt.indicators.AverageTrueRange(
        strategy.data,
        period=atr_period,
    )
    strategy.sea_turtle_entry_high_20 = bt.indicators.Highest(
        strategy.data.high,
        period=20,
    )
    strategy.sea_turtle_entry_high_55 = bt.indicators.Highest(
        strategy.data.high,
        period=55,
    )
    strategy.sea_turtle_exit_low_10 = bt.indicators.Lowest(
        strategy.data.low,
        period=10,
    )
    strategy.sea_turtle_exit_low_20 = bt.indicators.Lowest(
        strategy.data.low,
        period=20,
    )
    strategy.sea_turtle_range_low_55 = bt.indicators.Lowest(
        strategy.data.low,
        period=55,
    )
    strategy.sea_turtle_fast_ma = bt.indicators.SimpleMovingAverage(
        strategy.data.close,
        period=fast_period,
    )
    strategy.sea_turtle_mid_ma = bt.indicators.SimpleMovingAverage(
        strategy.data.close,
        period=mid_period,
    )
    strategy.sea_turtle_slow_ma = bt.indicators.SimpleMovingAverage(
        strategy.data.close,
        period=slow_period,
    )
    strategy.sea_turtle_volume_ma = bt.indicators.SimpleMovingAverage(
        strategy.data.volume,
        period=volume_window,
    )

    original_calculate_buy_size = strategy.calculate_buy_size
    state["original_calculate_buy_size"] = original_calculate_buy_size

    def _patched_calculate_buy_size(self: Any) -> int:
        return _calculate_buy_size_with_turtle(self)

    strategy.calculate_buy_size = MethodType(_patched_calculate_buy_size, strategy)


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    if not _has_min_history(strategy):
        return

    _sync_holding_state(strategy)

    if getattr(strategy, "order", None) is not None or not strategy.position:
        return

    exit_reason = _build_exit_reason(strategy)
    if exit_reason is not None:
        strategy.log(f"海龟补丁触发卖出 | 原因={exit_reason}")
        strategy.order = strategy.close()
        return

    state = _get_state(strategy)
    next_add_price = state.get("next_add_price")
    if next_add_price is None:
        return

    current_close = _current_close(strategy)
    max_units = _cfg_int(strategy, "sea_turtle_max_units", 4)
    if current_close < float(next_add_price) or int(state.get("units", 0)) >= max_units:
        return

    size = _calculate_add_on_size(strategy)
    if size <= 0:
        strategy.log("海龟补丁放弃加仓 | 原因=可用资金不足")
        return

    state["pending_entry_window"] = state.get("entry_window", 20)
    state["pending_breakout_label"] = "atr_add_on"
    add_on_gap = _cfg_float(strategy, "sea_turtle_add_unit_atr", 0.5)
    strategy.log(
        "海龟补丁触发加仓"
        f" | 当前价={current_close:.2f}"
        f" | 下一加仓价={float(next_add_price):.2f}"
        f" | ATR阶梯={add_on_gap:.2f}"
        f" | 数量={size}"
    )
    strategy.order = strategy.buy(size=size)


def after_next(strategy: Any, context: dict[str, Any]) -> None:
    if not _has_min_history(strategy):
        return
    _sync_holding_state(strategy)


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    if not _has_min_history(strategy):
        return {"allow": False, "reason": "海龟补丁预热未完成"}

    breakout = _build_breakout_snapshot(strategy)
    if not breakout["is_breakout"]:
        return {"allow": False, "reason": "未突破20日或55日高点"}

    qr_score = _compute_qr_score(
        close=_current_close(strategy),
        range_low=_line_value(strategy.sea_turtle_range_low_55, ago=-1),
        range_high=_line_value(strategy.sea_turtle_entry_high_55, ago=-1),
    )
    qr_threshold = _cfg_float(strategy, "sea_turtle_qr_threshold", 0.65)
    if qr_score < qr_threshold:
        return {
            "allow": False,
            "reason": f"QR过滤未通过 qr={qr_score:.3f} threshold={qr_threshold:.3f}",
        }

    if _current_close(strategy) <= _line_value(strategy.sea_turtle_slow_ma):
        return {"allow": False, "reason": "价格仍在55日均线下方，趋势过滤未通过"}

    if not _cdva_confirmed(strategy, float(breakout["breakout_level"])):
        return {"allow": False, "reason": "CDVA确认未通过，突破质量不足"}

    turtle_size = _calculate_buy_size_with_turtle(strategy)
    if turtle_size <= 0:
        return {"allow": False, "reason": "ATR风控后可买数量为0"}

    state = _get_state(strategy)
    state["pending_entry_window"] = int(breakout["entry_window"])
    state["pending_breakout_label"] = str(breakout["label"])
    return {"allow": True}


def allow_sell(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    if not strategy.position:
        return {"allow": True}

    if not _has_min_history(strategy):
        return {"allow": True}

    _sync_holding_state(strategy)
    exit_reason = _build_exit_reason(strategy)
    if exit_reason is None:
        return {"allow": False, "reason": "海龟持有验证仍有效，继续持有"}
    return {"allow": True, "reason": exit_reason}


def _calculate_buy_size_with_turtle(strategy: Any) -> int:
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
    risk_pct = _cfg_float(strategy, "sea_turtle_risk_pct", 0.02)
    risk_budget = account_value * risk_pct
    risk_per_share = _risk_per_share(strategy, current_price, atr_value)
    if risk_budget <= 0 or risk_per_share <= 0:
        return 0

    lot_size = max(_cfg_int(strategy, "lot_size", 100), 1)
    turtle_size = _round_down_lot(risk_budget / risk_per_share, lot_size)
    return min(original_size, turtle_size)


def _calculate_add_on_size(strategy: Any) -> int:
    state = _get_state(strategy)
    unit_size = int(state.get("unit_size") or 0)
    if unit_size <= 0:
        unit_size = _calculate_buy_size_with_turtle(strategy)
    if unit_size <= 0:
        return 0
    return min(unit_size, _calculate_buy_size_with_turtle(strategy))


def _has_min_history(strategy: Any) -> bool:
    required = max(
        _cfg_int(strategy, "sea_turtle_atr_period", 20),
        _cfg_int(strategy, "sea_turtle_volume_window", 20),
        _cfg_int(strategy, "sea_turtle_filter_period", 55),
        56,
    )
    return len(strategy.data) > required


def _build_breakout_snapshot(strategy: Any) -> dict[str, Any]:
    close_price = _current_close(strategy)
    breakout_20 = close_price > _line_value(strategy.sea_turtle_entry_high_20, ago=-1)
    breakout_55 = close_price > _line_value(strategy.sea_turtle_entry_high_55, ago=-1)
    if breakout_55:
        return {
            "is_breakout": True,
            "entry_window": 55,
            "breakout_level": _line_value(strategy.sea_turtle_entry_high_55, ago=-1),
            "label": "55d_breakout",
        }
    if breakout_20:
        return {
            "is_breakout": True,
            "entry_window": 20,
            "breakout_level": _line_value(strategy.sea_turtle_entry_high_20, ago=-1),
            "label": "20d_breakout",
        }
    return {"is_breakout": False}


def _cdva_confirmed(strategy: Any, breakout_level: float) -> bool:
    close_price = _current_close(strategy)
    open_price = _signal_price(strategy, "open")
    high_price = _signal_price(strategy, "high")
    low_price = _signal_price(strategy, "low")
    prev_close = _signal_price(strategy, "close", ago=-1)
    confirm_pct = _cfg_float(strategy, "sea_turtle_breakout_confirm_pct", 0.002)
    volume_ratio = _cfg_float(strategy, "sea_turtle_volume_confirm_ratio", 0.9)

    if high_price <= low_price:
        close_strength = 1.0
    else:
        close_strength = (close_price - low_price) / (high_price - low_price)

    avg_volume = _line_value(strategy.sea_turtle_volume_ma)
    current_volume = float(strategy.data.volume[0])
    volume_ok = avg_volume <= 0 or current_volume >= avg_volume * volume_ratio
    return (
        close_price >= breakout_level * (1 + confirm_pct)
        and close_price >= max(open_price, prev_close)
        and close_strength >= 0.55
        and volume_ok
    )


def _build_exit_reason(strategy: Any) -> str | None:
    state = _get_state(strategy)
    close_price = _current_close(strategy)
    stop_price = state.get("stop_price")
    if stop_price is not None and close_price <= float(stop_price):
        return (
            "固定止损/ATR止损触发"
            f" close={close_price:.2f} stop={float(stop_price):.2f}"
        )

    exit_window = 20 if int(state.get("entry_window", 20)) >= 55 else 10
    exit_line = (
        strategy.sea_turtle_exit_low_20
        if exit_window == 20
        else strategy.sea_turtle_exit_low_10
    )
    exit_floor = _line_value(exit_line, ago=-1)
    if close_price <= exit_floor:
        return f"跌破{exit_window}日低点 close={close_price:.2f} floor={exit_floor:.2f}"

    entry_base_price = state.get("entry_base_price")
    profit_target_pct = _cfg_float(strategy, "sea_turtle_profit_target_pct", 0.2)
    if (
        entry_base_price is not None
        and close_price >= float(entry_base_price) * (1 + profit_target_pct)
    ):
        return (
            "达到盈利目标"
            f" close={close_price:.2f}"
            f" target={float(entry_base_price) * (1 + profit_target_pct):.2f}"
        )

    belt_fish_valid, short_fish_valid = _trend_validation(strategy)
    if not belt_fish_valid and not short_fish_valid:
        return "带鱼/短鱼持有验证失效，趋势转弱"
    return None


def _trend_validation(strategy: Any) -> tuple[bool, bool]:
    close_price = _current_close(strategy)
    fast_ma = _line_value(strategy.sea_turtle_fast_ma)
    mid_ma = _line_value(strategy.sea_turtle_mid_ma)
    slow_ma = _line_value(strategy.sea_turtle_slow_ma)
    belt_fish_valid = close_price >= mid_ma and mid_ma >= slow_ma
    short_fish_valid = close_price >= fast_ma and fast_ma >= mid_ma
    return belt_fish_valid, short_fish_valid


def _sync_holding_state(strategy: Any) -> None:
    state = _get_state(strategy)
    position_size = abs(float(getattr(strategy.position, "size", 0.0) or 0.0))
    previous_size = float(state.get("observed_position_size", 0.0) or 0.0)
    if position_size <= 0:
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
        state["entry_window"] = int(state.get("pending_entry_window") or 20)
        state["entry_base_price"] = float(strategy.position.price)
        state["last_add_price"] = last_fill_price
        state["next_add_price"] = last_fill_price + current_atr * _cfg_float(
            strategy,
            "sea_turtle_add_unit_atr",
            0.5,
        )
        state["stop_price"] = _compute_stop_price(
            last_fill_price,
            current_atr,
            strategy.param,
        )
        state["pending_entry_window"] = None
        return

    state["observed_position_size"] = position_size
    if position_size > previous_size + 1e-9:
        unit_size = int(state.get("unit_size") or round(previous_size) or 1)
        state["units"] = max(int(round(position_size / unit_size)), 1)
        state["last_add_price"] = last_fill_price
        state["next_add_price"] = last_fill_price + current_atr * _cfg_float(
            strategy,
            "sea_turtle_add_unit_atr",
            0.5,
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
    state["pending_entry_window"] = None
    state["pending_breakout_label"] = None


def _get_state(strategy: Any) -> dict[str, Any]:
    state = getattr(strategy, STATE_KEY, None)
    if not isinstance(state, dict):
        raise RuntimeError("sea_turtle 补丁未初始化")
    return state


def _cfg_int(strategy: Any, key: str, default: int) -> int:
    return int(strategy.param.get(key, default))


def _cfg_float(strategy: Any, key: str, default: float) -> float:
    return float(strategy.param.get(key, default))


def _current_close(strategy: Any) -> float:
    return _signal_price(strategy, "close")


def _current_atr(strategy: Any) -> float:
    return max(_line_value(strategy.sea_turtle_atr), 0.0)


def _risk_per_share(strategy: Any, entry_price: float, atr_value: float) -> float:
    stop_price = _compute_stop_price(entry_price, atr_value, strategy.param)
    return max(entry_price - stop_price, 0.0)


def _compute_stop_price(
    entry_price: float,
    atr_value: float,
    config: dict[str, Any],
) -> float:
    stop_loss_pct = float(
        config.get(
            "sea_turtle_stop_loss_pct",
            config.get("stop_loss_pct", 0.1),
        )
    )
    atr_multiplier = float(config.get("sea_turtle_stop_atr_multiplier", 2.0))
    atr_stop = entry_price - atr_value * atr_multiplier
    fixed_stop = entry_price * (1 - stop_loss_pct)
    return max(atr_stop, fixed_stop)


def _compute_qr_score(close: float, range_low: float, range_high: float) -> float:
    if range_high <= range_low:
        return 0.0
    score = (close - range_low) / (range_high - range_low)
    return max(0.0, min(score, 1.0))


def _round_down_lot(size: float, lot_size: int) -> int:
    if size <= 0 or lot_size <= 0:
        return 0
    return int(math.floor(size / lot_size) * lot_size)


def _line_value(line: Any, ago: int = 0) -> float:
    return float(line[ago])


def _signal_price(strategy: Any, field: str, ago: int = 0) -> float:
    getter = getattr(strategy, "_get_signal_price", None)
    if callable(getter):
        return float(getter(field, ago=ago))
    return float(getattr(strategy.data, field)[ago])


def _trade_price(strategy: Any, field: str, ago: int = 0) -> float:
    getter = getattr(strategy, "_get_trade_price", None)
    if callable(getter):
        return float(getter(field, ago=ago))
    return float(getattr(strategy.data, field)[ago])

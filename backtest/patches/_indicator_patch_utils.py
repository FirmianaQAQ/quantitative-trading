from __future__ import annotations

from typing import Any


def cfg_int(strategy: Any, key: str, default: int) -> int:
    return int(getattr(strategy, "param", {}).get(key, default))


def cfg_float(strategy: Any, key: str, default: float) -> float:
    return float(getattr(strategy, "param", {}).get(key, default))


def line_value(line: Any, ago: int = 0) -> float:
    return float(line[ago])


def signal_price(strategy: Any, field: str, ago: int = 0) -> float:
    getter = getattr(strategy, "_get_signal_price", None)
    if callable(getter):
        return float(getter(field, ago=ago))
    return float(getattr(strategy.data, field)[ago])


def has_history(strategy: Any, required: int) -> bool:
    return len(strategy.data) > int(required)


def crosses_up(fast_line: Any, slow_line: Any) -> bool:
    return line_value(fast_line, ago=-1) <= line_value(slow_line, ago=-1) and (
        line_value(fast_line) > line_value(slow_line)
    )


def crosses_down(fast_line: Any, slow_line: Any) -> bool:
    return line_value(fast_line, ago=-1) >= line_value(slow_line, ago=-1) and (
        line_value(fast_line) < line_value(slow_line)
    )


def is_rising(line: Any, lookback: int = 1) -> bool:
    for index in range(lookback):
        if line_value(line, ago=-index) <= line_value(line, ago=-(index + 1)):
            return False
    return True


def is_falling(line: Any, lookback: int = 1) -> bool:
    for index in range(lookback):
        if line_value(line, ago=-index) >= line_value(line, ago=-(index + 1)):
            return False
    return True


def window_max(line: Any, start_ago: int, window: int) -> float:
    return max(line_value(line, ago=-(start_ago + offset)) for offset in range(window))


def window_min(line: Any, start_ago: int, window: int) -> float:
    return min(line_value(line, ago=-(start_ago + offset)) for offset in range(window))


def detect_bearish_divergence(
    price_line: Any,
    indicator_line: Any,
    *,
    window: int,
    tolerance_pct: float = 0.002,
) -> bool:
    if window <= 1:
        return False
    recent_price_high = window_max(price_line, 0, window)
    previous_price_high = window_max(price_line, window, window)
    recent_indicator_high = window_max(indicator_line, 0, window)
    previous_indicator_high = window_max(indicator_line, window, window)
    return (
        recent_price_high > previous_price_high * (1 + tolerance_pct)
        and recent_indicator_high < previous_indicator_high * (1 - tolerance_pct)
    )


def detect_bullish_divergence(
    price_line: Any,
    indicator_line: Any,
    *,
    window: int,
    tolerance_pct: float = 0.002,
) -> bool:
    if window <= 1:
        return False
    recent_price_low = window_min(price_line, 0, window)
    previous_price_low = window_min(price_line, window, window)
    recent_indicator_low = window_min(indicator_line, 0, window)
    previous_indicator_low = window_min(indicator_line, window, window)
    return (
        recent_price_low < previous_price_low * (1 - tolerance_pct)
        and recent_indicator_low > previous_indicator_low * (1 + tolerance_pct)
    )


def submit_patch_exit(
    strategy: Any,
    *,
    patch_name: str,
    reason: str,
) -> bool:
    if getattr(strategy, "order", None) is not None:
        return False
    has_position_method = getattr(strategy, "has_effective_position", None)
    if callable(has_position_method):
        if not bool(has_position_method()):
            return False
    elif not strategy.position:
        return False
    strategy.log(f"{patch_name} 补丁触发卖出 | 原因={reason}")
    strategy.order = strategy.close()
    return True

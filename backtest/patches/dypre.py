from __future__ import annotations

"""Dypre补丁：为动态前复权主策略补充数据校验与运行态记录。"""

from typing import Any


STATE_KEY = "_dypre_patch_state"
_REQUIRED_LINE_NAMES = (
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_preclose",
    "signal_factor",
    "position_adjust_ratio",
)
_EPSILON = 1e-12


def setup_patch(strategy: Any, context: dict[str, Any]) -> None:
    adjust_flag = _normalize_flag(getattr(strategy, "param", {}).get("adjust_flag"))
    missing_lines = _find_missing_lines(strategy)
    state = {
        "patch_name": context.get("patch_name", "dypre"),
        "enabled": adjust_flag == "dypre",
        "adjust_flag": adjust_flag,
        "missing_lines": missing_lines,
        "corporate_action_events": 0,
        "holding_adjustment_events": 0,
        "last_signal_factor": None,
        "last_position_adjust_ratio": None,
        "last_bar_index": None,
    }
    setattr(strategy, STATE_KEY, state)

    logger = getattr(strategy, "log", None)
    if not callable(logger):
        return

    if state["enabled"]:
        logger(
            "Dypre补丁已启用"
            " | 主策略继续负责动态前复权成交与持仓同步"
        )
        if missing_lines:
            logger(
                "Dypre补丁发现数据字段缺失"
                f" | 缺失={','.join(missing_lines)}"
            )
    else:
        logger(
            "Dypre补丁已旁路"
            f" | adjust_flag={adjust_flag or '-'}"
        )


def before_next(strategy: Any, context: dict[str, Any]) -> None:
    state = _get_state(strategy)
    snapshot = _build_snapshot(strategy)
    state["last_signal_factor"] = snapshot["signal_factor"]
    state["last_position_adjust_ratio"] = snapshot["position_adjust_ratio"]
    state["last_bar_index"] = context.get("bar_index")

    ratio = snapshot["position_adjust_ratio"]
    if ratio is None or ratio <= 0 or abs(ratio - 1.0) <= _EPSILON:
        return

    state["corporate_action_events"] += 1
    if bool(getattr(strategy, "position", None)):
        state["holding_adjustment_events"] += 1


def allow_buy(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    state = _get_state(strategy)
    if not state["enabled"]:
        return {"allow": True}

    snapshot = _build_snapshot(strategy)
    issues = _validate_snapshot(snapshot, state["missing_lines"])
    if issues:
        return {"allow": False, "reason": "Dypre数据校验失败: " + "；".join(issues)}
    return {"allow": True}


def allow_sell(strategy: Any, context: dict[str, Any]) -> dict[str, Any]:
    return {"allow": True}


def _get_state(strategy: Any) -> dict[str, Any]:
    state = getattr(strategy, STATE_KEY, None)
    if isinstance(state, dict):
        return state
    state = {
        "patch_name": "dypre",
        "enabled": False,
        "adjust_flag": "",
        "missing_lines": list(_REQUIRED_LINE_NAMES),
        "corporate_action_events": 0,
        "holding_adjustment_events": 0,
        "last_signal_factor": None,
        "last_position_adjust_ratio": None,
        "last_bar_index": None,
    }
    setattr(strategy, STATE_KEY, state)
    return state


def _normalize_flag(value: Any) -> str:
    return str(value or "").strip().lower()


def _find_missing_lines(strategy: Any) -> list[str]:
    missing: list[str] = []
    for line_name in _REQUIRED_LINE_NAMES:
        if _get_line_ref(strategy, line_name) is None:
            missing.append(line_name)
    return missing


def _get_line_ref(strategy: Any, line_name: str) -> Any:
    data = getattr(strategy, "data", None)
    if data is None:
        return None
    lines = getattr(data, "lines", None)
    if lines is not None:
        line = getattr(lines, line_name, None)
        if line is not None:
            return line
    return getattr(data, line_name, None)


def _read_line_value(strategy: Any, line_name: str) -> float | None:
    line = _get_line_ref(strategy, line_name)
    if line is None:
        return None

    try:
        raw_value = line[0]
    except Exception:
        raw_value = line

    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None

    if value != value:
        return None
    return value


def _build_snapshot(strategy: Any) -> dict[str, float | None]:
    signal_close = _read_line_value(strategy, "close")
    raw_close = _read_line_value(strategy, "raw_close")
    signal_factor = _read_line_value(strategy, "signal_factor")
    position_adjust_ratio = _read_line_value(strategy, "position_adjust_ratio")
    return {
        "signal_close": signal_close,
        "raw_close": raw_close,
        "signal_factor": signal_factor,
        "position_adjust_ratio": position_adjust_ratio,
    }


def _validate_snapshot(
    snapshot: dict[str, float | None],
    missing_lines: list[str],
) -> list[str]:
    issues: list[str] = []
    if missing_lines:
        issues.append("缺少字段 " + ",".join(missing_lines))

    signal_close = snapshot.get("signal_close")
    raw_close = snapshot.get("raw_close")
    signal_factor = snapshot.get("signal_factor")
    position_adjust_ratio = snapshot.get("position_adjust_ratio")

    if signal_close is None or signal_close <= 0:
        issues.append("close 无效")
    if raw_close is None or raw_close <= 0:
        issues.append("raw_close 无效")
    if signal_factor is None or signal_factor <= 0:
        issues.append("signal_factor 无效")
    if position_adjust_ratio is None or position_adjust_ratio <= 0:
        issues.append("position_adjust_ratio 无效")

    return issues

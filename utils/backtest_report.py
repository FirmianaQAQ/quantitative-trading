from __future__ import annotations

import json
import os
import re
from html import escape as html_escape
from pathlib import Path
from typing import Any
from datetime import datetime

import pandas as pd


ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@6.0.0/dist/echarts.min.js"
CURRENT_POSITION_AUTO = "auto"
CURRENT_POSITION_EMPTY = "empty"
CURRENT_POSITION_HOLD = "hold"
CURRENT_POSITION_CHOICES = {
    CURRENT_POSITION_AUTO,
    CURRENT_POSITION_EMPTY,
    CURRENT_POSITION_HOLD,
}
ADVICE_SOURCE_STRATEGY = "strategy"
ADVICE_SOURCE_OPTIMIZED = "optimized"
ADVICE_SOURCE_CHOICES = {
    ADVICE_SOURCE_STRATEGY,
    ADVICE_SOURCE_OPTIMIZED,
}


def _is_missing(value: Any) -> bool:
    return value is None or pd.isna(value)


def _to_serializable(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (pd.DatetimeIndex, pd.Index)):
        return [_to_serializable(item) for item in value.tolist()]
    if isinstance(value, pd.Series):
        return [_to_serializable(item) for item in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, dict):
        return {str(key): _to_serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_serializable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _json_dump(value: Any) -> str:
    return json.dumps(_to_serializable(value), ensure_ascii=False)


def _normalize_current_position(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    alias_map = {
        "": CURRENT_POSITION_AUTO,
        "auto": CURRENT_POSITION_AUTO,
        "empty": CURRENT_POSITION_EMPTY,
        "flat": CURRENT_POSITION_EMPTY,
        "none": CURRENT_POSITION_EMPTY,
        "hold": CURRENT_POSITION_HOLD,
        "holding": CURRENT_POSITION_HOLD,
        "position": CURRENT_POSITION_HOLD,
    }
    result = alias_map.get(normalized)
    if result is None:
        choices_text = ", ".join(sorted(CURRENT_POSITION_CHOICES))
        raise ValueError(f"current_position 不支持 {value}，可选值: {choices_text}")
    return result


def _extract_log_date(log_line: str) -> str | None:
    match = re.match(r"^(\d{4}-\d{2}-\d{2})\b", str(log_line))
    if match is None:
        return None
    return match.group(1)


def _split_date_parts(date_text: str | None) -> tuple[str, str, str]:
    if not date_text:
        return "", "", ""
    parts = date_text.split("-")
    year = parts[0] if len(parts) >= 1 else ""
    month = parts[1] if len(parts) >= 2 else ""
    day = parts[2] if len(parts) >= 3 else ""
    return year, month, day


def _strip_log_date_prefix(log_line: str) -> str:
    return re.sub(r"^\d{4}-\d{2}-\d{2}\s*", "", str(log_line)).strip()


ACTION_SPECS = [
    ("sell", "执行卖出", "建议按策略信号执行卖出，优先落袋或止损。"),
    ("buy", "执行买入", "建议按策略信号执行买入，分配本轮计划仓位。"),
    ("hold", "继续持有", "当前更适合继续持有，等待更明确的退出信号。"),
    ("watch_buy", "关注买点", "当前接近买点，先观察确认，不要抢跑。"),
    ("observe", "空仓观察", "当前没有明确买卖信号，继续观察即可。"),
]
ACTION_TITLE_MAP = {key: label for key, label, _ in ACTION_SPECS}
ACTION_SUMMARY_MAP = {key: desc for key, _label, desc in ACTION_SPECS}


def _rewrite_latest_action_for_position(
    action: str,
    reason: str,
    normalized_current_position: str,
) -> tuple[str, str, bool]:
    if normalized_current_position == CURRENT_POSITION_AUTO:
        return action, reason, action in {"buy", "sell", "watch_buy"}

    if normalized_current_position == CURRENT_POSITION_EMPTY:
        if action == "sell":
            return (
                "observe",
                "当前实际空仓，卖出信号无需执行，继续观察下一次买点。",
                False,
            )
        if action == "hold":
            return (
                "observe",
                "当前实际空仓，不执行持有建议，继续观察即可。",
                False,
            )
        if action == "observe":
            return (
                "observe",
                "当前实际空仓，暂时没有明确买点，继续观察即可。",
                False,
            )
        return action, reason, action in {"buy", "watch_buy"}

    if action == "sell":
        return action, reason, True
    if action == "buy":
        return (
            "hold",
            "当前实际持仓，买入信号可作为加仓参考，默认继续持有观察。",
            False,
        )
    if action == "watch_buy":
        return (
            "hold",
            "当前实际持仓，观察买点不作为新开仓信号，继续持有观察。",
            False,
        )
    if action == "observe":
        return (
            "hold",
            "当前实际持仓，暂无明确卖点，继续持有观察。",
            False,
        )
    return action, reason, False


def _extract_daily_advice_entries(
    report_data: list[dict[str, Any]],
    log_lines: list[str] | None,
    current_position: str = CURRENT_POSITION_AUTO,
) -> list[dict[str, str | bool]]:
    normalized_current_position = _normalize_current_position(current_position)
    buy_sell_payload: dict[str, Any] | None = None
    for item in report_data:
        if not isinstance(item, dict):
            continue
        if str(item.get("chart_name", "")).strip() != "买卖点":
            continue
        buy_sell_payload = _normalize_kline_payload(item.get("chart_data"))
        break

    if not buy_sell_payload:
        return []

    dates = [str(item) for item in buy_sell_payload.get("x_axis", [])]
    candles = buy_sell_payload.get("candles", []) or []
    if not dates:
        return []

    buy_price_map = {
        str(item[0]): float(item[1])
        for item in buy_sell_payload.get("buy_points", []) or []
        if isinstance(item, (list, tuple)) and len(item) >= 2
    }
    sell_price_map = {
        str(item[0]): float(item[1])
        for item in buy_sell_payload.get("sell_points", []) or []
        if isinstance(item, (list, tuple)) and len(item) >= 2
    }
    close_price_map: dict[str, float] = {}
    for index, date in enumerate(dates):
        if index >= len(candles):
            continue
        candle = candles[index]
        if isinstance(candle, (list, tuple)) and len(candle) >= 2:
            close_price_map[date] = float(candle[1])

    logs_by_date: dict[str, list[str]] = {}
    for line in log_lines or []:
        date_text = _extract_log_date(str(line))
        if not date_text:
            continue
        logs_by_date.setdefault(date_text, []).append(_strip_log_date_prefix(str(line)))

    action_priority = {
        "sell": [
            "卖出成交",
            "下单卖出",
            "准备卖出",
            "触发止损",
            "考虑卖出",
            "离最高的跌幅超过",
            "连续3天下跌",
            "已经等了10天",
        ],
        "buy": ["买入成交", "下单买入", "买点满足"],
        "hold": [
            "继续持有观察",
            "获利已有",
            "金叉且盈利超过",
            "可能只是震荡",
        ],
        "watch_buy": [
            "价格触发买入观察窗口",
            "切到水上",
            "切到水下",
        ],
    }

    def detect_action(
        day_logs: list[str],
        has_position: bool,
        date: str,
    ) -> tuple[str, str, bool]:
        for action, keywords in action_priority.items():
            for text in day_logs:
                if "power=" in text and len(day_logs) > 1:
                    continue
                if any(keyword in text for keyword in keywords):
                    return action, text, True
        if date in sell_price_map:
            return "sell", f"卖点触发，参考执行价 {sell_price_map[date]:.2f}", True
        if date in buy_price_map:
            return "buy", f"买点触发，参考执行价 {buy_price_map[date]:.2f}", True
        if has_position:
            return "hold", "当前处于持仓阶段，继续跟踪卖出信号。", False
        return "observe", "当前没有明确买卖信号，保持观察。", False

    entries: list[dict[str, str | bool]] = []
    has_position = False
    for index, date in enumerate(dates):
        day_logs = logs_by_date.get(date, [])
        is_latest_date = index == len(dates) - 1
        effective_has_position = has_position
        if is_latest_date and normalized_current_position != CURRENT_POSITION_AUTO:
            effective_has_position = normalized_current_position == CURRENT_POSITION_HOLD

        base_action, base_reason, _ = detect_action(
            day_logs,
            effective_has_position,
            date,
        )
        action = base_action
        reason = base_reason
        is_explicit_signal = action in {"buy", "sell", "watch_buy"}
        if is_latest_date:
            action, reason, is_explicit_signal = _rewrite_latest_action_for_position(
                base_action,
                base_reason,
                normalized_current_position,
            )
        reference_price = close_price_map.get(date)
        if base_action == "buy":
            reference_price = buy_price_map.get(date, reference_price)
            has_position = True
        elif base_action == "sell":
            reference_price = sell_price_map.get(date, reference_price)
            has_position = False
        elif base_action in {"hold", "watch_buy"} and date in buy_price_map:
            has_position = True
        elif base_action == "observe" and date in sell_price_map:
            has_position = False

        price_text = (
            f"{reference_price:.2f}" if reference_price is not None else "-"
        )
        entries.append(
            {
                "date": date,
                "action": action,
                "title": ACTION_TITLE_MAP.get(action, "空仓观察"),
                "price": price_text,
                "reason": reason or ACTION_SUMMARY_MAP.get(action, ""),
                "summary": ACTION_SUMMARY_MAP.get(action, ""),
                "is_signal": action in {"buy", "sell", "watch_buy"},
            }
        )

    entries.reverse()
    return entries


def _extract_optimized_advice_entries(
    report_data: list[dict[str, Any]],
    current_position: str = CURRENT_POSITION_AUTO,
) -> list[dict[str, str | bool]]:
    normalized_current_position = _normalize_current_position(current_position)
    payload: dict[str, Any] | None = None
    for item in report_data:
        if not isinstance(item, dict):
            continue
        if str(item.get("chart_name", "")).strip() != "优化买卖点":
            continue
        payload = _normalize_kline_payload(item.get("chart_data"))
        break

    if not payload:
        return []

    raw_entries = payload.get("advice_entries", []) or []
    if not raw_entries:
        return []

    entries: list[dict[str, str | bool]] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            continue
        date_text = str(raw_entry.get("date", "")).strip()
        if not date_text:
            continue
        action = str(raw_entry.get("action", "observe")).strip() or "observe"
        entries.append(
            {
                "date": date_text,
                "action": action,
                "title": str(raw_entry.get("title", ACTION_TITLE_MAP.get(action, "优化建议"))),
                "price": str(raw_entry.get("price", "-")),
                "reason": str(raw_entry.get("reason", ACTION_SUMMARY_MAP.get(action, ""))),
                "summary": str(raw_entry.get("summary", ACTION_SUMMARY_MAP.get(action, ""))),
                "is_signal": bool(raw_entry.get("is_signal", action in {"buy", "sell", "watch_buy"})),
            }
        )

    entries.sort(key=lambda item: str(item["date"]))
    if entries:
        latest_entry = entries[-1]
        action, reason, is_signal = _rewrite_latest_action_for_position(
            str(latest_entry["action"]),
            str(latest_entry["reason"]),
            normalized_current_position,
        )
        latest_entry["action"] = action
        latest_entry["reason"] = reason
        latest_entry["is_signal"] = is_signal
    entries.reverse()
    return entries


def _build_advice_panel(
    report_data: list[dict[str, Any]],
    log_lines: list[str] | None,
    current_position: str = CURRENT_POSITION_AUTO,
) -> str:
    normalized_current_position = _normalize_current_position(current_position)
    position_modes = [
        CURRENT_POSITION_AUTO,
        CURRENT_POSITION_EMPTY,
        CURRENT_POSITION_HOLD,
    ]
    source_entries_by_position = {
        ADVICE_SOURCE_STRATEGY: {
            mode: _extract_daily_advice_entries(
                report_data,
                log_lines,
                current_position=mode,
            )
            for mode in position_modes
        },
        ADVICE_SOURCE_OPTIMIZED: {
            mode: _extract_optimized_advice_entries(
                report_data,
                current_position=mode,
            )
            for mode in position_modes
        },
    }
    available_sources = [
        source
        for source, entries_by_position in source_entries_by_position.items()
        if any(entries_by_position.values())
    ]
    if not available_sources:
        return ""
    default_source = (
        ADVICE_SOURCE_OPTIMIZED
        if ADVICE_SOURCE_OPTIMIZED in available_sources
        else available_sources[0]
    )

    position_mode_labels = {
        CURRENT_POSITION_AUTO: "按回测信号自动推断",
        CURRENT_POSITION_EMPTY: "当前实际空仓",
        CURRENT_POSITION_HOLD: "当前实际持仓",
    }
    position_mode_tab_labels = {
        CURRENT_POSITION_AUTO: "自动推断",
        CURRENT_POSITION_EMPTY: "当前空仓",
        CURRENT_POSITION_HOLD: "当前持仓",
    }
    source_labels = {
        ADVICE_SOURCE_STRATEGY: "原策略",
        ADVICE_SOURCE_OPTIMIZED: "优化策略",
    }
    cards_html = []
    stats_html = []
    position_mode_chips_html = []
    source_chips_html = []

    if len(available_sources) > 1:
        for source in available_sources:
            source_chips_html.append(
                f"""
                <button
                  type="button"
                  class="advice-source-chip{' is-active' if source == default_source else ''}"
                  data-advice-source="{html_escape(source)}"
                >{html_escape(source_labels[source])}</button>
                """
            )

    for mode in position_modes:
        position_mode_chips_html.append(
            f"""
            <button
              type="button"
              class="advice-position-chip{' is-active' if mode == normalized_current_position else ''}"
              data-advice-position-mode="{html_escape(mode)}"
            >{html_escape(position_mode_tab_labels[mode])}</button>
            """
        )

    for source in available_sources:
        entries_by_position = source_entries_by_position[source]
        for mode in position_modes:
            entries = entries_by_position[mode]
            signal_count = sum(1 for entry in entries if entry["is_signal"])
            buy_count = sum(1 for entry in entries if entry["action"] == "buy")
            sell_count = sum(1 for entry in entries if entry["action"] == "sell")
            watch_count = sum(1 for entry in entries if entry["action"] == "watch_buy")
            stats_style = (
                ""
                if mode == normalized_current_position and source == default_source
                else ' style="display:none;"'
            )
            stats_html.append(
                f"""
                <div
                  class="advice-stats"
                  data-position-mode-stats="{html_escape(mode)}"
                  data-advice-source="{html_escape(source)}"{stats_style}
                >
                  <span class="advice-stat-pill">关键日 {signal_count}</span>
                  <span class="advice-stat-pill is-buy">买入 {buy_count}</span>
                  <span class="advice-stat-pill is-sell">卖出 {sell_count}</span>
                  <span class="advice-stat-pill is-watch">关注买点 {watch_count}</span>
                </div>
                """
            )
            for entry in entries:
                date_text = entry["date"]
                year, month, day = _split_date_parts(date_text)
                cards_html.append(
                    f"""
                    <article
                      class="advice-item"
                      data-advice-date="{html_escape(date_text)}"
                      data-advice-year="{html_escape(year)}"
                      data-advice-month="{html_escape(month)}"
                      data-advice-day="{html_escape(day)}"
                      data-advice-action="{html_escape(entry['action'])}"
                      data-advice-signal="{str(bool(entry['is_signal'])).lower()}"
                      data-advice-position-mode="{html_escape(mode)}"
                      data-advice-source="{html_escape(source)}"
                    >
                      <div class="advice-item-head">
                        <span class="advice-date">{date_text}</span>
                        <span class="advice-badge is-{html_escape(entry['action'])}">{html_escape(entry['title'])}</span>
                      </div>
                      <div class="advice-price">参考价格：{html_escape(entry['price'])}</div>
                      <div class="advice-summary">{html_escape(entry['summary'])}</div>
                      <div class="advice-reason">{html_escape(entry['reason'])}</div>
                    </article>
                    """
                )

    return f"""
    <aside class="advice-panel" data-default-advice-source="{html_escape(default_source)}">
      <div class="advice-panel-header">
        <h2>买卖建议</h2>
        <p>同一份报告内同时提供原策略与优化策略两套口径，并保留时间筛选与实际持仓切换。默认展示：{html_escape(source_labels[default_source])} / {html_escape(position_mode_labels[normalized_current_position])}。</p>
      </div>
      <div class="advice-toolbar">
        <div class="advice-source-group" id="advice-source-group">
          {''.join(source_chips_html)}
        </div>
        <div class="advice-position-group" id="advice-position-group">
          {''.join(position_mode_chips_html)}
        </div>
        {''.join(stats_html)}
        <div class="advice-mode-group" id="advice-mode-group">
          <button type="button" class="advice-mode-chip" data-advice-mode="signal">关键日</button>
          <button type="button" class="advice-mode-chip is-active" data-advice-mode="all">全部</button>
          <button type="button" class="advice-mode-chip" data-advice-mode="buy">只看买入</button>
          <button type="button" class="advice-mode-chip" data-advice-mode="sell">只看卖出</button>
          <button type="button" class="advice-mode-chip" data-advice-mode="hold">持有/观察</button>
        </div>
      </div>
      <div class="advice-list" id="advice-list">
        {''.join(cards_html)}
      </div>
      <div class="advice-empty" id="advice-empty" style="display:none;">当前筛选条件下没有可展示的操作建议。</div>
    </aside>
    """


def _normalize_series_payload(data: Any, default_name: str) -> dict[str, Any]:
    if isinstance(data, pd.Series):
        series = data.dropna()
        return {
            "x_axis": [pd.Timestamp(idx).strftime("%Y-%m-%d") for idx in series.index],
            "series": [
                {
                    "name": data.name or default_name,
                    "data": [round(float(v), 6) for v in series.tolist()],
                }
            ],
        }

    if isinstance(data, pd.DataFrame):
        frame = data.copy()
        if "date" in frame.columns:
            x_axis = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d").tolist()
            value_columns = [col for col in frame.columns if col != "date"]
        else:
            x_axis = [pd.Timestamp(idx).strftime("%Y-%m-%d") for idx in frame.index]
            value_columns = list(frame.columns)

        series_list = []
        for column in value_columns:
            values = pd.to_numeric(frame[column], errors="coerce")
            series_list.append(
                {
                    "name": str(column),
                    "data": [
                        None if _is_missing(v) else round(float(v), 6)
                        for v in values.tolist()
                    ],
                }
            )
        return {"x_axis": x_axis, "series": series_list}

    if isinstance(data, dict):
        payload = dict(data)
        if "x_axis" in payload and "series" in payload:
            return payload
        if "dates" in payload and "series" in payload:
            payload["x_axis"] = payload.pop("dates")
            return payload
        if "data" in payload and isinstance(payload["data"], (list, tuple, pd.Series)):
            return {
                "x_axis": payload.get("x_axis") or payload.get("dates") or [],
                "series": [
                    {
                        "name": payload.get("name", default_name),
                        "data": _to_serializable(payload["data"]),
                    }
                ],
            }

        if payload:
            keys = list(payload.keys())
            if all(not isinstance(payload[key], (dict, list, tuple, pd.Series, pd.DataFrame)) for key in keys):
                return {
                    "x_axis": keys,
                    "series": [{"name": default_name, "data": list(payload.values())}],
                }

    if isinstance(data, (list, tuple)):
        if not data:
            return {"x_axis": [], "series": []}

        first = data[0]
        if isinstance(first, dict):
            if {"date", "value"} <= set(first.keys()):
                return {
                    "x_axis": [str(item["date"]) for item in data],
                    "series": [
                        {
                            "name": default_name,
                            "data": [_to_serializable(item["value"]) for item in data],
                        }
                    ],
                }
            if {"name", "data"} <= set(first.keys()):
                x_axis = None
                series_list = []
                for item in data:
                    if x_axis is None:
                        x_axis = item.get("x_axis") or item.get("dates")
                    series_list.append(
                        {"name": item["name"], "data": _to_serializable(item["data"])}
                    )
                return {"x_axis": x_axis or [], "series": series_list}

        if all(isinstance(item, (int, float)) or _is_missing(item) for item in data):
            return {
                "x_axis": [str(i + 1) for i in range(len(data))],
                "series": [{"name": default_name, "data": _to_serializable(data)}],
            }

    return {"x_axis": [], "series": []}


def _normalize_benchmark_series(benchmarks: list[Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index, benchmark in enumerate(benchmarks or [], start=1):
        payload = _normalize_series_payload(benchmark, f"Benchmark {index}")
        result.extend(payload.get("series", []))
    return result


def _normalize_kline_payload(data: Any) -> dict[str, Any]:
    default_payload = {
        "x_axis": [],
        "candles": [],
        "volumes": [],
        "buy_points": [],
        "sell_points": [],
        "indicator_lines": [],
        "advice_entries": [],
    }

    if isinstance(data, pd.DataFrame):
        frame = data.copy()
        if "date" not in frame.columns:
            frame = frame.reset_index().rename(columns={"index": "date"})
        if not {"open", "high", "low", "close"} <= set(frame.columns):
            return default_payload

        frame["date"] = pd.to_datetime(frame["date"])
        payload = {
            "x_axis": frame["date"].dt.strftime("%Y-%m-%d").tolist(),
            "candles": frame[["open", "close", "low", "high"]].round(4).values.tolist(),
            "volumes": (
                frame["volume"].fillna(0).astype(float).round(4).tolist()
                if "volume" in frame.columns
                else [0] * len(frame)
            ),
            "buy_points": [],
            "sell_points": [],
            "indicator_lines": [],
            "advice_entries": [],
        }

        reserved_columns = {"date", "open", "high", "low", "close", "volume"}
        for column in frame.columns:
            if column in reserved_columns:
                continue
            values = pd.to_numeric(frame[column], errors="coerce")
            if values.notna().any():
                payload["indicator_lines"].append(
                    {
                        "name": str(column),
                        "data": [
                            None if _is_missing(v) else round(float(v), 4)
                            for v in values.tolist()
                        ],
                    }
                )
        return payload

    if isinstance(data, dict):
        payload = dict(default_payload)
        payload.update(_to_serializable(data))
        if "dates" in payload and "x_axis" not in payload:
            payload["x_axis"] = payload.pop("dates")
        if "kline" in payload and "candles" not in payload:
            payload["candles"] = payload.pop("kline")
        if "buy_markers" in payload and "buy_points" not in payload:
            payload["buy_points"] = payload.pop("buy_markers")
        if "sell_markers" in payload and "sell_points" not in payload:
            payload["sell_points"] = payload.pop("sell_markers")
        return payload

    return default_payload


def _normalize_heatmap_payload(data: Any) -> dict[str, Any]:
    default_payload = {"x_axis": [], "y_axis": [], "data": []}

    if isinstance(data, pd.DataFrame):
        frame = data.copy()
        x_axis = [str(column) for column in frame.columns.tolist()]
        y_axis = [str(index) for index in frame.index.tolist()]
        heatmap_data = []
        for row_idx, (_, row) in enumerate(frame.iterrows()):
            for col_idx, value in enumerate(row.tolist()):
                heatmap_data.append(
                    [
                        col_idx,
                        row_idx,
                        None if _is_missing(value) else round(float(value), 6),
                    ]
                )
        return {"x_axis": x_axis, "y_axis": y_axis, "data": heatmap_data}

    if isinstance(data, dict):
        payload = dict(default_payload)
        payload.update(_to_serializable(data))
        return payload

    if isinstance(data, list) and data and isinstance(data[0], dict):
        x_axis = sorted({str(item.get("x")) for item in data})
        y_axis = sorted({str(item.get("y")) for item in data})
        x_pos = {value: index for index, value in enumerate(x_axis)}
        y_pos = {value: index for index, value in enumerate(y_axis)}
        heatmap_data = []
        for item in data:
            x_value = str(item.get("x"))
            y_value = str(item.get("y"))
            value = item.get("value")
            heatmap_data.append(
                [
                    x_pos[x_value],
                    y_pos[y_value],
                    None if _is_missing(value) else round(float(value), 6),
                ]
            )
        return {"x_axis": x_axis, "y_axis": y_axis, "data": heatmap_data}

    return default_payload


def _resolve_metric_card_tone(label: str) -> str:
    normalized = str(label).strip()
    if normalized in {"股票代码", "策略名称"}:
        return "identity"
    if normalized in {
        "总收益率",
        "年化收益率",
        "期末资产",
        "夏普比率",
        "胜率",
        "净利润",
        "平均每笔净利润",
        "盈利次数",
    }:
        return "positive"
    if normalized in {"最大回撤", "最大回撤金额", "最大回撤周期", "亏损次数"}:
        return "risk"
    if normalized in {"总交易次数", "资金占用天数", "资金占用天数占比"}:
        return "focus"
    return "default"


def _build_metric_cards(report_data: list[dict[str, Any]]) -> str:
    metrics = []
    for item in report_data:
        chart_name = str(item.get("chart_name", ""))
        chart_data = item.get("chart_data")
        if chart_name not in {"指标概览", "绩效指标", "summary", "metrics"}:
            continue

        if isinstance(chart_data, pd.Series):
            source = chart_data.to_dict()
        elif isinstance(chart_data, pd.DataFrame) and not chart_data.empty:
            source = chart_data.iloc[0].to_dict()
        elif isinstance(chart_data, dict):
            source = chart_data
        else:
            source = {}

        for key, value in source.items():
            metrics.append((str(key), _to_serializable(value)))

    if not metrics:
        return ""

    cards = []
    for key, value in metrics:
        display = "-" if _is_missing(value) else str(value)
        tone = _resolve_metric_card_tone(key)
        cards.append(
            f"""
            <div class="metric-card is-{tone}" data-metric-label="{html_escape(key)}" data-original-value="{html_escape(display)}">
              <div class="metric-label">{key}</div>
              <div class="metric-value" data-metric-value>{display}</div>
            </div>
            """
        )
    return f'<section class="metrics-grid">{"".join(cards)}</section>'


def _build_filter_toolbar() -> str:
    return """
    <section class="filter-toolbar">
      <div class="filter-toolbar-title">
        <h2>时间筛选</h2>
        <p>支持按年、月、日筛选页面图表与交易日志。</p>
      </div>
      <div class="filter-toolbar-controls">
        <div class="filter-group">
          <span class="filter-group-label">年</span>
          <div class="filter-chip-row" id="report-filter-year-group"></div>
        </div>
        <div class="filter-group">
          <span class="filter-group-label">月</span>
          <div class="filter-chip-row" id="report-filter-month-group"></div>
        </div>
        <div class="filter-group">
          <span class="filter-group-label">日</span>
          <div class="filter-chip-row" id="report-filter-day-group"></div>
        </div>
        <div class="filter-actions">
          <button type="button" id="report-filter-reset">重置筛选</button>
          <button type="button" id="log-toggle-all">展开全部年份</button>
        </div>
      </div>
    </section>
    """


def _build_log_panel(log_lines: list[str] | None) -> str:
    if not log_lines:
        body = '<div class="log-empty">本次回测没有可展示的日志。</div>'
    else:
        grouped_logs: dict[str, list[tuple[str, str]]] = {}
        for line in reversed(log_lines):
            text = str(line)
            log_date = _extract_log_date(text)
            year, month, day = _split_date_parts(log_date)
            group_year = year or "未分类"
            grouped_logs.setdefault(group_year, []).append(
                (text, log_date or "", month, day)
            )

        groups_html: list[str] = []
        for index, (year, items) in enumerate(grouped_logs.items()):
            year_label = f"{year}年" if year.isdigit() else year
            log_items_html = "".join(
                (
                    f'<div class="log-line" data-log-date="{html_escape(log_date)}" '
                    f'data-log-year="{html_escape(year if year.isdigit() else "")}" '
                    f'data-log-month="{html_escape(month)}" '
                    f'data-log-day="{html_escape(day)}">{html_escape(text)}</div>'
                )
                for text, log_date, month, day in items
            )
            groups_html.append(
                f"""
                <details class="log-year-group" data-log-group-year="{html_escape(year)}" {"open" if index == 0 else ""}>
                  <summary>
                    <span class="log-year-label">{year_label}</span>
                    <span class="log-year-meta">
                      <span class="log-year-count" data-total-count="{len(items)}">{len(items)}</span>
                      <span class="log-year-toggle">{"收起" if index == 0 else "展开"}</span>
                    </span>
                  </summary>
                  <div class="log-year-list">
                    {log_items_html}
                  </div>
                </details>
                """
            )

        body = f'<div class="log-list">{"".join(groups_html)}</div>'

    return f"""
    <aside class="log-panel">
      <div class="log-panel-header">
        <h2>交易日志</h2>
        <p>最新记录在上，支持按年份展开收起，并可配合上方年/月/日筛选查看。</p>
      </div>
      {body}
    </aside>
    """


def _build_chart_block(chart_id: str, title: str, subtitle: str = "") -> str:
    subtitle_html = f'<p class="chart-subtitle">{subtitle}</p>' if subtitle else ""
    return f"""
    <section class="chart-card">
      <div class="chart-header">
        <h2>{title}</h2>
        {subtitle_html}
      </div>
      <div id="{chart_id}" class="chart"></div>
    </section>
    """


def _read_ai_report_html(ai_report_path: str | None) -> str:
    if not ai_report_path:
        return ""
    try:
        return Path(ai_report_path).read_text(encoding="utf-8")
    except OSError:
        return ""


def _build_embedded_ai_section(
    ai_report_html: str,
    ai_report_link: str | None = None,
) -> str:
    if not ai_report_html:
        return ""

    external_link_html = ""
    if ai_report_link:
        external_link_html = (
            f'<a class="embedded-ai-link" href="{html_escape(ai_report_link)}" '
            'target="_blank" rel="noopener noreferrer">查看独立 AI 页</a>'
        )

    return f"""
    <section class="embedded-ai-section" id="ai-analysis-section">
      <div class="embedded-ai-header">
        <div>
          <h2>AI 分析</h2>
          <p>已将 `llm_analysis` 报告内容直接内嵌到当前回测 HTML，分享这一份文件即可。</p>
        </div>
        {external_link_html}
      </div>
      <div class="embedded-ai-frame-wrap">
        <iframe
          class="embedded-ai-iframe"
          title="AI 分析报告"
          loading="lazy"
          srcdoc="{html_escape(ai_report_html)}"
        ></iframe>
      </div>
    </section>
    """


def _build_embedded_ai_style_block() -> str:
    return """
    .embedded-ai-section {
      margin-top: 24px;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: var(--shadow-elevated);
      overflow: hidden;
    }
    .embedded-ai-header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      padding: 22px 24px 18px;
      border-bottom: 1px solid var(--border);
      background:
        radial-gradient(circle at top right, rgba(249, 107, 238, 0.14), transparent 34%),
        linear-gradient(180deg, rgba(83, 58, 253, 0.08), rgba(83, 58, 253, 0.01));
    }
    .embedded-ai-header h2 {
      margin: 0;
      color: var(--heading);
      font-size: 24px;
      font-weight: 500;
      letter-spacing: -0.02em;
    }
    .embedded-ai-header p {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }
    .embedded-ai-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 132px;
      height: 38px;
      padding: 0 16px;
      border-radius: 6px;
      border: 1px solid rgba(83, 58, 253, 0.18);
      background: linear-gradient(180deg, #ffffff, #f9f9ff);
      color: var(--primary);
      text-decoration: none;
      font-size: 13px;
      font-weight: 600;
      white-space: nowrap;
      box-shadow: 0 12px 24px -20px rgba(50, 50, 93, 0.45);
    }
    .embedded-ai-link:hover {
      border-color: rgba(83, 58, 253, 0.28);
      background: linear-gradient(180deg, #ffffff, #f2f1ff);
    }
    .embedded-ai-frame-wrap {
      padding: 0;
      background: linear-gradient(180deg, #f6f9fc, #eef3fb);
    }
    .embedded-ai-iframe {
      display: block;
      width: 100%;
      min-height: 920px;
      border: 0;
      background: #fff;
    }
    @media (max-width: 768px) {
      .embedded-ai-header {
        flex-direction: column;
        padding: 18px 18px 16px;
      }
      .embedded-ai-link {
        min-width: 100%;
      }
      .embedded-ai-iframe {
        min-height: 760px;
      }
    }
    """


def _build_embedded_ai_resize_script() -> str:
    return """
    (function () {
      function resizeEmbeddedAIFrames() {
        document.querySelectorAll('.embedded-ai-iframe').forEach((frame) => {
          const doc = frame.contentWindow?.document;
          if (!doc?.body) return;
          const nextHeight = Math.max(
            doc.body.scrollHeight || 0,
            doc.documentElement?.scrollHeight || 0,
            760
          );
          frame.style.height = `${nextHeight + 8}px`;
        });
      }
      document.querySelectorAll('.embedded-ai-iframe').forEach((frame) => {
        frame.addEventListener('load', resizeEmbeddedAIFrames);
      });
      window.addEventListener('load', resizeEmbeddedAIFrames);
      window.addEventListener('resize', resizeEmbeddedAIFrames);
      setTimeout(resizeEmbeddedAIFrames, 180);
    })();
    """


def merge_backtest_html_with_ai_report(
    backtest_html_path: str,
    ai_report_path: str,
    output_path: str | None = None,
) -> Path:
    backtest_path = Path(backtest_html_path)
    ai_path = Path(ai_report_path)
    if not backtest_path.exists():
        raise FileNotFoundError(f"回测报告不存在: {backtest_path}")
    if not ai_path.exists():
        raise FileNotFoundError(f"AI 报告不存在: {ai_path}")

    merged_output_path = (
        Path(output_path)
        if output_path
        else backtest_path.with_name(f"{backtest_path.stem}-share.html")
    )
    merged_output_path.parent.mkdir(parents=True, exist_ok=True)

    backtest_html = backtest_path.read_text(encoding="utf-8")
    ai_report_html = ai_path.read_text(encoding="utf-8")
    ai_report_link = Path(
        os.path.relpath(ai_path, start=merged_output_path.parent)
    ).as_posix()
    embedded_section_html = _build_embedded_ai_section(
        ai_report_html,
        ai_report_link=ai_report_link,
    )

    # 若历史文件已经合并过，先移除旧区块，再重建一份，保证结果可重复生成。
    backtest_html = re.sub(
        r"\s*<section class=\"embedded-ai-section\" id=\"ai-analysis-section\">.*?</section>\s*",
        "\n",
        backtest_html,
        flags=re.DOTALL,
    )

    if ".embedded-ai-section" not in backtest_html and "</style>" in backtest_html:
        backtest_html = backtest_html.replace(
            "</style>",
            f"{_build_embedded_ai_style_block()}\n  </style>",
            1,
        )

    if 'class="page-header-ai-link"' in backtest_html:
        backtest_html = re.sub(
            r'(<a class="page-header-ai-link"[^>]*href=")[^"]+(")',
            r'\1#ai-analysis-section\2',
            backtest_html,
            count=1,
        )

    content_marker = "</div>\n  <script>"
    if content_marker in backtest_html:
        backtest_html = backtest_html.replace(
            content_marker,
            f"    {embedded_section_html}\n  </div>\n  <script>",
            1,
        )
    elif "</body>" in backtest_html:
        backtest_html = backtest_html.replace(
            "</body>",
            f"{embedded_section_html}\n</body>",
            1,
        )
    else:
        backtest_html = f"{backtest_html}\n{embedded_section_html}"

    if "resizeEmbeddedAIFrames" not in backtest_html:
        if "</script>" in backtest_html:
            script_pos = backtest_html.rfind("</script>")
            backtest_html = (
                backtest_html[:script_pos]
                + "\n"
                + _build_embedded_ai_resize_script()
                + "\n"
                + backtest_html[script_pos:]
            )
        elif "</body>" in backtest_html:
            backtest_html = backtest_html.replace(
                "</body>",
                f"<script>\n{_build_embedded_ai_resize_script()}\n</script>\n</body>",
                1,
            )
        else:
            backtest_html = (
                f"{backtest_html}\n<script>\n{_build_embedded_ai_resize_script()}\n</script>\n"
            )

    merged_output_path.write_text(backtest_html, encoding="utf-8")
    return merged_output_path


def _build_buy_sell_chart_script(chart_id: str, payload: dict[str, Any]) -> str:
    return f"""
    window.__BTReport.registerChart({{
      kind: 'buy_sell',
      chartId: '{chart_id}',
      payload: {_json_dump(payload)}
    }});
    """


def _build_line_chart_script(
    chart_id: str,
    payload: dict[str, Any],
    y_axis_name: str,
    percent_axis: bool = True,
) -> str:
    return f"""
    window.__BTReport.registerChart({{
      kind: 'line',
      chartId: '{chart_id}',
      payload: {_json_dump(payload)},
      yAxisName: {_json_dump(y_axis_name)},
      percentAxis: {str(percent_axis).lower()}
    }});
    """


def _build_bar_chart_script(
    chart_id: str,
    payload: dict[str, Any],
    y_axis_name: str,
    percent_axis: bool = True,
) -> str:
    return f"""
    window.__BTReport.registerChart({{
      kind: 'bar',
      chartId: '{chart_id}',
      payload: {_json_dump(payload)},
      yAxisName: {_json_dump(y_axis_name)},
      percentAxis: {str(percent_axis).lower()}
    }});
    """


def _build_area_chart_script(
    chart_id: str,
    payload: dict[str, Any],
    y_axis_name: str,
    percent_axis: bool = False,
) -> str:
    return f"""
    window.__BTReport.registerChart({{
      kind: 'area',
      chartId: '{chart_id}',
      payload: {_json_dump(payload)},
      yAxisName: {_json_dump(y_axis_name)},
      percentAxis: {str(percent_axis).lower()}
    }});
    """


def _build_heatmap_chart_script(chart_id: str, payload: dict[str, Any], percent_axis: bool = True) -> str:
    return f"""
    window.__BTReport.registerChart({{
      kind: 'heatmap',
      chartId: '{chart_id}',
      payload: {_json_dump(payload)},
      percentAxis: {str(percent_axis).lower()}
    }});
    """


def _build_report_bootstrap_script() -> str:
    return """
    window.__BTReport = (function () {
      const registry = [];
      const charts = new Map();
      const currentFilter = { year: '', month: '', day: '' };
      let currentAdviceMode = 'all';
      let currentAdvicePositionMode = 'auto';
      let currentAdviceSource = 'strategy';
      const THEME = {
        text: '#425466',
        heading: '#061b31',
        muted: '#6b7c93',
        primary: '#533afd',
        primarySoft: 'rgba(83, 58, 253, 0.10)',
        buy: '#15be53',
        sell: '#ea2261',
        volume: '#8aa2c6',
        grid: '#e6ebf1',
        surface: '#f6f9fc',
        series: ['#533afd', '#2874ad', '#15be53', '#ea2261', '#8b7bff', '#9b6829'],
      };

      function baseAxisStyle(axisLabelFormatter) {
        return {
          axisLine: { lineStyle: { color: THEME.grid } },
          axisTick: { show: false },
          axisLabel: axisLabelFormatter
            ? { color: THEME.muted, formatter: axisLabelFormatter }
            : { color: THEME.muted },
          splitLine: { lineStyle: { color: THEME.grid, opacity: 0.9 } },
        };
      }

      function baseTooltip() {
        return {
          backgroundColor: 'rgba(255, 255, 255, 0.98)',
          borderColor: THEME.grid,
          borderWidth: 1,
          textStyle: { color: THEME.heading },
          extraCssText: 'box-shadow: rgba(50, 50, 93, 0.25) 0px 24px 40px -28px, rgba(0, 0, 0, 0.08) 0px 12px 24px -18px; border-radius: 8px;',
        };
      }

      function parseDateParts(value) {
        const text = String(value || '');
        const match = text.match(/^(\\d{4})(?:-(\\d{2}))?(?:-(\\d{2}))?/);
        if (!match) return null;
        return {
          year: match[1] || '',
          month: match[2] || '',
          day: match[3] || '',
        };
      }

      function matchesDateFilter(value, filter = currentFilter) {
        if (!filter.year && !filter.month && !filter.day) return true;
        const parts = parseDateParts(value);
        if (!parts) return true;
        if (filter.year && parts.year !== filter.year) return false;
        if (filter.month && parts.month !== filter.month) return false;
        if (filter.day && parts.day !== filter.day) return false;
        return true;
      }

      function collectAvailableDates() {
        const dates = new Set();
        document.querySelectorAll('.log-line[data-log-date]').forEach((node) => {
          const value = node.dataset.logDate;
          if (value) dates.add(value);
        });
        document.querySelectorAll('.advice-item[data-advice-date]').forEach((node) => {
          const value = node.dataset.adviceDate;
          if (value) dates.add(value);
        });
        registry.forEach((item) => {
          const payload = item.payload || {};
          const xAxis = Array.isArray(payload.x_axis) ? payload.x_axis : [];
          xAxis.forEach((value) => {
            const parts = parseDateParts(value);
            if (parts && parts.year && parts.month && parts.day) {
              dates.add(`${parts.year}-${parts.month}-${parts.day}`);
            }
          });
        });
        return Array.from(dates).sort();
      }

      function renderChipGroup(containerId, values, selectedValue, labelFormatter, onSelect, disabledText) {
        const container = document.getElementById(containerId);
        if (!container) return;
        if (!values.length) {
          container.innerHTML = `<button type="button" class="filter-chip is-disabled" disabled>${disabledText}</button>`;
          return;
        }
        const buttons = [];
        buttons.push(
          `<button type="button" class="filter-chip${selectedValue ? '' : ' is-active'}" data-filter-value="">全部</button>`
        );
        values.forEach((value) => {
          buttons.push(
            `<button type="button" class="filter-chip${value === selectedValue ? ' is-active' : ''}" data-filter-value="${value}">${labelFormatter(value)}</button>`
          );
        });
        container.innerHTML = buttons.join('');
        container.querySelectorAll('.filter-chip[data-filter-value]').forEach((node) => {
          node.addEventListener('click', () => onSelect(node.dataset.filterValue || ''));
        });
      }

      function updateFilterControls() {
        const dates = collectAvailableDates().map(parseDateParts).filter(Boolean);
        const years = Array.from(new Set(dates.map((item) => item.year))).sort();
        renderChipGroup(
          'report-filter-year-group',
          years,
          currentFilter.year,
          (value) => `${value}年`,
          (value) => {
            currentFilter.year = value;
            currentFilter.month = '';
            currentFilter.day = '';
            applyFilter();
          },
          '暂无年份'
        );

        const months = currentFilter.year
          ? Array.from(
              new Set(
                dates
                  .filter((item) => item.year === currentFilter.year)
                  .map((item) => item.month)
                  .filter(Boolean)
              )
            ).sort()
          : [];
        if (!currentFilter.year) currentFilter.month = '';
        renderChipGroup(
          'report-filter-month-group',
          months,
          currentFilter.month,
          (value) => `${Number(value)}月`,
          (value) => {
            currentFilter.month = value;
            currentFilter.day = '';
            applyFilter();
          },
          currentFilter.year ? '该年暂无月份' : '先选择年份'
        );

        const days = currentFilter.year && currentFilter.month
          ? Array.from(
              new Set(
                dates
                  .filter(
                    (item) =>
                      item.year === currentFilter.year &&
                      item.month === currentFilter.month
                  )
                  .map((item) => item.day)
                  .filter(Boolean)
              )
            ).sort()
          : [];
        if (!(currentFilter.year && currentFilter.month)) currentFilter.day = '';
        renderChipGroup(
          'report-filter-day-group',
          days,
          currentFilter.day,
          (value) => `${Number(value)}日`,
          (value) => {
            currentFilter.day = value;
            applyFilter();
          },
          currentFilter.year && currentFilter.month ? '该月暂无日期' : '先选择年月'
        );
      }

      function filterLineLikePayload(payload) {
        const xAxis = Array.isArray(payload.x_axis) ? payload.x_axis : [];
        const keepIndexes = xAxis
          .map((value, index) => (matchesDateFilter(value) ? index : -1))
          .filter((value) => value >= 0);
        return {
          ...payload,
          x_axis: keepIndexes.map((index) => xAxis[index]),
          series: (payload.series || []).map((series) => ({
            ...series,
            data: keepIndexes.map((index) => (series.data || [])[index]),
          })),
        };
      }

      function filterBuySellPayload(payload) {
        const xAxis = Array.isArray(payload.x_axis) ? payload.x_axis : [];
        const keepIndexes = xAxis
          .map((value, index) => (matchesDateFilter(value) ? index : -1))
          .filter((value) => value >= 0);
        const allowedDates = new Set(keepIndexes.map((index) => xAxis[index]));
        return {
          ...payload,
          x_axis: keepIndexes.map((index) => xAxis[index]),
          candles: keepIndexes.map((index) => (payload.candles || [])[index]),
          volumes: keepIndexes.map((index) => (payload.volumes || [])[index]),
          indicator_lines: (payload.indicator_lines || []).map((line) => ({
            ...line,
            data: keepIndexes.map((index) => (line.data || [])[index]),
          })),
          buy_points: (payload.buy_points || []).filter(
            (item) => Array.isArray(item) && allowedDates.has(item[0])
          ),
          sell_points: (payload.sell_points || []).filter(
            (item) => Array.isArray(item) && allowedDates.has(item[0])
          ),
        };
      }

      function filterHeatmapPayload(payload) {
        if (!currentFilter.year && !currentFilter.month) return payload;
        let xAxis = [...(payload.x_axis || [])];
        let yAxis = [...(payload.y_axis || [])];
        let data = [...(payload.data || [])];

        if (currentFilter.year) {
          const yIndexes = yAxis
            .map((value, index) => (String(value) === currentFilter.year ? index : -1))
            .filter((value) => value >= 0);
          const ySet = new Set(yIndexes);
          yAxis = yIndexes.map((index) => yAxis[index]);
          data = data.filter((item) => ySet.has(item[1]));
        }

        if (currentFilter.month) {
          const monthAliases = new Set([
            currentFilter.month,
            String(Number(currentFilter.month)),
            `${Number(currentFilter.month)}月`,
          ]);
          const xIndexes = xAxis
            .map((value, index) => (monthAliases.has(String(value)) ? index : -1))
            .filter((value) => value >= 0);
          const xSet = new Set(xIndexes);
          xAxis = xIndexes.map((index) => xAxis[index]);
          data = data.filter((item) => xSet.has(item[0]));
        }

        const xIndexMap = new Map(xAxis.map((value, index) => [String(value), index]));
        const yIndexMap = new Map(yAxis.map((value, index) => [String(value), index]));
        const originalX = payload.x_axis || [];
        const originalY = payload.y_axis || [];

        return {
          ...payload,
          x_axis: xAxis,
          y_axis: yAxis,
          data: data
            .map((item) => {
              const originalXLabel = String(originalX[item[0]]);
              const originalYLabel = String(originalY[item[1]]);
              if (!xIndexMap.has(originalXLabel) || !yIndexMap.has(originalYLabel)) {
                return null;
              }
              return [
                xIndexMap.get(originalXLabel),
                yIndexMap.get(originalYLabel),
                item[2],
              ];
            })
            .filter(Boolean),
        };
      }

      function buildBuySellOption(payload) {
        const xAxis = payload.x_axis || [];
        const candles = payload.candles || [];
        const volumes = payload.volumes || [];
        const indicatorLines = payload.indicator_lines || [];
        const buyMap = Object.fromEntries(
          (payload.buy_points || [])
            .filter((item) => Array.isArray(item) && item.length >= 2)
            .map((item) => [String(item[0]), item[1]])
        );
        const sellMap = Object.fromEntries(
          (payload.sell_points || [])
            .filter((item) => Array.isArray(item) && item.length >= 2)
            .map((item) => [String(item[0]), item[1]])
        );
        const buySeries = xAxis.filter((date) => date in buyMap).map((date) => [date, buyMap[date]]);
        const sellSeries = xAxis.filter((date) => date in sellMap).map((date) => [date, sellMap[date]]);
        const legendLabels = ['K线', '买点', '卖点', '成交量'].concat(
          indicatorLines.map((item) => item.name || 'Indicator')
        );
        const candleData = candles;

        return {
          animation: false,
          color: THEME.series,
          legend: { top: 0, data: legendLabels },
          tooltip: {
            ...baseTooltip(),
            trigger: 'axis',
            axisPointer: { type: 'cross' },
            formatter(params) {
              const htmls = [];
              params.forEach((point) => {
                if (point.seriesType === 'candlestick') {
                  const values = point.data || [];
                  const open = values[0];
                  const close = values[1];
                  const low = values[2];
                  const high = values[3];
                  const date = point.name;
                  const idx = point.dataIndex;
                  const prevClose = idx > 0 && candleData[idx - 1] ? candleData[idx - 1][1] : null;
                  const change = prevClose == null ? 0 : close - prevClose;
                  const changePercent = prevClose ? (change / prevClose * 100).toFixed(2) : '0.00';
                  const changeSign = change >= 0 ? '+' : '';
                  const color = change >= 0 ? THEME.sell : THEME.buy;
                  htmls.push(`<strong>${date}</strong><br/>开: ${open}<br/>收: <span style="color:${color}; font-weight:bold;">${close}</span><br/>高: ${high}<br/>低: ${low}<br/>幅: <span style="color:${color}; font-weight:bold;">${changeSign}${changePercent}%</span><br/><hr style="margin: 4px 0;">`);
                } else if (Array.isArray(point.data)) {
                  const color = point.seriesName === '卖点' ? THEME.sell : THEME.primary;
                  const pointDate = point.data[0];
                  const pointPrice = Number(point.data[1]).toFixed(2);
                  htmls.push(
                    `<div style="margin:4px 0 0;">`
                    + `<span style="display:inline-block; min-width:52px; color:${color}; font-weight:700;">${point.seriesName}</span>`
                    + `<span style="color:#475467;"> 日期=${pointDate}</span>`
                    + `<span style="color:${color}; font-weight:700;"> 价格=${pointPrice}</span>`
                    + `</div>`
                  );
                } else {
                  htmls.push(`${point.seriesName}: ${point.data}<br/>`);
                }
              });
              return htmls.join('');
            },
          },
          grid: [
            { left: '8%', right: '4%', top: 48, height: '58%' },
            { left: '8%', right: '4%', top: '74%', height: '16%' },
          ],
          xAxis: [
            {
              ...baseAxisStyle(),
              type: 'category',
              data: xAxis,
              boundaryGap: true,
              axisLine: { onZero: false, lineStyle: { color: THEME.grid } },
              min: 'dataMin',
              max: 'dataMax',
            },
            {
              ...baseAxisStyle(),
              type: 'category',
              gridIndex: 1,
              data: xAxis,
              boundaryGap: true,
              axisLine: { onZero: false, lineStyle: { color: THEME.grid } },
              axisLabel: { show: false },
              min: 'dataMin',
              max: 'dataMax',
            },
          ],
          yAxis: [
            {
              ...baseAxisStyle(),
              scale: true,
              splitArea: { show: true, areaStyle: { color: ['rgba(246, 249, 252, 0.65)', '#ffffff'] } },
            },
            {
              ...baseAxisStyle(),
              scale: true,
              gridIndex: 1,
              splitNumber: 2,
            },
          ],
          dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
            {
              show: true,
              xAxisIndex: [0, 1],
              type: 'slider',
              bottom: 10,
              start: 0,
              end: 100,
              borderColor: THEME.grid,
              backgroundColor: '#f6f9fc',
              fillerColor: 'rgba(83, 58, 253, 0.12)',
              dataBackground: {
                lineStyle: { color: '#b9c5d4' },
                areaStyle: { color: 'rgba(83, 58, 253, 0.08)' },
              },
            },
          ],
          series: [
            {
              name: 'K线',
              type: 'candlestick',
              data: candleData,
              z: 2,
              itemStyle: {
                color: 'rgba(209, 74, 97, 0.58)',
                color0: 'rgba(21, 190, 83, 0.52)',
                borderColor: '#d14a61',
                borderColor0: '#15be53',
              },
            },
            {
              name: '买点',
              type: 'scatter',
              data: buySeries,
              symbolSize: 12,
              z: 10,
              zlevel: 1,
              itemStyle: { color: THEME.primary },
              tooltip: { valueFormatter: (value) => (value == null ? '-' : Number(value).toFixed(2)) },
            },
            {
              name: '卖点',
              type: 'scatter',
              data: sellSeries,
              symbolSize: 12,
              z: 10,
              zlevel: 1,
              itemStyle: { color: THEME.sell },
              tooltip: { valueFormatter: (value) => (value == null ? '-' : Number(value).toFixed(2)) },
            },
            {
              name: '成交量',
              type: 'bar',
              xAxisIndex: 1,
              yAxisIndex: 1,
              data: volumes,
              z: 1,
              itemStyle: { color: THEME.volume },
            },
            ...indicatorLines.map((item) => ({
              name: item.name || 'Indicator',
              type: 'line',
              data: item.data || [],
              showSymbol: false,
              smooth: false,
              yAxisIndex: 0,
              z: 3,
              lineStyle: { width: 1.6 },
            })),
          ],
        };
      }

      function buildLineLikeOption(kind, payload, yAxisName, percentAxis) {
        const axisLabelFormatter = percentAxis
          ? (value) => `${(Number(value) * 100).toFixed(0)}%`
          : (value) => `${Number(value).toFixed(2)}`;
        const tooltipValueFormatter = percentAxis
          ? (value) => (value == null ? '-' : `${(Number(value) * 100).toFixed(2)}%`)
          : (value) => (value == null ? '-' : Number(value).toFixed(4));

        return {
          animation: false,
          color: THEME.series,
          tooltip: {
            ...baseTooltip(),
            trigger: 'axis',
            axisPointer: { type: kind === 'bar' ? 'shadow' : 'cross' },
            valueFormatter: tooltipValueFormatter,
          },
          legend: { top: 0, textStyle: { color: THEME.text } },
          grid: { left: '8%', right: '4%', top: 48, bottom: 48 },
          xAxis: {
            ...baseAxisStyle(),
            type: 'category',
            data: payload.x_axis || [],
            boundaryGap: kind === 'bar',
          },
          yAxis: {
            ...baseAxisStyle(axisLabelFormatter),
            type: 'value',
            name: yAxisName,
            scale: true,
            nameTextStyle: { color: THEME.muted },
          },
          dataZoom: kind === 'bar'
            ? []
            : [
                { type: 'inside', start: 0, end: 100 },
                {
                  type: 'slider',
                  start: 0,
                  end: 100,
                  bottom: 10,
                  borderColor: THEME.grid,
                  backgroundColor: '#f6f9fc',
                  fillerColor: 'rgba(83, 58, 253, 0.12)',
                  dataBackground: {
                    lineStyle: { color: '#b9c5d4' },
                    areaStyle: { color: 'rgba(83, 58, 253, 0.08)' },
                  },
                },
              ],
          series: (payload.series || []).map((item) => ({
            name: item.name || 'Series',
            type: kind === 'bar' ? 'bar' : 'line',
            data: item.data || [],
            barMaxWidth: kind === 'bar' ? 36 : undefined,
            showSymbol: kind === 'bar' ? undefined : false,
            smooth: false,
            connectNulls: false,
            areaStyle: kind === 'area' ? { color: 'rgba(83, 58, 253, 0.12)' } : undefined,
            lineStyle: kind === 'bar' ? undefined : { width: kind === 'area' ? 1.6 : 2.2 },
            itemStyle: kind === 'bar' ? { borderRadius: [4, 4, 0, 0] } : undefined,
          })),
        };
      }

      function buildHeatmapOption(payload, percentAxis) {
        const visualMin = percentAxis ? -20 : -1;
        const visualMax = percentAxis ? 20 : 1;
        return {
          animation: false,
          color: THEME.series,
          tooltip: {
            ...baseTooltip(),
            position: 'top',
            formatter: (params) => {
              const value = params.data?.[2];
              if (value == null || Number.isNaN(Number(value))) return '-';
              return percentAxis ? `${Number(value).toFixed(2)}%` : Number(value).toFixed(2);
            },
          },
          grid: { left: '8%', right: '12%', top: 48, bottom: 24 },
          xAxis: {
            type: 'category',
            data: payload.x_axis || [],
            axisLine: { lineStyle: { color: THEME.grid } },
            axisTick: { show: false },
            axisLabel: { color: THEME.muted },
            splitArea: { show: true },
          },
          yAxis: {
            type: 'category',
            data: payload.y_axis || [],
            axisLine: { lineStyle: { color: THEME.grid } },
            axisTick: { show: false },
            axisLabel: { color: THEME.muted },
            splitArea: { show: true },
          },
          visualMap: {
            min: visualMin,
            max: visualMax,
            calculable: true,
            orient: 'vertical',
            right: 8,
            top: 'middle',
            textStyle: { color: THEME.muted },
            inRange: { color: ['#15be53', '#f8fafc', '#ea2261'] },
          },
          series: [
            {
              type: 'heatmap',
              data: payload.data || [],
              label: {
                show: true,
                formatter: (params) => {
                  const value = params.data?.[2];
                  if (value == null || Number.isNaN(Number(value))) return '';
                  return percentAxis ? `${Number(value).toFixed(1)}%` : Number(value).toFixed(2);
                },
              },
              emphasis: {
                itemStyle: {
                  shadowBlur: 10,
                  shadowColor: 'rgba(0, 0, 0, 0.3)',
                },
              },
            },
          ],
        };
      }

      function parseMetricNumber(value) {
        const text = String(value || '').replace(/,/g, '').replace(/%/g, '').trim();
        if (!text || text === '-' || text === 'N/A') return null;
        const number = Number(text);
        return Number.isFinite(number) ? number : null;
      }

      function formatMetricNumber(value, digits = 2) {
        if (value == null || !Number.isFinite(value)) return 'N/A';
        return Number(value).toLocaleString('en-US', {
          minimumFractionDigits: digits,
          maximumFractionDigits: digits,
        });
      }

      function formatMetricPercent(value, digits = 2) {
        if (value == null || !Number.isFinite(value)) return 'N/A';
        return `${Number(value).toFixed(digits)}%`;
      }

      function getMetricCard(label) {
        return document.querySelector(`.metric-card[data-metric-label="${label}"]`);
      }

      function setMetricValue(label, value) {
        const card = getMetricCard(label);
        const valueNode = card?.querySelector('[data-metric-value]');
        if (valueNode) valueNode.textContent = value;
      }

      function restoreMetricValues() {
        document.querySelectorAll('.metric-card[data-original-value]').forEach((card) => {
          const valueNode = card.querySelector('[data-metric-value]');
          if (valueNode) valueNode.textContent = card.dataset.originalValue || '-';
        });
      }

      function getPrimaryReturnsItem() {
        return registry.find((item) => {
          if (item.kind !== 'line' || !item.percentAxis) return false;
          const xAxis = item.payload?.x_axis || [];
          const first = xAxis[0];
          return Boolean(parseDateParts(first)) && xAxis.length > 10;
        });
      }

      function getPrimaryBuySellItem() {
        return registry.find((item) => item.kind === 'buy_sell');
      }

      function computeDrawdownStats(assetValues) {
        let peak = -Infinity;
        let maxDrawdownPct = 0;
        let maxDrawdownAmount = 0;
        let currentDuration = 0;
        let maxDuration = 0;
        assetValues.forEach((value) => {
          if (!Number.isFinite(value)) return;
          if (value >= peak) {
            peak = value;
            currentDuration = 0;
            return;
          }
          currentDuration += 1;
          maxDuration = Math.max(maxDuration, currentDuration);
          if (peak > 0) {
            const amount = peak - value;
            const pct = amount / peak * 100;
            if (pct > maxDrawdownPct) maxDrawdownPct = pct;
            if (amount > maxDrawdownAmount) maxDrawdownAmount = amount;
          }
        });
        return {
          maxDrawdownPct,
          maxDrawdownAmount,
          maxDuration,
        };
      }

      function computeTradeStatsFromLogs() {
        const visibleLines = Array.from(document.querySelectorAll('.log-line')).filter(
          (node) => node.style.display !== 'none'
        );
        const netProfits = [];
        visibleLines.forEach((node) => {
          const text = node.textContent || '';
          const match = text.match(/净收益=([-+]?\\d+(?:\\.\\d+)?)/);
          if (match) netProfits.push(Number(match[1]));
        });
        const tradesTotal = netProfits.length;
        const tradesWon = netProfits.filter((value) => value > 0).length;
        const tradesLost = netProfits.filter((value) => value < 0).length;
        const netProfit = netProfits.reduce((sum, value) => sum + value, 0);
        return {
          tradesTotal,
          tradesWon,
          tradesLost,
          winRatePct: tradesTotal ? (tradesWon / tradesTotal) * 100 : 0,
          netProfit,
          avgTradeProfit: tradesTotal ? netProfit / tradesTotal : 0,
        };
      }

      function computePositionStats() {
        const buySellItem = getPrimaryBuySellItem();
        if (!buySellItem) return null;
        const originalPayload = buySellItem.payload || {};
        const filteredPayload = getFilteredPayload(buySellItem);
        const visibleDates = filteredPayload.x_axis || [];
        if (!visibleDates.length) return null;

        const buyDates = new Set((filteredPayload.buy_points || []).map((item) => String(item[0])));
        const sellDates = new Set((filteredPayload.sell_points || []).map((item) => String(item[0])));
        const firstVisibleDate = visibleDates[0];
        let hasPosition = false;

        const originalActions = [];
        (originalPayload.buy_points || []).forEach((item) => {
          if (Array.isArray(item) && item[0] < firstVisibleDate) originalActions.push([String(item[0]), 'buy']);
        });
        (originalPayload.sell_points || []).forEach((item) => {
          if (Array.isArray(item) && item[0] < firstVisibleDate) originalActions.push([String(item[0]), 'sell']);
        });
        originalActions.sort((a, b) => a[0].localeCompare(b[0]));
        if (originalActions.length) {
          hasPosition = originalActions[originalActions.length - 1][1] === 'buy';
        }

        let positionDays = 0;
        let idleDays = 0;
        visibleDates.forEach((date) => {
          if (buyDates.has(date)) hasPosition = true;
          if (hasPosition) positionDays += 1;
          else idleDays += 1;
          if (sellDates.has(date)) hasPosition = false;
        });
        const totalDays = positionDays + idleDays;
        return {
          positionDays,
          idleDays,
          positionRatio: totalDays ? (positionDays / totalDays) * 100 : 0,
          idleRatio: totalDays ? (idleDays / totalDays) * 100 : 0,
        };
      }

      function updateMetricCards() {
        const hasFilter = Boolean(currentFilter.year || currentFilter.month || currentFilter.day);
        if (!hasFilter) {
          restoreMetricValues();
          return;
        }

        const returnsItem = getPrimaryReturnsItem();
        if (!returnsItem) return;
        const filteredPayload = getFilteredPayload(returnsItem);
        const strategySeries = filteredPayload.series?.[0]?.data || [];
        const xAxis = filteredPayload.x_axis || [];
        if (!strategySeries.length || !xAxis.length) return;

        const baseInitial = parseMetricNumber(getMetricCard('初始资金')?.dataset.originalValue);
        if (!Number.isFinite(baseInitial) || baseInitial <= 0) return;

        const validPoints = xAxis
          .map((date, index) => ({ date, value: strategySeries[index] }))
          .filter((item) => item.value != null && Number.isFinite(Number(item.value)));
        if (!validPoints.length) return;

        const firstCumulativeReturn = Number(validPoints[0].value);
        const rebasedReturns = validPoints.map((item) => {
          const currentReturn = Number(item.value);
          return (1 + currentReturn) / (1 + firstCumulativeReturn) - 1;
        });
        const assetValues = rebasedReturns.map((value) => baseInitial * (1 + value));
        const initialValue = baseInitial;
        const finalValue = assetValues[assetValues.length - 1];
        const totalReturnPct = initialValue > 0 ? (finalValue / initialValue - 1) * 100 : null;
        const firstDate = new Date(validPoints[0].date);
        const lastDate = new Date(validPoints[validPoints.length - 1].date);
        const daySpan = Math.max((lastDate - firstDate) / (1000 * 60 * 60 * 24), 0);
        const annualReturnPct =
          initialValue > 0 && finalValue > 0 && daySpan > 0
            ? (Math.pow(finalValue / initialValue, 365 / daySpan) - 1) * 100
            : null;

        const filteredReturns = assetValues
          .map((value, index) => (index === 0 ? null : value / assetValues[index - 1] - 1))
          .filter((value) => value != null && Number.isFinite(value));
        const avgReturn = filteredReturns.length
          ? filteredReturns.reduce((sum, value) => sum + value, 0) / filteredReturns.length
          : null;
        const variance = filteredReturns.length
          ? filteredReturns.reduce((sum, value) => sum + Math.pow(value - avgReturn, 2), 0) / filteredReturns.length
          : null;
        const stdDev = variance != null ? Math.sqrt(variance) : null;
        const sharpeRatio =
          avgReturn != null && stdDev != null && stdDev > 0
            ? (avgReturn / stdDev) * Math.sqrt(252)
            : null;
        const drawdownStats = computeDrawdownStats(assetValues);
        const tradeStats = computeTradeStatsFromLogs();
        const positionStats = computePositionStats();

        setMetricValue('初始资金', formatMetricNumber(initialValue));
        setMetricValue('期末资产', formatMetricNumber(finalValue));
        setMetricValue('总收益率', formatMetricPercent(totalReturnPct));
        setMetricValue('年化收益率', formatMetricPercent(annualReturnPct));
        setMetricValue('最大回撤', formatMetricPercent(drawdownStats.maxDrawdownPct));
        setMetricValue('最大回撤金额', formatMetricNumber(drawdownStats.maxDrawdownAmount));
        setMetricValue('最大回撤周期', String(drawdownStats.maxDuration));
        setMetricValue('夏普比率', sharpeRatio == null ? 'N/A' : Number(sharpeRatio).toFixed(2));
        setMetricValue('总交易次数', String(tradeStats.tradesTotal));
        setMetricValue('盈利次数', String(tradeStats.tradesWon));
        setMetricValue('亏损次数', String(tradeStats.tradesLost));
        setMetricValue('胜率', formatMetricPercent(tradeStats.winRatePct));
        setMetricValue('净利润', formatMetricNumber(tradeStats.netProfit));
        setMetricValue('平均每笔净利润', formatMetricNumber(tradeStats.avgTradeProfit));

        if (positionStats) {
          setMetricValue('资金占用天数', String(positionStats.positionDays));
          setMetricValue('资金占用天数占比', formatMetricPercent(positionStats.positionRatio));
          setMetricValue('资金空闲天数', String(positionStats.idleDays));
          setMetricValue('资金空闲天数占比', formatMetricPercent(positionStats.idleRatio));
        }
      }

      function getFilteredPayload(item) {
        if (item.kind === 'buy_sell') return filterBuySellPayload(item.payload);
        if (item.kind === 'line' || item.kind === 'bar' || item.kind === 'area') {
          return filterLineLikePayload(item.payload);
        }
        if (item.kind === 'heatmap') return filterHeatmapPayload(item.payload);
        return item.payload;
      }

      function renderChart(item) {
        const element = document.getElementById(item.chartId);
        if (!element) return;
        let chart = charts.get(item.chartId);
        if (!chart) {
          chart = echarts.init(element);
          charts.set(item.chartId, chart);
          window.addEventListener('resize', () => chart.resize());
        }

        const payload = getFilteredPayload(item);
        let option;
        if (item.kind === 'buy_sell') {
          option = buildBuySellOption(payload);
        } else if (item.kind === 'line' || item.kind === 'bar' || item.kind === 'area') {
          option = buildLineLikeOption(item.kind, payload, item.yAxisName, item.percentAxis);
        } else if (item.kind === 'heatmap') {
          option = buildHeatmapOption(payload, item.percentAxis);
        } else {
          option = {};
        }
        chart.setOption(option, true);
      }

      function updateLogVisibility() {
        const groups = document.querySelectorAll('.log-year-group');
        groups.forEach((group) => {
          let visibleCount = 0;
          group.querySelectorAll('.log-line').forEach((line) => {
            const matched = matchesDateFilter(line.dataset.logDate || '');
            line.style.display = matched ? '' : 'none';
            if (matched) visibleCount += 1;
          });
          const countNode = group.querySelector('.log-year-count');
          const totalCount = countNode?.dataset.totalCount || '0';
          if (countNode) {
            countNode.textContent = currentFilter.year || currentFilter.month || currentFilter.day
              ? `${visibleCount} / ${totalCount}`
              : totalCount;
          }
          group.style.display = visibleCount > 0 ? '' : 'none';
          if (visibleCount > 0 && (currentFilter.year || currentFilter.month || currentFilter.day)) {
            group.open = true;
          }
        });
        updatePerYearToggleButtons();
        updateLogToggleButton();
      }

      function updatePerYearToggleButtons() {
        document.querySelectorAll('.log-year-group').forEach((group) => {
          const toggleNode = group.querySelector('.log-year-toggle');
          if (!toggleNode) return;
          toggleNode.textContent = group.open ? '收起' : '展开';
        });
      }

      function updateLogToggleButton() {
        const toggleButton = document.getElementById('log-toggle-all');
        if (!toggleButton) return;
        const visibleGroups = Array.from(document.querySelectorAll('.log-year-group')).filter(
          (group) => group.style.display !== 'none'
        );
        if (!visibleGroups.length) {
          toggleButton.textContent = '展开全部年份';
          toggleButton.disabled = true;
          return;
        }
        toggleButton.disabled = false;
        const allExpanded = visibleGroups.every((group) => group.open);
        toggleButton.textContent = allExpanded ? '收起全部年份' : '展开全部年份';
      }

      function matchesAdviceMode(item) {
        const action = item.dataset.adviceAction || '';
        const isSignal = item.dataset.adviceSignal === 'true';
        const positionMode = item.dataset.advicePositionMode || 'auto';
        const adviceSource = item.dataset.adviceSource || 'strategy';
        if (adviceSource !== currentAdviceSource) return false;
        if (positionMode !== currentAdvicePositionMode) return false;
        if (currentAdviceMode === 'all') return true;
        if (currentAdviceMode === 'signal') return isSignal;
        if (currentAdviceMode === 'buy') return action === 'buy';
        if (currentAdviceMode === 'sell') return action === 'sell';
        if (currentAdviceMode === 'hold') return action === 'hold' || action === 'observe';
        return true;
      }

      function updateAdviceModeButtons() {
        document.querySelectorAll('.advice-mode-chip').forEach((item) => {
          item.classList.toggle('is-active', item.dataset.adviceMode === currentAdviceMode);
        });
      }

      function updateAdvicePositionButtons() {
        document.querySelectorAll('.advice-position-chip').forEach((item) => {
          item.classList.toggle(
            'is-active',
            item.dataset.advicePositionMode === currentAdvicePositionMode
          );
        });
      }

      function updateAdviceSourceButtons() {
        document.querySelectorAll('.advice-source-chip').forEach((item) => {
          item.classList.toggle(
            'is-active',
            item.dataset.adviceSource === currentAdviceSource
          );
        });
      }

      function updateAdviceStatsVisibility() {
        document.querySelectorAll('[data-position-mode-stats]').forEach((item) => {
          item.style.display =
            item.dataset.positionModeStats === currentAdvicePositionMode &&
            (item.dataset.adviceSource || 'strategy') === currentAdviceSource
              ? ''
              : 'none';
        });
      }

      function updateAdviceVisibility() {
        const adviceItems = Array.from(document.querySelectorAll('.advice-item'));
        const emptyNode = document.getElementById('advice-empty');
        let visibleCount = 0;
        adviceItems.forEach((item) => {
          const matched =
            matchesDateFilter(item.dataset.adviceDate || '') && matchesAdviceMode(item);
          item.style.display = matched ? '' : 'none';
          if (matched) visibleCount += 1;
        });
        updateAdviceModeButtons();
        updateAdvicePositionButtons();
        updateAdviceSourceButtons();
        updateAdviceStatsVisibility();
        if (emptyNode) {
          emptyNode.style.display = visibleCount > 0 ? 'none' : '';
        }
      }

      function applyFilter() {
        updateFilterControls();
        registry.forEach(renderChart);
        updateAdviceVisibility();
        updateLogVisibility();
        updateMetricCards();
      }

      function bindFilterEvents() {
        const resetButton = document.getElementById('report-filter-reset');
        const toggleButton = document.getElementById('log-toggle-all');
        resetButton?.addEventListener('click', () => {
          currentFilter.year = '';
          currentFilter.month = '';
          currentFilter.day = '';
          applyFilter();
        });
        toggleButton?.addEventListener('click', () => {
          const visibleGroups = Array.from(document.querySelectorAll('.log-year-group')).filter(
            (group) => group.style.display !== 'none'
          );
          const allExpanded = visibleGroups.length > 0 && visibleGroups.every((group) => group.open);
          visibleGroups.forEach((group) => {
            group.open = !allExpanded;
          });
          updatePerYearToggleButtons();
          updateLogToggleButton();
        });
        document.querySelectorAll('.log-year-group').forEach((group) => {
          group.addEventListener('toggle', () => {
            updatePerYearToggleButtons();
            updateLogToggleButton();
          });
        });
        document.querySelectorAll('.advice-mode-chip').forEach((item) => {
          item.addEventListener('click', () => {
            currentAdviceMode = item.dataset.adviceMode || 'all';
            updateAdviceVisibility();
          });
        });
        document.querySelectorAll('.advice-position-chip').forEach((item) => {
          item.addEventListener('click', () => {
            currentAdvicePositionMode =
              item.dataset.advicePositionMode || 'auto';
            updateAdviceVisibility();
          });
        });
        document.querySelectorAll('.advice-source-chip').forEach((item) => {
          item.addEventListener('click', () => {
            currentAdviceSource = item.dataset.adviceSource || 'strategy';
            updateAdviceVisibility();
          });
        });
      }

      return {
        registerChart(config) {
          registry.push(config);
          renderChart(config);
          updateFilterControls();
        },
        init() {
          const advicePanel = document.querySelector('.advice-panel[data-default-advice-source]');
          if (advicePanel?.dataset.defaultAdviceSource) {
            currentAdviceSource = advicePanel.dataset.defaultAdviceSource;
          }
          bindFilterEvents();
          applyFilter();
        },
      };
    })();
    """


def html(
    report_data: list,
    output_path: str,
    benchmarks: list,
    title: str = "回测报告",
    log_lines: list[str] | None = None,
    current_position: str = CURRENT_POSITION_AUTO,
    ai_report_link: str | None = None,
    ai_report_path: str | None = None,
) -> None:
    """
    生成一份 HTML 回测报告。

    :param report_data:
        回测图表数据列表。每个元素通常为 dict，支持以下字段：
        - chart_name: 图表名称，支持“买卖点”“累计收益率”“年末收益率”
        - chart_data: 图表数据
        - subtitle: 图表副标题，可选

        chart_data 支持常见输入：
        1. 买卖点
           - DataFrame，需包含 date/open/high/low/close，可选 volume 与指标列
           - dict，可包含 x_axis/candles/volumes/buy_points/sell_points/indicator_lines
        2. 累计收益率、年末收益率
           - Series / DataFrame
           - dict，包含 x_axis + series
           - list[dict]，每个元素包含 name + data

    :param output_path:
        输出 HTML 文件路径。
    :param benchmarks:
        基准序列列表，会自动叠加到“累计收益率”和“年末收益率”图表中。
    :param title:
        HTML 标题。
    """

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    embedded_ai_report_html = _read_ai_report_html(ai_report_path)
    ai_report_link_html = (
        '<a class="page-header-ai-link" href="#ai-analysis-section">AI</a>'
        if embedded_ai_report_html
        else (
            f'<a class="page-header-ai-link" href="{html_escape(ai_report_link)}" target="_blank" rel="noopener noreferrer">AI</a>'
            if ai_report_link
            else ""
        )
    )
    embedded_ai_section_html = _build_embedded_ai_section(
        embedded_ai_report_html,
        ai_report_link=ai_report_link,
    )

    report_items = list(report_data or [])
    metric_cards_html = _build_metric_cards(report_items)
    filter_toolbar_html = _build_filter_toolbar()
    advice_panel_html = _build_advice_panel(
        report_items,
        log_lines,
        current_position=current_position,
    )
    chart_sections: list[str] = []
    chart_scripts: list[str] = []
    chart_index = 0
    benchmark_series = _normalize_benchmark_series(benchmarks)
    log_panel_html = _build_log_panel(log_lines)
    bootstrap_script = _build_report_bootstrap_script()
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in report_items:
        if not isinstance(item, dict):
            continue

        chart_name = str(item.get("chart_name", "")).strip()
        chart_data = item.get("chart_data")
        subtitle = str(item.get("subtitle", "") or "")

        if chart_name in {"指标概览", "绩效指标", "summary", "metrics"}:
            continue

        chart_index += 1
        chart_id = f"chart_{chart_index}"

        if chart_name in {"买卖点", "优化买卖点"}:
            payload = _normalize_kline_payload(chart_data)
            chart_sections.append(_build_chart_block(chart_id, chart_name, subtitle))
            chart_scripts.append(_build_buy_sell_chart_script(chart_id, payload))
            continue

        if chart_name in {"累计收益率", "累计收益", "cumulative_returns"}:
            payload = _normalize_series_payload(chart_data, "策略")
            payload["series"] = payload.get("series", []) + benchmark_series
            chart_sections.append(_build_chart_block(chart_id, "累计收益率", subtitle))
            chart_scripts.append(_build_line_chart_script(chart_id, payload, "收益率", percent_axis=True))
            continue

        if chart_name in {"年末收益率", "年度收益率", "年度收益", "eoy_returns"}:
            payload = _normalize_series_payload(chart_data, "策略")
            payload["series"] = payload.get("series", []) + benchmark_series
            chart_sections.append(_build_chart_block(chart_id, "年末收益率", subtitle))
            chart_scripts.append(_build_bar_chart_script(chart_id, payload, "收益率", percent_axis=True))
            continue

        if chart_name in {"rolling beta to benchmark", "Rolling Beta To Benchmark", "rolling_beta_to_benchmark"}:
            payload = _normalize_series_payload(chart_data, chart_name)
            chart_sections.append(_build_chart_block(chart_id, "滚动Beta与基准对比", subtitle))
            chart_scripts.append(_build_line_chart_script(chart_id, payload, "Beta", percent_axis=False))
            continue

        if chart_name in {"rolling Volatility(6-months)", "Rolling Volatility(6-Months)", "rolling_volatility_6m"}:
            payload = _normalize_series_payload(chart_data, chart_name)
            chart_sections.append(_build_chart_block(chart_id, "滚动波动率 (6-Months)", subtitle))
            chart_scripts.append(_build_line_chart_script(chart_id, payload, "波动率", percent_axis=True))
            continue

        if chart_name in {"Rolling Sharpe(6-Months)", "rolling sharpe(6-months)", "rolling_sharpe_6m"}:
            payload = _normalize_series_payload(chart_data, chart_name)
            chart_sections.append(_build_chart_block(chart_id, "滚动夏普比率 (6-Months)", subtitle))
            chart_scripts.append(_build_line_chart_script(chart_id, payload, "Sharpe", percent_axis=False))
            continue

        if chart_name in {"Rolling Sortino(6-Months)", "rolling sortino(6-months)", "rolling_sortino_6m"}:
            payload = _normalize_series_payload(chart_data, chart_name)
            chart_sections.append(_build_chart_block(chart_id, "滚动索提诺比率 (6-Months)", subtitle))
            chart_scripts.append(_build_line_chart_script(chart_id, payload, "Sortino", percent_axis=False))
            continue

        if chart_name in {"Underwater Plot", "underwater_plot"}:
            payload = _normalize_series_payload(chart_data, chart_name)
            chart_sections.append(_build_chart_block(chart_id, "Underwater Plot", subtitle))
            chart_scripts.append(_build_area_chart_script(chart_id, payload, "回撤", percent_axis=True))
            continue

        if chart_name in {"Monthly Returns(%)", "Monthly Returns (%)", "monthly_returns_pct"}:
            payload = _normalize_heatmap_payload(chart_data)
            chart_sections.append(_build_chart_block(chart_id, "月度收益率 (%)", subtitle))
            chart_scripts.append(_build_heatmap_chart_script(chart_id, payload, percent_axis=True))
            continue

        payload = _normalize_series_payload(chart_data, chart_name or "策略")
        chart_sections.append(_build_chart_block(chart_id, chart_name or f"图表 {chart_index}", subtitle))
        chart_scripts.append(_build_line_chart_script(chart_id, payload, "数值", percent_axis=False))

    if not chart_sections and not metric_cards_html:
        chart_sections.append(
            """
            <section class="empty-card">
              <h2>暂无回测数据</h2>
              <p>请检查 report_data 是否已传入图表数据。</p>
            </section>
            """
        )

    report_badges = [
        "自动生成回测报告",
        f"图表 {chart_index} 个",
        "含交易日志" if log_lines else "无交易日志",
    ]
    if advice_panel_html:
        report_badges.append("含买卖建议")
    if embedded_ai_report_html:
        report_badges.append("已内嵌 AI 分析")
    elif ai_report_link:
        report_badges.append("可跳转 AI 分析")
    report_badges_html = "".join(
        f'<span class="page-header-badge">{html_escape(text)}</span>'
        for text in report_badges
    )

    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <script src="{ECHARTS_CDN}"></script>
  <style>
    :root {{
      --bg: #f6f9fc;
      --surface: #fbfdff;
      --card: #ffffff;
      --heading: #061b31;
      --text: #425466;
      --muted: #6b7c93;
      --primary: #533afd;
      --primary-hover: #4434d4;
      --primary-soft: rgba(83, 58, 253, 0.1);
      --success: #15be53;
      --danger: #d14a61;
      --warning: #9b6829;
      --border: #e6ebf1;
      --border-strong: #d4dee9;
      --shadow: rgba(23, 23, 23, 0.08) 0px 15px 35px 0px;
      --shadow-elevated: rgba(50, 50, 93, 0.25) 0px 30px 45px -30px, rgba(0, 0, 0, 0.1) 0px 18px 36px -18px;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "SF Pro Display", "PingFang SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(83, 58, 253, 0.08), transparent 26%),
        radial-gradient(circle at top right, rgba(249, 107, 238, 0.08), transparent 20%),
        linear-gradient(180deg, #f7faff 0%, #f6f9fc 42%, #f2f6fb 100%),
        var(--bg);
      color: var(--text);
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
    }}
    .container {{
      max-width: 1360px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .page-header {{
      margin-bottom: 24px;
      padding: 28px 30px 24px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background:
        radial-gradient(circle at right top, rgba(249, 107, 238, 0.12), transparent 26%),
        linear-gradient(180deg, rgba(83, 58, 253, 0.06), rgba(83, 58, 253, 0.01) 38%, #ffffff 100%);
      box-shadow: var(--shadow-elevated);
    }}
    .page-header-kicker {{
      margin: 0 0 10px;
      color: var(--primary);
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}
    .page-header h1 {{
      margin: 0;
      color: var(--heading);
      font-size: 38px;
      font-weight: 500;
      line-height: 1.08;
      letter-spacing: -0.04em;
    }}
    .page-header-title {{
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 14px;
    }}
    .page-header-ai-link,
    .embedded-ai-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      height: 38px;
      padding: 0 16px;
      border-radius: 6px;
      border: 1px solid rgba(83, 58, 253, 0.18);
      background: linear-gradient(180deg, #ffffff, #f9f9ff);
      color: var(--primary);
      text-decoration: none;
      font-size: 13px;
      font-weight: 600;
      white-space: nowrap;
      box-shadow: 0 12px 24px -20px rgba(50, 50, 93, 0.45);
      transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, background 0.18s ease;
    }}
    .page-header-ai-link {{
      min-width: 54px;
    }}
    .embedded-ai-link {{
      min-width: 132px;
    }}
    .page-header-ai-link:hover,
    .embedded-ai-link:hover {{
      transform: translateY(-1px);
      border-color: rgba(83, 58, 253, 0.28);
      background: linear-gradient(180deg, #ffffff, #f2f1ff);
      box-shadow: 0 18px 28px -22px rgba(50, 50, 93, 0.58);
    }}
    .page-header p {{
      margin: 12px 0 0;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.7;
      max-width: 860px;
    }}
    .page-header-badges {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 18px;
    }}
    .page-header-badge {{
      display: inline-flex;
      align-items: center;
      min-height: 28px;
      padding: 0 10px;
      border: 1px solid rgba(83, 58, 253, 0.14);
      border-radius: 4px;
      background: rgba(255, 255, 255, 0.86);
      color: var(--heading);
      font-size: 12px;
      font-weight: 500;
      white-space: nowrap;
      box-shadow: 0 10px 20px -22px rgba(50, 50, 93, 0.45);
    }}
    .filter-toolbar,
    .metric-card,
    .chart-card,
    .empty-card,
    .advice-panel,
    .log-panel,
    .embedded-ai-section {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
    }}
    .filter-toolbar {{
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: 16px;
      margin-bottom: 24px;
      padding: 20px 22px;
      box-shadow: var(--shadow);
    }}
    .filter-toolbar-title {{
      width: 100%;
    }}
    .filter-toolbar-title h2,
    .advice-panel-header h2,
    .log-panel-header h2 {{
      margin: 0;
      color: var(--heading);
      font-size: 22px;
      font-weight: 500;
      letter-spacing: -0.02em;
    }}
    .filter-toolbar-title p,
    .advice-panel-header p,
    .log-panel-header p,
    .chart-subtitle,
    .embedded-ai-header p,
    .empty-card p {{
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }}
    .filter-toolbar-controls {{
      display: flex;
      flex-direction: column;
      align-items: stretch;
      gap: 10px;
      width: 100%;
    }}
    .filter-group {{
      display: flex;
      align-items: flex-start;
      gap: 12px;
      min-width: 100%;
    }}
    .filter-group-label {{
      flex: 0 0 24px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
      line-height: 38px;
    }}
    .filter-chip-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      min-height: 40px;
      align-items: center;
      flex: 1 1 auto;
    }}
    .filter-chip,
    .filter-actions button,
    .advice-position-chip,
    .advice-source-chip,
    .advice-mode-chip,
    .log-year-toggle {{
      border: 1px solid var(--border);
      border-radius: 6px;
      background: linear-gradient(180deg, #ffffff, #fbfcff);
      color: var(--text);
      box-shadow: 0 10px 20px -24px rgba(50, 50, 93, 0.45);
      transition: all 0.18s ease;
    }}
    .filter-chip,
    .filter-actions button {{
      height: 38px;
      padding: 0 14px;
      cursor: pointer;
      font-size: 13px;
      font-weight: 500;
    }}
    .filter-chip:hover,
    .filter-actions button:hover,
    .advice-position-chip:hover,
    .advice-source-chip:hover,
    .advice-mode-chip:hover {{
      border-color: rgba(83, 58, 253, 0.28);
      color: var(--heading);
      transform: translateY(-1px);
      box-shadow: 0 16px 26px -24px rgba(50, 50, 93, 0.5);
    }}
    .filter-chip.is-active,
    .advice-position-chip.is-active,
    .advice-source-chip.is-active,
    .advice-mode-chip.is-active {{
      border-color: rgba(83, 58, 253, 0.34);
      background: linear-gradient(180deg, rgba(83, 58, 253, 0.12), rgba(83, 58, 253, 0.04));
      color: var(--heading);
      box-shadow: 0 18px 30px -26px rgba(50, 50, 93, 0.58);
    }}
    .filter-chip.is-disabled {{
      cursor: not-allowed;
      color: #98a2b3;
      background: var(--surface);
      border-style: dashed;
      box-shadow: none;
    }}
    .filter-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding-top: 4px;
    }}
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 14px;
      margin-bottom: 24px;
    }}
    .metric-card {{
      position: relative;
      padding: 18px 18px 20px;
      overflow: hidden;
      box-shadow: var(--shadow);
    }}
    .metric-card::before {{
      content: "";
      position: absolute;
      inset: 0 auto auto 0;
      width: 100%;
      height: 3px;
      background: var(--border-strong);
    }}
    .metric-card.is-positive::before {{
      background: linear-gradient(90deg, #15be53, #84cc16);
    }}
    .metric-card.is-risk::before {{
      background: linear-gradient(90deg, #ea2261, #f96bee);
    }}
    .metric-card.is-focus::before {{
      background: linear-gradient(90deg, #533afd, #8b7bff);
    }}
    .metric-card.is-identity::before {{
      background: linear-gradient(90deg, #061b31, #2874ad);
    }}
    .metric-label {{
      margin-bottom: 10px;
      color: var(--muted);
      font-size: 13px;
      letter-spacing: 0.01em;
    }}
    .metric-value {{
      color: var(--heading);
      font-size: 28px;
      font-weight: 500;
      line-height: 1.12;
      letter-spacing: -0.03em;
      word-break: break-word;
      font-variant-numeric: tabular-nums;
    }}
    .chart-card {{
      margin-bottom: 20px;
      padding: 20px 20px 12px;
      box-shadow: var(--shadow);
    }}
    .chart-header {{
      margin-bottom: 14px;
      padding-bottom: 14px;
      border-bottom: 1px solid var(--border);
    }}
    .chart-header h2,
    .embedded-ai-header h2,
    .empty-card h2 {{
      margin: 0;
      color: var(--heading);
      font-size: 24px;
      font-weight: 500;
      letter-spacing: -0.02em;
    }}
    .chart {{
      width: 100%;
      height: 560px;
    }}
    .content-layout {{
      display: flex;
      align-items: flex-start;
      gap: 20px;
    }}
    .charts-column {{
      flex: 1 1 auto;
      min-width: 0;
    }}
    .logs-column {{
      flex: 0 0 360px;
      width: 360px;
      max-width: 100%;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }}
    .advice-panel,
    .log-panel {{
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .advice-panel-header,
    .log-panel-header {{
      padding: 20px 22px 16px;
      border-bottom: 1px solid var(--border);
      background:
        radial-gradient(circle at top right, rgba(249, 107, 238, 0.12), transparent 34%),
        linear-gradient(180deg, rgba(83, 58, 253, 0.06), rgba(83, 58, 253, 0));
    }}
    .advice-toolbar {{
      padding: 14px 16px 12px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, rgba(246, 249, 252, 0.96), rgba(250, 252, 255, 0.92));
    }}
    .advice-stats,
    .advice-position-group,
    .advice-source-group {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 10px;
    }}
    .advice-mode-group {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .advice-position-chip,
    .advice-source-chip {{
      height: 36px;
      padding: 0 12px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
    }}
    .advice-mode-chip {{
      height: 32px;
      padding: 0 12px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
    }}
    .advice-stat-pill,
    .advice-badge {{
      padding: 5px 10px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 600;
      border: 1px solid transparent;
    }}
    .advice-stat-pill {{
      color: var(--heading);
      background: #f7f9fc;
      border-color: var(--border);
    }}
    .advice-stat-pill.is-buy,
    .advice-badge.is-buy {{
      color: #108c3d;
      background: rgba(21, 190, 83, 0.1);
      border-color: rgba(21, 190, 83, 0.2);
    }}
    .advice-stat-pill.is-sell,
    .advice-badge.is-sell {{
      color: #c23d63;
      background: rgba(234, 34, 97, 0.1);
      border-color: rgba(234, 34, 97, 0.18);
    }}
    .advice-stat-pill.is-watch,
    .advice-badge.is-hold {{
      color: var(--warning);
      background: rgba(155, 104, 41, 0.1);
      border-color: rgba(155, 104, 41, 0.18);
    }}
    .advice-badge.is-watch_buy {{
      color: var(--primary);
      background: rgba(83, 58, 253, 0.1);
      border-color: rgba(83, 58, 253, 0.18);
    }}
    .advice-badge.is-observe {{
      color: var(--text);
      background: rgba(107, 124, 147, 0.1);
      border-color: rgba(107, 124, 147, 0.16);
    }}
    .advice-list {{
      max-height: 420px;
      overflow: auto;
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .advice-item {{
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: linear-gradient(180deg, #ffffff, #fbfcff);
    }}
    .advice-item-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .advice-date,
    .advice-price,
    .log-year-label {{
      color: var(--heading);
      font-weight: 600;
      font-variant-numeric: tabular-nums;
    }}
    .advice-date,
    .advice-price {{
      font-size: 13px;
    }}
    .advice-price,
    .advice-summary {{
      margin-bottom: 6px;
    }}
    .advice-summary {{
      color: var(--heading);
      font-size: 14px;
      line-height: 1.7;
    }}
    .advice-reason,
    .advice-empty,
    .log-empty {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .advice-empty {{
      padding: 20px;
    }}
    .log-panel {{
      position: sticky;
      top: 20px;
    }}
    .log-list {{
      max-height: calc(100vh - 120px);
      overflow: auto;
      padding: 8px 0;
    }}
    .log-year-group {{
      border-bottom: 1px solid var(--border);
    }}
    .log-year-group:last-child {{
      border-bottom: 0;
    }}
    .log-year-group summary {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 16px;
      cursor: pointer;
      list-style: none;
      user-select: none;
      background: linear-gradient(180deg, rgba(83, 58, 253, 0.04), rgba(83, 58, 253, 0));
    }}
    .log-year-group summary::-webkit-details-marker {{
      display: none;
    }}
    .log-year-meta {{
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--muted);
      font-size: 12px;
    }}
    .log-year-count {{
      min-width: 44px;
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .log-year-toggle {{
      min-width: 52px;
      padding: 4px 10px;
      text-align: center;
      white-space: nowrap;
    }}
    .log-year-list {{
      background: #fff;
    }}
    .log-line {{
      padding: 10px 16px;
      border-bottom: 1px solid var(--border);
      font-family: Consolas, "SFMono-Regular", monospace;
      font-size: 12px;
      line-height: 1.6;
      color: #273951;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .log-line:last-child {{
      border-bottom: 0;
    }}
    .log-empty {{
      padding: 24px 20px;
      font-size: 14px;
    }}
    .embedded-ai-section {{
      margin-top: 24px;
      overflow: hidden;
      box-shadow: var(--shadow-elevated);
    }}
    .embedded-ai-header {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      padding: 22px 24px 18px;
      border-bottom: 1px solid var(--border);
      background:
        radial-gradient(circle at top right, rgba(249, 107, 238, 0.14), transparent 34%),
        linear-gradient(180deg, rgba(83, 58, 253, 0.08), rgba(83, 58, 253, 0.01));
    }}
    .embedded-ai-frame-wrap {{
      padding: 0;
      background: linear-gradient(180deg, #f6f9fc, #eef3fb);
    }}
    .embedded-ai-iframe {{
      display: block;
      width: 100%;
      min-height: 920px;
      border: 0;
      background: #fff;
    }}
    .empty-card {{
      padding: 40px 28px;
      text-align: center;
      box-shadow: var(--shadow);
    }}
    .empty-card h2 {{
      font-size: 26px;
      letter-spacing: -0.03em;
    }}
    @media (max-width: 768px) {{
      .container {{
        padding: 20px 12px 36px;
      }}
      .page-header {{
        padding: 20px 18px 18px;
      }}
      .page-header h1 {{
        font-size: 30px;
      }}
      .page-header p {{
        font-size: 14px;
      }}
      .filter-toolbar {{
        padding: 14px;
      }}
      .filter-toolbar-controls {{
        width: 100%;
      }}
      .filter-group {{
        min-width: 100%;
        flex-direction: column;
        gap: 6px;
      }}
      .filter-group-label {{
        line-height: 1.2;
      }}
      .content-layout {{
        flex-direction: column;
      }}
      .logs-column {{
        width: 100%;
      }}
      .advice-list {{
        max-height: 360px;
      }}
      .advice-toolbar {{
        padding: 12px;
      }}
      .embedded-ai-header {{
        flex-direction: column;
        padding: 18px 18px 16px;
      }}
      .embedded-ai-link {{
        min-width: 100%;
      }}
      .embedded-ai-iframe {{
        min-height: 760px;
      }}
      .log-panel {{
        position: static;
      }}
      .log-list {{
        max-height: 420px;
      }}
      .chart {{
        height: 420px;
      }}
    }}
  </style>
</head>
<body>
	 <div class="container">
	    <header class="page-header">
	      <div class="page-header-kicker">Quantitative Trading Report</div>
	      <div class="page-header-title">
	        <h1>{title}</h1>
	        {ai_report_link_html}
	      </div>
	      <p>{generated_at} 生成。本报告聚合了回测核心指标、图表、交易日志与操作建议，适合直接复盘策略收益、风险和交易节奏。</p>
        <div class="page-header-badges">
          {report_badges_html}
        </div>
	    </header>
    {filter_toolbar_html}
    {metric_cards_html}
    <div class="content-layout">
      <main class="charts-column">
        {''.join(chart_sections)}
      </main>
      <div class="logs-column">
        {advice_panel_html}
        {log_panel_html}
      </div>
    </div>
    {embedded_ai_section_html}
  </div>
  <script>
    {bootstrap_script}
    {''.join(chart_scripts)}
    window.__BTReport.init();
    (function () {{
      function resizeEmbeddedAIFrames() {{
        document.querySelectorAll('.embedded-ai-iframe').forEach((frame) => {{
          const doc = frame.contentWindow?.document;
          if (!doc?.body) return;
          const nextHeight = Math.max(
            doc.body.scrollHeight || 0,
            doc.documentElement?.scrollHeight || 0,
            760
          );
          frame.style.height = `${{nextHeight + 8}}px`;
        }});
      }}
      document.querySelectorAll('.embedded-ai-iframe').forEach((frame) => {{
        frame.addEventListener('load', resizeEmbeddedAIFrames);
      }});
      window.addEventListener('load', resizeEmbeddedAIFrames);
      window.addEventListener('resize', resizeEmbeddedAIFrames);
      setTimeout(resizeEmbeddedAIFrames, 180);
    }})();
  </script>
</body>
</html>
"""

    output.write_text(html_text, encoding="utf-8")

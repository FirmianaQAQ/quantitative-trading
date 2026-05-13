from __future__ import annotations

import json
import re
from html import escape as html_escape
from pathlib import Path
from typing import Any
from datetime import datetime

import pandas as pd


ECHARTS_CDN = "https://cdn.jsdelivr.net/npm/echarts@6.0.0/dist/echarts.min.js"


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
        cards.append(
            f"""
            <div class="metric-card">
              <div class="metric-label">{key}</div>
              <div class="metric-value">{display}</div>
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
        <label class="filter-field">
          <span>年</span>
          <select id="report-filter-year">
            <option value="">全部</option>
          </select>
        </label>
        <label class="filter-field">
          <span>月</span>
          <select id="report-filter-month" disabled>
            <option value="">全部</option>
          </select>
        </label>
        <label class="filter-field">
          <span>日</span>
          <select id="report-filter-day" disabled>
            <option value="">全部</option>
          </select>
        </label>
        <div class="filter-actions">
          <button type="button" id="report-filter-reset">重置筛选</button>
          <button type="button" id="log-expand-all">展开全部年份</button>
          <button type="button" id="log-collapse-all">收起全部年份</button>
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
                      <span class="log-year-toggle">展开 / 收起</span>
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

      function buildOptions(select, values, placeholder, selectedValue) {
        const options = [`<option value="">${placeholder}</option>`];
        values.forEach((value) => {
          const selected = value === selectedValue ? ' selected' : '';
          options.push(`<option value="${value}"${selected}>${value}</option>`);
        });
        select.innerHTML = options.join('');
      }

      function updateFilterSelects() {
        const yearSelect = document.getElementById('report-filter-year');
        const monthSelect = document.getElementById('report-filter-month');
        const daySelect = document.getElementById('report-filter-day');
        if (!yearSelect || !monthSelect || !daySelect) return;

        const dates = collectAvailableDates().map(parseDateParts).filter(Boolean);
        const years = Array.from(new Set(dates.map((item) => item.year))).sort();
        buildOptions(yearSelect, years, '全部', currentFilter.year);

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
        monthSelect.disabled = !currentFilter.year;
        if (!currentFilter.year) currentFilter.month = '';
        buildOptions(monthSelect, months, '全部', currentFilter.month);

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
        daySelect.disabled = !(currentFilter.year && currentFilter.month);
        if (!(currentFilter.year && currentFilter.month)) currentFilter.day = '';
        buildOptions(daySelect, days, '全部', currentFilter.day);
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
          legend: { top: 0, data: legendLabels },
          tooltip: {
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
                  const color = change >= 0 ? '#f00' : '#0f0';
                  htmls.push(`<strong>${date}</strong><br/>开: ${open}<br/>收: <span style="color:${color}; font-weight:bold;">${close}</span><br/>高: ${high}<br/>低: ${low}<br/>幅: <span style="color:${color}; font-weight:bold;">${changeSign}${changePercent}%</span><br/><hr style="margin: 4px 0;">`);
                } else if (Array.isArray(point.data)) {
                  const color = point.seriesName === '卖点' ? '#f00' : '#00f';
                  htmls.push(`<span style="color:${color}">${point.seriesName}: ${point.data[1]}</span><br/>`);
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
              type: 'category',
              data: xAxis,
              boundaryGap: true,
              axisLine: { onZero: false },
              min: 'dataMin',
              max: 'dataMax',
            },
            {
              type: 'category',
              gridIndex: 1,
              data: xAxis,
              boundaryGap: true,
              axisLine: { onZero: false },
              axisTick: { show: false },
              axisLabel: { show: false },
              min: 'dataMin',
              max: 'dataMax',
            },
          ],
          yAxis: [
            { scale: true, splitArea: { show: true } },
            { scale: true, gridIndex: 1, splitNumber: 2 },
          ],
          dataZoom: [
            { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
            { show: true, xAxisIndex: [0, 1], type: 'slider', bottom: 10, start: 0, end: 100 },
          ],
          series: [
            {
              name: 'K线',
              type: 'candlestick',
              data: candleData,
              z: 2,
              itemStyle: {
                color: 'rgba(209, 74, 97, 0.45)',
                color0: 'rgba(58, 162, 114, 0.45)',
                borderColor: 'rgba(209, 74, 97, 0.65)',
                borderColor0: 'rgba(58, 162, 114, 0.65)',
              },
            },
            {
              name: '买点',
              type: 'scatter',
              data: buySeries,
              symbolSize: 12,
              z: 10,
              zlevel: 1,
              itemStyle: { color: '#0f4cdb' },
              tooltip: { valueFormatter: (value) => (value == null ? '-' : Number(value).toFixed(2)) },
            },
            {
              name: '卖点',
              type: 'scatter',
              data: sellSeries,
              symbolSize: 12,
              z: 10,
              zlevel: 1,
              itemStyle: { color: '#c62828' },
              tooltip: { valueFormatter: (value) => (value == null ? '-' : Number(value).toFixed(2)) },
            },
            {
              name: '成交量',
              type: 'bar',
              xAxisIndex: 1,
              yAxisIndex: 1,
              data: volumes,
              z: 1,
              itemStyle: { color: '#91cc75' },
            },
            ...indicatorLines.map((item) => ({
              name: item.name || 'Indicator',
              type: 'line',
              data: item.data || [],
              showSymbol: false,
              smooth: false,
              yAxisIndex: 0,
              z: 3,
              lineStyle: { width: 1.5 },
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
          tooltip: {
            trigger: 'axis',
            axisPointer: { type: kind === 'bar' ? 'shadow' : 'cross' },
            valueFormatter: tooltipValueFormatter,
          },
          legend: { top: 0 },
          grid: { left: '8%', right: '4%', top: 48, bottom: 48 },
          xAxis: {
            type: 'category',
            data: payload.x_axis || [],
            boundaryGap: kind === 'bar',
          },
          yAxis: {
            type: 'value',
            name: yAxisName,
            scale: true,
            axisLabel: { formatter: axisLabelFormatter },
          },
          dataZoom: kind === 'bar'
            ? []
            : [
                { type: 'inside', start: 0, end: 100 },
                { type: 'slider', start: 0, end: 100, bottom: 10 },
              ],
          series: (payload.series || []).map((item) => ({
            name: item.name || 'Series',
            type: kind === 'bar' ? 'bar' : 'line',
            data: item.data || [],
            barMaxWidth: kind === 'bar' ? 36 : undefined,
            showSymbol: kind === 'bar' ? undefined : false,
            smooth: false,
            connectNulls: false,
            areaStyle: kind === 'area' ? {} : undefined,
            lineStyle: kind === 'bar' ? undefined : { width: kind === 'area' ? 1.5 : 2 },
          })),
        };
      }

      function buildHeatmapOption(payload, percentAxis) {
        const visualMin = percentAxis ? -20 : -1;
        const visualMax = percentAxis ? 20 : 1;
        return {
          animation: false,
          tooltip: {
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
            splitArea: { show: true },
          },
          yAxis: {
            type: 'category',
            data: payload.y_axis || [],
            splitArea: { show: true },
          },
          visualMap: {
            min: visualMin,
            max: visualMax,
            calculable: true,
            orient: 'vertical',
            right: 8,
            top: 'middle',
            inRange: { color: ['#1d4ed8', '#f8fafc', '#b91c1c'] },
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
      }

      function applyFilter() {
        updateFilterSelects();
        registry.forEach(renderChart);
        updateLogVisibility();
      }

      function bindFilterEvents() {
        const yearSelect = document.getElementById('report-filter-year');
        const monthSelect = document.getElementById('report-filter-month');
        const daySelect = document.getElementById('report-filter-day');
        const resetButton = document.getElementById('report-filter-reset');
        const expandButton = document.getElementById('log-expand-all');
        const collapseButton = document.getElementById('log-collapse-all');

        yearSelect?.addEventListener('change', (event) => {
          currentFilter.year = event.target.value;
          currentFilter.month = '';
          currentFilter.day = '';
          applyFilter();
        });
        monthSelect?.addEventListener('change', (event) => {
          currentFilter.month = event.target.value;
          currentFilter.day = '';
          applyFilter();
        });
        daySelect?.addEventListener('change', (event) => {
          currentFilter.day = event.target.value;
          applyFilter();
        });
        resetButton?.addEventListener('click', () => {
          currentFilter.year = '';
          currentFilter.month = '';
          currentFilter.day = '';
          applyFilter();
        });
        expandButton?.addEventListener('click', () => {
          document.querySelectorAll('.log-year-group').forEach((group) => {
            if (group.style.display !== 'none') group.open = true;
          });
        });
        collapseButton?.addEventListener('click', () => {
          document.querySelectorAll('.log-year-group').forEach((group) => {
            if (group.style.display !== 'none') group.open = false;
          });
        });
      }

      return {
        registerChart(config) {
          registry.push(config);
          renderChart(config);
          updateFilterSelects();
        },
        init() {
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

    report_items = list(report_data or [])
    metric_cards_html = _build_metric_cards(report_items)
    filter_toolbar_html = _build_filter_toolbar()
    chart_sections: list[str] = []
    chart_scripts: list[str] = []
    chart_index = 0
    benchmark_series = _normalize_benchmark_series(benchmarks)
    log_panel_html = _build_log_panel(log_lines)
    bootstrap_script = _build_report_bootstrap_script()

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

        if chart_name == "买卖点":
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

    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <script src="{ECHARTS_CDN}"></script>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --text: #1f2937;
      --muted: #667085;
      --border: #e5e7eb;
      --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(84, 112, 198, 0.08), transparent 28%),
        radial-gradient(circle at top right, rgba(145, 204, 117, 0.08), transparent 24%),
        var(--bg);
      color: var(--text);
    }}
    .container {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .page-header {{
      margin-bottom: 20px;
    }}
    .page-header h1 {{
      margin: 0;
      font-size: 30px;
      line-height: 1.2;
    }}
    .page-header p {{
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .filter-toolbar {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 20px;
      padding: 16px 18px;
      background: var(--card);
      border: 1px solid rgba(229, 231, 235, 0.9);
      border-radius: 18px;
      box-shadow: var(--shadow);
    }}
    .filter-toolbar-title h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .filter-toolbar-title p {{
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
    }}
    .filter-toolbar-controls {{
      display: flex;
      flex-wrap: wrap;
      align-items: flex-end;
      gap: 12px;
    }}
    .filter-field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
    }}
    .filter-field select {{
      min-width: 88px;
      height: 36px;
      padding: 0 12px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fff;
      color: var(--text);
    }}
    .filter-field select:disabled {{
      background: #f3f4f6;
      color: #9ca3af;
    }}
    .filter-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }}
    .filter-actions button {{
      height: 36px;
      padding: 0 14px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fff;
      color: var(--text);
      cursor: pointer;
    }}
    .filter-actions button:hover {{
      background: #f8fafc;
    }}
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 16px;
      margin-bottom: 20px;
    }}
    .metric-card,
    .chart-card,
    .empty-card {{
      background: var(--card);
      border: 1px solid rgba(229, 231, 235, 0.9);
      border-radius: 18px;
      box-shadow: var(--shadow);
    }}
    .metric-card {{
      padding: 18px 20px;
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
    }}
    .metric-value {{
      font-size: 24px;
      font-weight: 700;
      line-height: 1.2;
      word-break: break-word;
    }}
    .chart-card {{
      margin-bottom: 20px;
      padding: 18px 18px 10px;
    }}
    .chart-header {{
      margin-bottom: 8px;
    }}
    .chart-header h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .chart-subtitle {{
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 13px;
    }}
    .chart {{
      width: 100%;
      height: 560px;
    }}
    .empty-card {{
      padding: 32px 24px;
      text-align: center;
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
    }}
    .log-panel {{
      position: sticky;
      top: 20px;
      background: var(--card);
      border: 1px solid rgba(229, 231, 235, 0.9);
      border-radius: 18px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .log-panel-header {{
      padding: 18px 20px 12px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, rgba(84, 112, 198, 0.06), rgba(84, 112, 198, 0));
    }}
    .log-panel-header h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .log-panel-header p {{
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }}
    .log-list {{
      max-height: calc(100vh - 120px);
      overflow: auto;
      padding: 8px 0;
    }}
    .log-year-group {{
      border-bottom: 1px solid rgba(229, 231, 235, 0.7);
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
      background: rgba(84, 112, 198, 0.04);
    }}
    .log-year-group summary::-webkit-details-marker {{
      display: none;
    }}
    .log-year-label {{
      font-weight: 700;
      font-size: 13px;
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
      white-space: nowrap;
    }}
    .log-year-list {{
      background: #fff;
    }}
    .log-line {{
      padding: 10px 16px;
      border-bottom: 1px solid rgba(229, 231, 235, 0.7);
      font-family: Consolas, "SFMono-Regular", monospace;
      font-size: 12px;
      line-height: 1.6;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .log-line:last-child {{
      border-bottom: 0;
    }}
    .log-empty {{
      padding: 24px 20px;
      color: var(--muted);
      font-size: 14px;
    }}
    .empty-card h2 {{
      margin: 0 0 10px;
    }}
    .empty-card p {{
      margin: 0;
      color: var(--muted);
    }}
    @media (max-width: 768px) {{
      .container {{
        padding: 20px 12px 32px;
      }}
      .page-header h1 {{
        font-size: 24px;
      }}
      .filter-toolbar {{
        padding: 14px;
      }}
      .filter-toolbar-controls {{
        width: 100%;
      }}
      .filter-field {{
        flex: 1 1 90px;
      }}
      .filter-field select {{
        width: 100%;
      }}
      .content-layout {{
        flex-direction: column;
      }}
      .logs-column {{
        width: 100%;
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
      <h1>{title}</h1>
      <p>{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} 本报告由本地回测结果自动生成，图表使用 ECharts 6 渲染。</p>
    </header>
    {filter_toolbar_html}
    {metric_cards_html}
    <div class="content-layout">
      <main class="charts-column">
        {''.join(chart_sections)}
      </main>
      <div class="logs-column">
        {log_panel_html}
      </div>
    </div>
  </div>
  <script>
    {bootstrap_script}
    {''.join(chart_scripts)}
    window.__BTReport.init();
  </script>
</body>
</html>
"""

    output.write_text(html_text, encoding="utf-8")

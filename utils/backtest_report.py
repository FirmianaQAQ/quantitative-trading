from __future__ import annotations

import json
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


def _build_log_panel(log_lines: list[str] | None) -> str:
    if not log_lines:
        body = '<div class="log-empty">本次回测没有可展示的日志。</div>'
    else:
        items = "".join(
            f'<div class="log-line">{html_escape(str(line))}</div>'
            for line in reversed(log_lines)
        )
        body = f'<div class="log-list">{items}</div>'

    return f"""
    <aside class="log-panel">
      <div class="log-panel-header">
        <h2>交易日志</h2>
        <p>同步展示策略 log() 输出，便于对照图表排查买卖点。</p>
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
    x_axis = payload.get("x_axis", [])
    candles = payload.get("candles", [])
    volumes = payload.get("volumes", [])
    buy_points = payload.get("buy_points", [])
    sell_points = payload.get("sell_points", [])
    indicator_lines = payload.get("indicator_lines", [])

    buy_map = {str(item[0]): item[1] for item in buy_points if len(item) >= 2}
    sell_map = {str(item[0]): item[1] for item in sell_points if len(item) >= 2}
    buy_series = [[date, buy_map[date]] for date in x_axis if date in buy_map]
    sell_series = [[date, sell_map[date]] for date in x_axis if date in sell_map]

    indicator_series_js = []
    for item in indicator_lines:
        indicator_series_js.append(
            f"""{{
                name: {_json_dump(item.get("name", "Indicator"))},
                type: 'line',
                data: {_json_dump(item.get("data", []))},
                showSymbol: false,
                smooth: false,
                yAxisIndex: 0,
                z: 3,
                lineStyle: {{ width: 1.5 }},
            }}"""
        )

    legend_labels = ["K线", "买点", "卖点", "成交量"] + [
        item.get("name", "Indicator") for item in indicator_lines
    ]
    series_blocks = [
        f"""{{
            name: 'K线',
            type: 'candlestick',
            data: candleData,
            z: 2,
            itemStyle: {{
              color: 'rgba(209, 74, 97, 0.45)',
              color0: 'rgba(58, 162, 114, 0.45)',
              borderColor: 'rgba(209, 74, 97, 0.65)',
              borderColor0: 'rgba(58, 162, 114, 0.65)'
            }}
        }}""",
        f"""{{
            name: '买点',
            type: 'scatter',
            data: {_json_dump(buy_series)},
            symbolSize: 12,
            z: 10,
            zlevel: 1,
            itemStyle: {{ color: '#0f4cdb' }},
            tooltip: {{ valueFormatter: value => value == null ? '-' : Number(value).toFixed(2) }}
        }}""",
        f"""{{
            name: '卖点',
            type: 'scatter',
            data: {_json_dump(sell_series)},
            symbolSize: 12,
            z: 10,
            zlevel: 1,
            itemStyle: {{ color: '#c62828' }},
            tooltip: {{ valueFormatter: value => value == null ? '-' : Number(value).toFixed(2) }}
        }}""",
        f"""{{
            name: '成交量',
            type: 'bar',
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: {_json_dump(volumes)},
            z: 1,
            itemStyle: {{ color: '#91cc75' }}
        }}""",
    ] + indicator_series_js

    return f"""
    (function() {{
      const chart = echarts.init(document.getElementById('{chart_id}'));
      const candleData = {_json_dump(candles)};
      const option = {{
        animation: false,
        legend: {{
          top: 0,
          data: {_json_dump(legend_labels)}
        }},
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'cross' }},
          
          formatter: function (params) {{
            const htmls = [];

            params.forEach(p => {{
              if (p.seriesType === 'candlestick') {{
                const dataValues = p.data; // 数据数组: [开盘, 收盘, 最低, 最高]
                const open = dataValues[1];
                const close = dataValues[2];
                const low = dataValues[3];
                const high = dataValues[4];
                const date = p.name;

                // 计算涨跌幅
                const idx = p.dataIndex;
                const prevClose = idx > 0 ? candleData[idx - 1][1] : null;
                const change = close - prevClose;
                const changePercent = prevClose ? (change / prevClose * 100).toFixed(2) : 0;
                const changeSign = change >= 0 ? '+' : ''; // 为正数时加上'+'号

                // 根据涨跌决定颜色
                const color = change >= 0 ? '#f00' : '#0f0'; // 涨红跌绿
                htmls.push(`<strong>${{date}}</strong><br/>
                开: ${{open}}<br/>
                收: <span style="color:${{color}}; font-weight:bold;">${{close}}</span><br/>
                高: ${{high}}<br/>
                低: ${{low}}<br/>
                幅: <span style="color:${{color}}; font-weight:bold;">${{changeSign}}${{changePercent}}%</span><br/>
                <hr style="margin: 4px 0;">`);
              }} else if (Array.isArray(p.data)) {{
                const color = p.seriesName === '卖点' ? '#f00' : '#00f'; // 根据数值决定颜色
                htmls.push('<span style="color:' + color + '">' + p.seriesName + ': ' + p.data[1] + '</span><br/>');
              }} else {{
                htmls.push(p.seriesName + ': ' + p.data + ' <br/>');
              }}
            }});

            // 构建并返回HTML内容
            return htmls.join('');
          }}
        }},
        grid: [
          {{ left: '8%', right: '4%', top: 48, height: '58%' }},
          {{ left: '8%', right: '4%', top: '74%', height: '16%' }}
        ],
        xAxis: [
          {{
            type: 'category',
            data: {_json_dump(x_axis)},
            boundaryGap: true,
            axisLine: {{ onZero: false }},
            min: 'dataMin',
            max: 'dataMax'
          }},
          {{
            type: 'category',
            gridIndex: 1,
            data: {_json_dump(x_axis)},
            boundaryGap: true,
            axisLine: {{ onZero: false }},
            axisTick: {{ show: false }},
            axisLabel: {{ show: false }},
            min: 'dataMin',
            max: 'dataMax'
          }}
        ],
        yAxis: [
          {{
            scale: true,
            splitArea: {{ show: true }}
          }},
          {{
            scale: true,
            gridIndex: 1,
            splitNumber: 2
          }}
        ],
        dataZoom: [
          {{ type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 }},
          {{ show: true, xAxisIndex: [0, 1], type: 'slider', bottom: 10, start: 0, end: 100 }}
        ],
        series: [{",".join(series_blocks)}]
      }};
      chart.setOption(option);
      window.addEventListener('resize', function() {{ chart.resize(); }});
    }})();
    """


def _build_line_chart_script(
    chart_id: str,
    payload: dict[str, Any],
    y_axis_name: str,
    percent_axis: bool = True,
) -> str:
    series_blocks = []
    for item in payload.get("series", []):
        series_blocks.append(
            f"""{{
                name: {_json_dump(item.get("name", "Series"))},
                type: 'line',
                data: {_json_dump(item.get("data", []))},
                showSymbol: false,
                smooth: false,
                connectNulls: false,
                lineStyle: {{ width: 2 }}
            }}"""
        )

    axis_label_formatter = (
        "value => `${(Number(value) * 100).toFixed(0)}%`"
        if percent_axis
        else "value => `${Number(value).toFixed(2)}`"
    )
    tooltip_value_formatter = (
        "value => value == null ? '-' : `${(Number(value) * 100).toFixed(2)}%`"
        if percent_axis
        else "value => value == null ? '-' : Number(value).toFixed(4)"
    )

    return f"""
    (function() {{
      const chart = echarts.init(document.getElementById('{chart_id}'));
      chart.setOption({{
        animation: false,
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'cross' }},
          valueFormatter: {tooltip_value_formatter}
        }},
        legend: {{
          top: 0
        }},
        grid: {{
          left: '8%',
          right: '4%',
          top: 48,
          bottom: 48
        }},
        xAxis: {{
          type: 'category',
          data: {_json_dump(payload.get("x_axis", []))},
          boundaryGap: false
        }},
        yAxis: {{
          type: 'value',
          name: {_json_dump(y_axis_name)},
          scale: true,
          axisLabel: {{
            formatter: {axis_label_formatter}
          }}
        }},
        dataZoom: [
          {{ type: 'inside', start: 0, end: 100 }},
          {{ type: 'slider', start: 0, end: 100, bottom: 10 }}
        ],
        series: [{",".join(series_blocks)}]
      }});
      window.addEventListener('resize', function() {{ chart.resize(); }});
    }})();
    """


def _build_bar_chart_script(
    chart_id: str,
    payload: dict[str, Any],
    y_axis_name: str,
    percent_axis: bool = True,
) -> str:
    series_blocks = []
    for item in payload.get("series", []):
        series_blocks.append(
            f"""{{
                name: {_json_dump(item.get("name", "Series"))},
                type: 'bar',
                data: {_json_dump(item.get("data", []))},
                barMaxWidth: 36
            }}"""
        )

    tooltip_value_formatter = (
        "value => value == null ? '-' : `${Number(value).toFixed(2)}%`"
        if percent_axis
        else "value => value == null ? '-' : Number(value).toFixed(4)"
    )
    axis_label_formatter = (
        "value => `${Number(value).toFixed(0)}%`"
        if percent_axis
        else "value => `${Number(value).toFixed(2)}`"
    )

    return f"""
    (function() {{
      const chart = echarts.init(document.getElementById('{chart_id}'));
      chart.setOption({{
        animation: false,
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'shadow' }},
          valueFormatter: {tooltip_value_formatter}
        }},
        legend: {{
          top: 0
        }},
        grid: {{
          left: '8%',
          right: '4%',
          top: 48,
          bottom: 48
        }},
        xAxis: {{
          type: 'category',
          data: {_json_dump(payload.get("x_axis", []))}
        }},
        yAxis: {{
          type: 'value',
          name: {_json_dump(y_axis_name)},
          scale: true,
          axisLabel: {{
            formatter: {axis_label_formatter}
          }}
        }},
        series: [{",".join(series_blocks)}]
      }});
      window.addEventListener('resize', function() {{ chart.resize(); }});
    }})();
    """


def _build_area_chart_script(
    chart_id: str,
    payload: dict[str, Any],
    y_axis_name: str,
    percent_axis: bool = False,
) -> str:
    series_blocks = []
    for item in payload.get("series", []):
        series_blocks.append(
            f"""{{
                name: {_json_dump(item.get("name", "Series"))},
                type: 'line',
                data: {_json_dump(item.get("data", []))},
                showSymbol: false,
                smooth: false,
                connectNulls: false,
                areaStyle: {{}},
                lineStyle: {{ width: 1.5 }}
            }}"""
        )

    axis_label_formatter = (
        "value => `${(Number(value) * 100).toFixed(0)}%`"
        if percent_axis
        else "value => `${value}`"
    )
    tooltip_value_formatter = (
        "value => value == null ? '-' : `${(Number(value) * 100).toFixed(2)}%`"
        if percent_axis
        else "value => value == null ? '-' : Number(value).toFixed(2)"
    )

    return f"""
    (function() {{
      const chart = echarts.init(document.getElementById('{chart_id}'));
      chart.setOption({{
        animation: false,
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'cross' }},
          valueFormatter: {tooltip_value_formatter}
        }},
        legend: {{
          top: 0
        }},
        grid: {{
          left: '8%',
          right: '4%',
          top: 48,
          bottom: 48
        }},
        xAxis: {{
          type: 'category',
          data: {_json_dump(payload.get("x_axis", []))},
          boundaryGap: false
        }},
        yAxis: {{
          type: 'value',
          name: {_json_dump(y_axis_name)},
          scale: true,
          axisLabel: {{
            formatter: {axis_label_formatter}
          }}
        }},
        dataZoom: [
          {{ type: 'inside', start: 0, end: 100 }},
          {{ type: 'slider', start: 0, end: 100, bottom: 10 }}
        ],
        series: [{",".join(series_blocks)}]
      }});
      window.addEventListener('resize', function() {{ chart.resize(); }});
    }})();
    """


def _build_heatmap_chart_script(chart_id: str, payload: dict[str, Any], percent_axis: bool = True) -> str:
    visual_min = -20 if percent_axis else -1
    visual_max = 20 if percent_axis else 1
    tooltip_formatter = (
        """
        params => {
          const value = params.data?.[2];
          if (value == null || Number.isNaN(Number(value))) return '-';
          return `${Number(value).toFixed(2)}%`;
        }
        """
        if percent_axis
        else """
        params => {
          const value = params.data?.[2];
          if (value == null || Number.isNaN(Number(value))) return '-';
          return Number(value).toFixed(2);
        }
        """
    )

    return f"""
    (function() {{
      const chart = echarts.init(document.getElementById('{chart_id}'));
      chart.setOption({{
        animation: false,
        tooltip: {{
          position: 'top',
          formatter: {tooltip_formatter}
        }},
        grid: {{
          left: '8%',
          right: '12%',
          top: 48,
          bottom: 24
        }},
        xAxis: {{
          type: 'category',
          data: {_json_dump(payload.get("x_axis", []))},
          splitArea: {{ show: true }}
        }},
        yAxis: {{
          type: 'category',
          data: {_json_dump(payload.get("y_axis", []))},
          splitArea: {{ show: true }}
        }},
        visualMap: {{
          min: {visual_min},
          max: {visual_max},
          calculable: true,
          orient: 'vertical',
          right: 8,
          top: 'middle',
          inRange: {{
            color: ['#1d4ed8', '#f8fafc', '#b91c1c']
          }}
        }},
        series: [{{
          type: 'heatmap',
          data: {_json_dump(payload.get("data", []))},
          label: {{
            show: true,
            formatter: params => {{
              const value = params.data?.[2];
              if (value == null || Number.isNaN(Number(value))) return '';
              return {("`${Number(value).toFixed(1)}%`" if percent_axis else "Number(value).toFixed(2)")};
            }}
          }},
          emphasis: {{
            itemStyle: {{
              shadowBlur: 10,
              shadowColor: 'rgba(0, 0, 0, 0.3)'
            }}
          }}
        }}]
      }});
      window.addEventListener('resize', function() {{ chart.resize(); }});
    }})();
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
    chart_sections: list[str] = []
    chart_scripts: list[str] = []
    chart_index = 0
    benchmark_series = _normalize_benchmark_series(benchmarks)
    log_panel_html = _build_log_panel(log_lines)

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
    {''.join(chart_scripts)}
  </script>
</body>
</html>
"""

    output.write_text(html_text, encoding="utf-8")

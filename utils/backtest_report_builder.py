from __future__ import annotations

import math
from typing import Any

import pandas as pd
import backtrader as bt

from utils.dict_utils import get_nested_value
from utils.float_int_utils import safe_round
from utils.project_utils import load_daily_data


def build_stock_returns_series(
    stock_code: str, adjust_flag: str, from_date: str = None, to_date: str = None
) -> pd.Series:
    """加载基准数据并计算日收益率序列"""
    if not stock_code:
        return pd.Series(dtype=float)

    benchmark_df = load_daily_data(stock_code, adjust_flag)
    benchmark_df["date"] = pd.to_datetime(benchmark_df["date"])
    benchmark_df = benchmark_df.sort_values("date").reset_index(drop=True)

    if from_date:
        benchmark_df = benchmark_df[benchmark_df["date"] >= pd.Timestamp(from_date)]
    if to_date:
        benchmark_df = benchmark_df[benchmark_df["date"] <= pd.Timestamp(to_date)]

    benchmark_returns = benchmark_df.set_index("date")["close"].pct_change().dropna()
    benchmark_returns.name = stock_code
    return benchmark_returns


def build_returns_series(strategy: bt.Strategy) -> pd.Series:
    """从 Backtrader 策略实例中提取时间序列收益率数据，并返回一个以日期为索引的 Pandas Series。"""
    returns_dict = strategy.analyzers.time_return.get_analysis()
    if not returns_dict:
        return pd.Series(dtype=float)

    returns_series = pd.Series(returns_dict, dtype=float)
    returns_series.index = pd.to_datetime(returns_series.index)
    returns_series = returns_series.sort_index()
    returns_series.name = "strategy_returns"
    return returns_series


def extract_trade_metrics(trade_analysis: Any) -> dict[str, Any]:
    """从 TradeAnalyzer 的结果中提取交易相关的指标，提供默认值以避免 KeyError"""
    total_closed = get_nested_value(trade_analysis, ["total", "closed"], 0) or 0
    won_total = get_nested_value(trade_analysis, ["won", "total"], 0) or 0
    lost_total = get_nested_value(trade_analysis, ["lost", "total"], 0) or 0
    net_total = get_nested_value(trade_analysis, ["pnl", "net", "total"])
    net_average = get_nested_value(trade_analysis, ["pnl", "net", "average"])
    win_rate = (won_total / total_closed * 100) if total_closed else 0.0

    return {
        "trades_total": total_closed,
        "trades_won": won_total,
        "trades_lost": lost_total,
        "win_rate_pct": safe_round(win_rate),
        "net_profit": safe_round(net_total),
        "avg_trade_profit": safe_round(net_average),
    }


def summarize_result(strategy: bt.Strategy, initial_value: float) -> dict[str, Any]:
    """总结回测结果，提取关键指标并格式化输出"""
    final_value = strategy.broker.getvalue()
    total_return_pct = (final_value / initial_value - 1) * 100

    returns_analysis = strategy.analyzers.returns.get_analysis()
    drawdown_analysis = strategy.analyzers.drawdown.get_analysis()
    sharpe_analysis = strategy.analyzers.sharpe.get_analysis()
    trade_analysis = strategy.analyzers.trades.get_analysis()

    drawdown_max = get_nested_value(drawdown_analysis, ["max", "drawdown"])
    drawdown_max_money = get_nested_value(drawdown_analysis, ["max", "moneydown"])
    drawdown_max_len = get_nested_value(drawdown_analysis, ["max", "len"])

    result = {
        "initial_value": safe_round(initial_value),
        "final_value": safe_round(final_value),
        "total_return_pct": safe_round(total_return_pct),
        "annual_return_pct": safe_round(returns_analysis.get("rnorm100")),
        "max_drawdown_pct": safe_round(drawdown_max),
        "drawdown_max_len": safe_round(drawdown_max_len),
        "max_drawdown_amount": safe_round(drawdown_max_money),
        "sharpe_ratio": safe_round(sharpe_analysis.get("sharperatio")),
        "position_days_total": int(getattr(strategy, "position_days_total", 0) or 0),
        "idle_cash_days_total": int(getattr(strategy, "idle_cash_days_total", 0) or 0),
    }
    result.update(extract_trade_metrics(trade_analysis))
    return result


def build_backtest_report_data(
    strategy: bt.Strategy,
    config: dict[str, Any],
    ma: list[int]=[],
) -> list[dict[str, Any]]:
    """
    把 bt.Strategy 数据转为 utils/backtest_report.py generate_backtest_html 方法所需的回测报告数据结构
    :param strategy: 已经运行完成的 bt.Strategy 实例
    :param ma: 需要计算的均线周期列表，例如 [5, 10, 20]
    :param config: 回测配置字典
    """
    returns_series = build_returns_series(strategy)

    report_data: list[dict[str, Any]] = []
    if returns_series.empty:
        return report_data
    
    summary = summarize_result(strategy, config["cash"])
    total_days = summary["position_days_total"] + summary["idle_cash_days_total"]

    summary_metrics = format_summary_metrics(
        [
            {"label": "股票代码", "value": config["code"]},
            {"label": "策略名称", "value": config.get("strategy_name", "my strategy")},
            {"label": "初始资金", "value": summary["initial_value"], "kind": "number"},
            {"label": "期末资产", "value": summary["final_value"], "kind": "number"},
            {
                "label": "总收益率",
                "value": summary["total_return_pct"],
                "kind": "percent",
            },
            {
                "label": "年化收益率",
                "value": summary["annual_return_pct"],
                "kind": "percent",
            },
            {
                "label": "最大回撤",
                "value": summary["max_drawdown_pct"],
                "kind": "percent",
            },
            {
                "label": "最大回撤金额",
                "value": summary["max_drawdown_amount"],
                "kind": "number",
            },
            {"label": "最大回撤周期", "value": summary["drawdown_max_len"]},
            {"label": "夏普比率", "value": summary["sharpe_ratio"], "kind": "number"},
            {"label": "总交易次数", "value": summary["trades_total"]},
            {"label": "盈利次数", "value": summary["trades_won"]},
            {"label": "亏损次数", "value": summary["trades_lost"]},
            {"label": "胜率", "value": summary["win_rate_pct"], "kind": "percent"},
            {"label": "净利润", "value": summary["net_profit"], "kind": "number"},
            {
                "label": "平均每笔净利润",
                "value": summary["avg_trade_profit"],
                "kind": "number",
            },
            {"label": "资金占用天数", "value": summary["position_days_total"]},
            {"label": "资金占用天数占比", "value": (summary["position_days_total"] / total_days * 100) if total_days > 0 else 0, "kind": "percent"},
            {"label": "资金空闲天数", "value": summary["idle_cash_days_total"]},
            {"label": "资金空闲天数占比", "value": (summary["idle_cash_days_total"] / total_days * 100) if total_days > 0 else 0, "kind": "percent"},
        ]
    )
    
    # 加载标的股票日线数据并进行日期过滤
    df = load_daily_data(config["code"], config["adjust_flag"])
    filtered_df = filter_backtest_data(
        df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    )
    filtered_df = filtered_df.copy()
    
    # 计算均线数据并添加到指标线列表中
    indicator_lines = []
    ma_source_df = df.copy()
    ma_source_df["date"] = pd.to_datetime(ma_source_df["date"])
    filtered_dates = pd.to_datetime(filtered_df["date"])
    for m in ma:
        temp = ma_source_df[["date"]].copy()
        temp[f"MA{m}"] = (
            ma_source_df["close"]
            .rolling(window=m, min_periods=1)
            .mean()
            .round(4)
        )
        aligned_ma = (
            temp.set_index("date")
            .reindex(filtered_dates)[f"MA{m}"]
            .round(4)
            .tolist()
        )
        indicator_lines.append({
            "name": f"MA{m}",
            "data": aligned_ma,
        })
    # 计算基准收益率序列
    benchmark_returns = build_stock_returns_series(
        config.get("benchmark_code", ""), config["adjust_flag"], config["from_date"], config["to_date"]
    )
    
    report_data = build_backtrader_report_payload(
        price_data=filtered_df,
        returns_series=returns_series,
        summary_metrics=summary_metrics,
        buy_points=[
            [pd.Timestamp(dt).strftime("%Y-%m-%d"), round(float(price), 4)]
            for dt, price in strategy.buy_markers
        ],
        sell_points=[
            [pd.Timestamp(dt).strftime("%Y-%m-%d"), round(float(price), 4)]
            for dt, price in strategy.sell_markers
        ],
        indicator_lines=indicator_lines,
        benchmark_returns=benchmark_returns if not benchmark_returns.empty else None,
        strategy_name="本策略",
        benchmark_name=config.get("benchmark_code", "基准"),
        asset_name=config["code"],
        buy_sell_subtitle=f"{config['code']} 日线 + 均线信号",
    )
    return report_data


def filter_backtest_data(
    df: pd.DataFrame,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    filtered = df.copy()
    filtered["date"] = pd.to_datetime(filtered["date"])
    filtered = filtered.sort_values("date").reset_index(drop=True)

    if from_date:
        filtered = filtered[filtered["date"] >= pd.Timestamp(from_date)]
    if to_date:
        filtered = filtered[filtered["date"] <= pd.Timestamp(to_date)]

    return filtered.reset_index(drop=True)


def build_cumulative_returns_series(returns_series: pd.Series, name: str) -> pd.Series:
    if returns_series.empty:
        return pd.Series(dtype=float, name=name)

    normalized = returns_series.copy().sort_index()
    cumulative = (1 + normalized).cumprod() - 1
    cumulative.name = name

    first_date = cumulative.index.min()
    start_value = pd.Series([0.0], index=[first_date], name=name)
    cumulative = pd.concat([start_value, cumulative]).sort_index()
    cumulative = cumulative[~cumulative.index.duplicated(keep="first")]
    return cumulative


def build_yearly_returns_series(returns_series: pd.Series, name: str) -> pd.Series:
    if returns_series.empty:
        return pd.Series(dtype=float, name=name)

    normalized = returns_series.copy().sort_index()
    yearly_returns = (1 + normalized).groupby(normalized.index.year).prod() - 1
    yearly_returns.index = yearly_returns.index.astype(str)
    yearly_returns = yearly_returns * 100
    yearly_returns.name = name
    return yearly_returns


def format_summary_metrics(items: list[dict[str, Any]]) -> dict[str, Any]:
    def fmt_number(value: Any, digits: int = 2) -> str:
        if value is None:
            return "N/A"
        return f"{float(value):,.{digits}f}"

    def fmt_percent(value: Any, digits: int = 2) -> str:
        if value is None:
            return "N/A"
        return f"{float(value):.{digits}f}%"

    formatters = {
        "number": fmt_number,
        "percent": fmt_percent,
        "raw": lambda value, digits=2: "N/A" if value is None else value,
    }

    formatted: dict[str, Any] = {}
    for item in items:
        label = str(item["label"])
        value = item.get("value")
        kind = item.get("kind", "raw")
        digits = int(item.get("digits", 2))
        formatter = formatters.get(kind, formatters["raw"])
        formatted[label] = formatter(value, digits)
    return formatted


def align_benchmark_returns(
    returns_series: pd.Series,
    benchmark_returns: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    if returns_series.empty or benchmark_returns.empty:
        return returns_series, benchmark_returns

    aligned_index = returns_series.index.intersection(benchmark_returns.index)
    return returns_series.loc[aligned_index], benchmark_returns.loc[aligned_index]


def build_return_comparison_chart_data(
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series | None = None,
    strategy_name: str = "策略",
    benchmark_name: str = "基准",
    period: str = "cumulative",
) -> pd.Series | dict[str, Any]:
    if period == "yearly":
        strategy_series = build_yearly_returns_series(strategy_returns, strategy_name)
        benchmark_series = (
            build_yearly_returns_series(benchmark_returns, benchmark_name)
            if benchmark_returns is not None and not benchmark_returns.empty
            else None
        )
    else:
        strategy_series = build_cumulative_returns_series(
            strategy_returns, strategy_name
        )
        benchmark_series = (
            build_cumulative_returns_series(benchmark_returns, benchmark_name)
            if benchmark_returns is not None and not benchmark_returns.empty
            else None
        )

    if benchmark_series is None or benchmark_series.empty:
        return strategy_series

    x_axis = sorted(
        set(strategy_series.index.tolist()) | set(benchmark_series.index.tolist())
    )
    if period == "cumulative":
        x_axis = [pd.Timestamp(item).strftime("%Y-%m-%d") for item in x_axis]
        strategy_values = (
            strategy_series.reindex(pd.to_datetime(x_axis)).round(6).tolist()
        )
        benchmark_values = (
            benchmark_series.reindex(pd.to_datetime(x_axis)).round(6).tolist()
        )
    else:
        x_axis = [str(item) for item in x_axis]
        strategy_values = strategy_series.reindex(x_axis).round(6).tolist()
        benchmark_values = benchmark_series.reindex(x_axis).round(6).tolist()

    return {
        "x_axis": x_axis,
        "series": [
            {"name": strategy_name, "data": strategy_values},
            {"name": benchmark_name, "data": benchmark_values},
        ],
    }


def build_kline_chart_data(
    df: pd.DataFrame,
    buy_points: list[list[Any]] | None = None,
    sell_points: list[list[Any]] | None = None,
    indicator_lines: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    chart_df = df.copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"])
    chart_df = chart_df.sort_values("date").reset_index(drop=True)

    return {
        "x_axis": chart_df["date"].dt.strftime("%Y-%m-%d").tolist(),
        "candles": chart_df[["open", "close", "low", "high"]].round(4).values.tolist(),
        "volumes": chart_df["volume"].fillna(0).astype(float).round(4).tolist(),
        "buy_points": buy_points or [],
        "sell_points": sell_points or [],
        "indicator_lines": indicator_lines or [],
    }


def build_price_returns_series(
    price_data: pd.DataFrame, name: str = "标的"
) -> pd.Series:
    if (
        price_data.empty
        or "date" not in price_data.columns
        or "close" not in price_data.columns
    ):
        return pd.Series(dtype=float, name=name)

    frame = price_data.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["date", "close"]).sort_values("date")
    returns = frame.set_index("date")["close"].pct_change().dropna()
    returns.name = name
    return returns


def _annualize_sharpe(
    window_returns: pd.Series, periods_per_year: int = 252
) -> float | None:
    """计算年化夏普比率，假设无风险利率为0"""
    std = window_returns.std()
    if std is None or pd.isna(std) or std == 0:
        return 0
    mean = window_returns.mean()
    if mean is None or pd.isna(mean):
        return 0
    return float(mean / std * math.sqrt(periods_per_year))


def _annualize_sortino(
    window_returns: pd.Series, periods_per_year: int = 252
) -> float | None:
    downside = window_returns[window_returns < 0]
    downside_std = downside.std()
    if downside_std is None or pd.isna(downside_std) or downside_std == 0:
        return 0
    mean = window_returns.mean()
    if mean is None or pd.isna(mean):
        return 0
    return float(mean / downside_std * math.sqrt(periods_per_year))


def build_rolling_beta_chart_data(
    returns_series: pd.Series,
    benchmark_returns: pd.Series,
    window: int = 126,
    name: str = "Rolling Beta to Benchmark",
) -> pd.Series:
    if returns_series.empty or benchmark_returns.empty:
        return pd.Series(dtype=float, name=name)

    strategy_aligned, benchmark_aligned = align_benchmark_returns(
        returns_series, benchmark_returns
    )
    if strategy_aligned.empty or benchmark_aligned.empty:
        return pd.Series(dtype=float, name=name)

    covariance = strategy_aligned.rolling(window).cov(benchmark_aligned)
    variance = benchmark_aligned.rolling(window).var()
    beta = covariance.div(variance.where(variance != 0)).dropna()
    beta.name = name
    return beta


def build_rolling_volatility_chart_data(
    returns_series: pd.Series,
    window: int = 126,
    periods_per_year: int = 252,
    name: str = "Rolling Volatility (6-Months)",
) -> pd.Series:
    if returns_series.empty:
        return pd.Series(dtype=float, name=name)

    rolling = returns_series.sort_index().rolling(window).std() * math.sqrt(
        periods_per_year
    )
    rolling = rolling.dropna()
    rolling.name = name
    return rolling


def build_rolling_sharpe_chart_data(
    returns_series: pd.Series,
    window: int = 126,
    periods_per_year: int = 252,
    name: str = "Rolling Sharpe (6-Months)",
) -> pd.Series:
    if returns_series.empty:
        return pd.Series(dtype=float, name=name)

    rolling = (
        returns_series.sort_index()
        .rolling(window)
        .apply(
            lambda values: _annualize_sharpe(pd.Series(values), periods_per_year),
            raw=False,
        )
    )
    rolling = rolling.dropna()
    rolling.name = name
    return rolling


def build_rolling_sortino_chart_data(
    returns_series: pd.Series,
    window: int = 126,
    periods_per_year: int = 252,
    name: str = "Rolling Sortino (6-Months)",
) -> pd.Series:
    if returns_series.empty:
        return pd.Series(dtype=float, name=name)

    rolling = (
        returns_series.sort_index()
        .rolling(window)
        .apply(
            lambda values: _annualize_sortino(pd.Series(values), periods_per_year),
            raw=False,
        )
    )
    rolling = rolling.dropna()
    rolling.name = name
    return rolling


def build_underwater_series(
    returns_series: pd.Series, name: str = "Underwater"
) -> pd.Series:
    if returns_series.empty:
        return pd.Series(dtype=float, name=name)

    cumulative = (1 + returns_series.sort_index()).cumprod()
    running_peak = cumulative.cummax()
    underwater = cumulative / running_peak - 1
    underwater.name = name
    return underwater


def build_monthly_returns_heatmap(returns_series: pd.Series) -> pd.DataFrame:
    if returns_series.empty:
        return pd.DataFrame()

    normalized = returns_series.sort_index()
    monthly_returns = (1 + normalized).resample("ME").prod() - 1
    if monthly_returns.empty:
        return pd.DataFrame()

    month_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    heatmap = pd.DataFrame(
        index=sorted(monthly_returns.index.year.unique()),
        columns=month_names,
        dtype=float,
    )
    for dt, value in monthly_returns.items():
        heatmap.loc[dt.year, month_names[dt.month - 1]] = round(float(value) * 100, 4)
    heatmap.index = heatmap.index.astype(str)
    return heatmap


def build_report_payload(
    *,
    summary_metrics: dict[str, Any] | None = None,
    buy_sell_chart_data: dict[str, Any] | None = None,
    buy_sell_subtitle: str = "",
    cumulative_chart_data: pd.Series | dict[str, Any] | None = None,
    cumulative_subtitle: str = "策略累计收益率与基准对比",
    yearly_chart_data: pd.Series | dict[str, Any] | None = None,
    yearly_subtitle: str = "按自然年统计的年度收益率",
    extra_charts: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    report_data: list[dict[str, Any]] = []

    if summary_metrics:
        report_data.append({"chart_name": "指标概览", "chart_data": summary_metrics})

    if buy_sell_chart_data:
        report_data.append(
            {
                "chart_name": "买卖点",
                "subtitle": buy_sell_subtitle,
                "chart_data": buy_sell_chart_data,
            }
        )

    if cumulative_chart_data is not None:
        report_data.append(
            {
                "chart_name": "累计收益率",
                "subtitle": cumulative_subtitle,
                "chart_data": cumulative_chart_data,
            }
        )

    if yearly_chart_data is not None:
        report_data.append(
            {
                "chart_name": "年末收益率",
                "subtitle": yearly_subtitle,
                "chart_data": yearly_chart_data,
            }
        )

    if extra_charts:
        report_data.extend(extra_charts)

    return report_data


def build_backtrader_report_payload(
    *,
    price_data: pd.DataFrame,
    returns_series: pd.Series,
    summary_metrics: dict[str, Any],
    buy_points: list[list[Any]] | None = None,
    sell_points: list[list[Any]] | None = None,
    indicator_lines: list[dict[str, Any]] | None = None,
    benchmark_returns: pd.Series | None = None,
    strategy_name: str = "策略",
    benchmark_name: str = "基准",
    asset_name: str = "标的",
    buy_sell_subtitle: str = "",
    cumulative_subtitle: str = "策略累计收益率与基准对比",
    yearly_subtitle: str = "按自然年统计的年度收益率",
    rolling_window: int = 126,
    periods_per_year: int = 252,
) -> list[dict[str, Any]]:
    aligned_returns = returns_series
    aligned_benchmark = benchmark_returns
    if benchmark_returns is not None and not benchmark_returns.empty:
        aligned_returns, aligned_benchmark = align_benchmark_returns(
            returns_series,
            benchmark_returns,
        )

    buy_sell_chart_data = build_kline_chart_data(
        price_data,
        buy_points=buy_points,
        sell_points=sell_points,
        indicator_lines=indicator_lines,
    )
    asset_returns = build_price_returns_series(price_data, asset_name)

    cumulative_chart_data = build_return_comparison_chart_data(
        aligned_returns,
        (
            aligned_benchmark
            if aligned_benchmark is not None and not aligned_benchmark.empty
            else None
        ),
        strategy_name=strategy_name,
        benchmark_name=benchmark_name,
        period="cumulative",
    )
    if not asset_returns.empty:
        asset_cumulative = build_cumulative_returns_series(asset_returns, asset_name)
        if isinstance(cumulative_chart_data, pd.Series):
            strategy_cumulative = cumulative_chart_data
            cumulative_index = sorted(
                set(strategy_cumulative.index.tolist())
                | set(asset_cumulative.index.tolist())
            )
            cumulative_chart_data = {
                "x_axis": [
                    pd.Timestamp(item).strftime("%Y-%m-%d") for item in cumulative_index
                ],
                "series": [
                    {
                        "name": strategy_name,
                        "data": strategy_cumulative.reindex(cumulative_index)
                        .round(6)
                        .tolist(),
                    },
                    {
                        "name": asset_name,
                        "data": asset_cumulative.reindex(cumulative_index)
                        .round(6)
                        .tolist(),
                    },
                ],
            }
        else:
            cumulative_index = pd.to_datetime(cumulative_chart_data.get("x_axis", []))
            cumulative_series = list(cumulative_chart_data.get("series", []))
            merged_index = sorted(
                set(cumulative_index.tolist()) | set(asset_cumulative.index.tolist())
            )
            aligned_series = []
            for item in cumulative_series:
                series_data = pd.Series(
                    item.get("data", []), index=cumulative_index, dtype=float
                )
                aligned_series.append(
                    {
                        "name": item.get("name"),
                        "data": series_data.reindex(merged_index).round(6).tolist(),
                    }
                )
            aligned_series.append(
                {
                    "name": asset_name,
                    "data": asset_cumulative.reindex(merged_index).round(6).tolist(),
                }
            )
            cumulative_chart_data = {
                "x_axis": [
                    pd.Timestamp(item).strftime("%Y-%m-%d") for item in merged_index
                ],
                "series": aligned_series,
            }
    yearly_chart_data = build_return_comparison_chart_data(
        aligned_returns,
        (
            aligned_benchmark
            if aligned_benchmark is not None and not aligned_benchmark.empty
            else None
        ),
        strategy_name=strategy_name,
        benchmark_name=benchmark_name,
        period="yearly",
    )
    if not asset_returns.empty:
        asset_yearly = build_yearly_returns_series(asset_returns, asset_name)
        if isinstance(yearly_chart_data, pd.Series):
            strategy_yearly = yearly_chart_data
            yearly_index = sorted(
                set(strategy_yearly.index.tolist()) | set(asset_yearly.index.tolist())
            )
            yearly_chart_data = {
                "x_axis": [str(item) for item in yearly_index],
                "series": [
                    {
                        "name": strategy_name,
                        "data": strategy_yearly.reindex(yearly_index).round(6).tolist(),
                    },
                    {
                        "name": asset_name,
                        "data": asset_yearly.reindex(yearly_index).round(6).tolist(),
                    },
                ],
            }
        else:
            yearly_index = pd.Index(yearly_chart_data.get("x_axis", []), dtype=str)
            yearly_series = list(yearly_chart_data.get("series", []))
            merged_index = sorted(
                set(yearly_index.tolist()) | set(asset_yearly.index.tolist())
            )
            aligned_series = []
            for item in yearly_series:
                series_data = pd.Series(
                    item.get("data", []), index=yearly_index, dtype=float
                )
                aligned_series.append(
                    {
                        "name": item.get("name"),
                        "data": series_data.reindex(merged_index).round(6).tolist(),
                    }
                )
            aligned_series.append(
                {
                    "name": asset_name,
                    "data": asset_yearly.reindex(merged_index).round(6).tolist(),
                }
            )
            yearly_chart_data = {
                "x_axis": [str(item) for item in merged_index],
                "series": aligned_series,
            }

    extra_charts: list[dict[str, Any]] = []

    rolling_volatility = build_rolling_volatility_chart_data(
        aligned_returns,
        window=rolling_window,
        periods_per_year=periods_per_year,
        name=strategy_name,
    )
    rolling_volatility_chart_data: pd.Series | dict[str, Any] | None = None
    if not rolling_volatility.empty:
        rolling_volatility_chart_data = rolling_volatility

    asset_rolling_volatility = build_rolling_volatility_chart_data(
        asset_returns,
        window=rolling_window,
        periods_per_year=periods_per_year,
        name=asset_name,
    )

    if aligned_benchmark is not None and not aligned_benchmark.empty:
        benchmark_rolling_volatility = build_rolling_volatility_chart_data(
            aligned_benchmark,
            window=rolling_window,
            periods_per_year=periods_per_year,
            name=benchmark_name,
        )
        if not benchmark_rolling_volatility.empty:
            volatility_index = sorted(
                set(rolling_volatility.index.tolist())
                | set(benchmark_rolling_volatility.index.tolist())
                | set(asset_rolling_volatility.index.tolist())
            )
            rolling_volatility_chart_data = {
                "x_axis": [
                    pd.Timestamp(item).strftime("%Y-%m-%d") for item in volatility_index
                ],
                "series": [
                    {
                        "name": strategy_name,
                        "data": rolling_volatility.reindex(volatility_index)
                        .round(6)
                        .tolist(),
                    },
                    {
                        "name": benchmark_name,
                        "data": benchmark_rolling_volatility.reindex(volatility_index)
                        .round(6)
                        .tolist(),
                    },
                    {
                        "name": asset_name,
                        "data": asset_rolling_volatility.reindex(volatility_index)
                        .round(6)
                        .tolist(),
                    },
                ],
            }
    elif not asset_rolling_volatility.empty:
        volatility_index = sorted(
            set(rolling_volatility.index.tolist())
            | set(asset_rolling_volatility.index.tolist())
        )
        rolling_volatility_chart_data = {
            "x_axis": [
                pd.Timestamp(item).strftime("%Y-%m-%d") for item in volatility_index
            ],
            "series": [
                {
                    "name": strategy_name,
                    "data": rolling_volatility.reindex(volatility_index)
                    .round(6)
                    .tolist(),
                },
                {
                    "name": asset_name,
                    "data": asset_rolling_volatility.reindex(volatility_index)
                    .round(6)
                    .tolist(),
                },
            ],
        }

    if rolling_volatility_chart_data is not None:
        extra_charts.append(
            {
                "chart_name": "rolling Volatility(6-months)",
                "subtitle": "基于近 6 个月滚动窗口计算的年化波动率",
                "chart_data": rolling_volatility_chart_data,
            }
        )

    rolling_sharpe = build_rolling_sharpe_chart_data(
        aligned_returns,
        window=rolling_window,
        periods_per_year=periods_per_year,
    )
    if not rolling_sharpe.empty:
        extra_charts.append(
            {
                "chart_name": "Rolling Sharpe(6-Months)",
                "subtitle": "基于近 6 个月滚动窗口计算的年化 Sharpe",
                "chart_data": rolling_sharpe,
            }
        )

    rolling_sortino = build_rolling_sortino_chart_data(
        aligned_returns,
        window=rolling_window,
        periods_per_year=periods_per_year,
    )
    if not rolling_sortino.empty:
        extra_charts.append(
            {
                "chart_name": "Rolling Sortino(6-Months)",
                "subtitle": "基于近 6 个月滚动窗口计算的年化 Sortino",
                "chart_data": rolling_sortino,
            }
        )

    underwater = build_underwater_series(aligned_returns, "Underwater")
    if not underwater.empty:
        extra_charts.append(
            {
                "chart_name": "Underwater Plot",
                "subtitle": "策略净值相对历史高点的回撤轨迹",
                "chart_data": underwater,
            }
        )

    monthly_returns = build_monthly_returns_heatmap(aligned_returns)
    if not monthly_returns.empty:
        extra_charts.append(
            {
                "chart_name": "Monthly Returns(%)",
                "subtitle": "按自然月统计的收益率热力图",
                "chart_data": monthly_returns,
            }
        )

    if aligned_benchmark is not None and not aligned_benchmark.empty:
        rolling_beta = build_rolling_beta_chart_data(
            aligned_returns,
            aligned_benchmark,
            window=rolling_window,
        )
        if not rolling_beta.empty:
            extra_charts.append(
                {
                    "chart_name": "rolling beta to benchmark",
                    "subtitle": f"基于近 6 个月滚动窗口计算的相对 {benchmark_name} Beta",
                    "chart_data": rolling_beta,
                }
            )

    return build_report_payload(
        summary_metrics=summary_metrics,
        buy_sell_chart_data=buy_sell_chart_data,
        buy_sell_subtitle=buy_sell_subtitle,
        cumulative_chart_data=cumulative_chart_data,
        cumulative_subtitle=cumulative_subtitle,
        yearly_chart_data=yearly_chart_data,
        yearly_subtitle=yearly_subtitle,
        extra_charts=extra_charts,
    )

from __future__ import annotations

import math
from typing import Any

import pandas as pd
import backtrader as bt

from analysis.context_enricher import (
    build_strategy_enhancement_patch,
    enrich_single_stock_context,
)
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


def _is_unadjusted_flag(adjust_flag: str) -> bool:
    return str(adjust_flag or "").strip().lower() in {"", "3", "bfq", "cq", "raw", "none"}


def describe_adjust_flag(adjust_flag: str) -> str:
    normalized = str(adjust_flag or "").strip().lower()
    if normalized == "dypre":
        return "Dypre 动态前复权（信号按前复权、成交与估值按不复权，除权日同步调整持仓股数）"
    if normalized in {"", "3", "bfq", "cq", "raw", "none"}:
        return "不复权（cq）"
    if normalized in {"2", "qfq"}:
        return "前复权（qfq）"
    if normalized in {"1", "hfq"}:
        return "后复权（hfq）"
    return str(adjust_flag or "")


def attach_ex_right_close_column(
    price_df: pd.DataFrame,
    stock_code: str,
    adjust_flag: str,
) -> pd.DataFrame:
    frame = price_df.copy()
    frame["date"] = pd.to_datetime(frame["date"])

    if _is_unadjusted_flag(adjust_flag):
        frame["ex_right_close"] = pd.to_numeric(frame["close"], errors="coerce")
        return frame

    try:
        ex_right_df = load_daily_data(stock_code, "cq")
    except FileNotFoundError:
        frame["ex_right_close"] = pd.NA
        return frame

    ex_right_frame = ex_right_df[["date", "close"]].copy()
    ex_right_frame["date"] = pd.to_datetime(ex_right_frame["date"])
    ex_right_frame = ex_right_frame.rename(columns={"close": "ex_right_close"})
    ex_right_frame["ex_right_close"] = pd.to_numeric(
        ex_right_frame["ex_right_close"], errors="coerce"
    )
    return frame.merge(ex_right_frame, on="date", how="left")


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


def extract_trade_metrics(
    trade_analysis: Any,
    strategy: bt.Strategy | None = None,
) -> dict[str, Any]:
    """从 TradeAnalyzer 的结果中提取交易相关的指标，提供默认值以避免 KeyError"""
    total_closed = get_nested_value(trade_analysis, ["total", "closed"], 0) or 0
    won_total = get_nested_value(trade_analysis, ["won", "total"], 0) or 0
    lost_total = get_nested_value(trade_analysis, ["lost", "total"], 0) or 0
    net_total = get_nested_value(trade_analysis, ["pnl", "net", "total"])
    net_average = get_nested_value(trade_analysis, ["pnl", "net", "average"])

    # 一些自定义成交/复权场景下，TradeAnalyzer 可能拿不到闭合交易。
    # 这时回退到策略自身维护的成交统计，避免汇总指标误报为 0。
    fallback_total = int(getattr(strategy, "completed_trades_total", 0) or 0)
    fallback_sell_orders = int(getattr(strategy, "completed_sell_orders", 0) or 0)
    fallback_sell_markers = len(getattr(strategy, "sell_markers", []) or [])
    fallback_total = max(fallback_total, fallback_sell_orders, fallback_sell_markers)
    if total_closed <= 0 and fallback_total > 0:
        total_closed = fallback_total
        if int(getattr(strategy, "completed_trades_total", 0) or 0) > 0:
            won_total = int(getattr(strategy, "completed_trades_won", 0) or 0)
            lost_total = int(getattr(strategy, "completed_trades_lost", 0) or 0)
            if won_total + lost_total > total_closed:
                won_total = min(won_total, total_closed)
                lost_total = min(lost_total, total_closed - won_total)
            net_total = float(
                getattr(strategy, "completed_trade_net_profit", 0.0) or 0.0
            )
            net_average = (net_total / total_closed) if total_closed else None
        else:
            won_total = int(getattr(strategy, "completed_sell_estimated_won", 0) or 0)
            lost_total = int(getattr(strategy, "completed_sell_estimated_lost", 0) or 0)
            if won_total + lost_total > total_closed:
                won_total = min(won_total, total_closed)
                lost_total = min(lost_total, total_closed - won_total)
            net_total = float(
                getattr(strategy, "completed_sell_estimated_net_profit", 0.0) or 0.0
            )
            net_average = (net_total / total_closed) if total_closed else None

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
        "buy_signals_total": int(getattr(strategy, "buy_signals_total", 0) or 0),
        "buy_signals_blocked": int(getattr(strategy, "buy_signals_blocked", 0) or 0),
    }
    result.update(extract_trade_metrics(trade_analysis, strategy=strategy))
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
    
    # 加载标的股票日线数据并进行日期过滤
    df = load_daily_data(config["code"], config["adjust_flag"])
    df = attach_ex_right_close_column(
        df,
        stock_code=config["code"],
        adjust_flag=config["adjust_flag"],
    )
    filtered_df = filter_backtest_data(
        df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    )
    filtered_df = filtered_df.copy()
    external_context = enrich_single_stock_context(config, filtered_df)
    
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
    optimized_chart_data = build_optimized_trade_chart_data(
        source_df=df,
        filtered_df=filtered_df,
        config=config,
        indicator_lines=indicator_lines,
        ma_periods=ma,
    )
    optimized_chart_data = build_enhanced_trade_chart_data(
        filtered_df=filtered_df,
        optimized_chart_data=optimized_chart_data,
        external_context=external_context,
    )
    next_trade_plan_by_position = {
        "empty": extract_next_trade_plan_from_chart_data(
            optimized_chart_data,
            current_position="empty",
        ),
        "hold": extract_next_trade_plan_from_chart_data(
            optimized_chart_data,
            current_position="hold",
        ),
    }
    empty_entry_timing = build_empty_entry_timing_plan(
        source_df=df,
        config=config,
        ma_periods=ma,
    )
    if next_trade_plan_by_position.get("empty") and empty_entry_timing:
        next_trade_plan_by_position["empty"]["entry_timing"] = empty_entry_timing

    summary_items = [
        {"label": "股票代码", "value": config["code"]},
        {"label": "策略名称", "value": config.get("strategy_name", "my strategy")},
        {"label": "复权口径", "value": describe_adjust_flag(config.get("adjust_flag"))},
        {
            "label": "均线说明",
            "value": "快线看短期节奏，慢线看中期趋势；快线强于慢线通常表示趋势偏强。",
        },
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
        {"label": "买点触发次数", "value": summary["buy_signals_total"]},
        {"label": "补丁阻止买入次数", "value": summary["buy_signals_blocked"]},
        {"label": "资金占用天数", "value": summary["position_days_total"]},
        {"label": "资金占用天数占比", "value": (summary["position_days_total"] / total_days * 100) if total_days > 0 else 0, "kind": "percent"},
        {"label": "资金空闲天数", "value": summary["idle_cash_days_total"]},
        {"label": "资金空闲天数占比", "value": (summary["idle_cash_days_total"] / total_days * 100) if total_days > 0 else 0, "kind": "percent"},
    ]
    summary_items.extend(
        _build_external_context_summary_items(external_context)
    )
    empty_plan = next_trade_plan_by_position.get("empty")
    hold_plan = next_trade_plan_by_position.get("hold")
    if empty_plan:
        summary_items.extend(
            [
                {"label": "空仓-下一交易日策略", "value": empty_plan["display_action"]},
                {"label": "空仓-预判摘要", "value": empty_plan["summary"]},
            ]
        )
        empty_entry_timing = empty_plan.get("entry_timing")
        if isinstance(empty_entry_timing, dict) and empty_entry_timing:
            summary_items.extend(
                [
                    {"label": "空仓-建仓时机", "value": empty_entry_timing.get("label", "")},
                    {"label": "空仓-建仓提示", "value": empty_entry_timing.get("summary", "")},
                ]
            )
    if hold_plan:
        summary_items.extend(
            [
                {"label": "持仓-下一交易日策略", "value": hold_plan["display_action"]},
                {"label": "持仓-预判摘要", "value": hold_plan["summary"]},
            ]
        )
    summary_metrics = format_summary_metrics(summary_items)

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
    if optimized_chart_data:
        report_data.append(
            {
                "chart_name": "优化买卖点",
                "subtitle": "基于动态前复权主策略叠加趋势确认、回撤保护、不追高过滤，并结合新闻、资金流和财报约束的增强视角。",
                "chart_data": optimized_chart_data,
            }
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
    advice_entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    chart_df = df.copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"])
    chart_df = chart_df.sort_values("date").reset_index(drop=True)

    return {
        "x_axis": chart_df["date"].dt.strftime("%Y-%m-%d").tolist(),
        "candles": chart_df[["open", "close", "low", "high"]].round(4).values.tolist(),
        "ex_right_closes": (
            [
                None if pd.isna(value) else round(float(value), 4)
                for value in chart_df["ex_right_close"].tolist()
            ]
            if "ex_right_close" in chart_df.columns
            else [None] * len(chart_df)
        ),
        "volumes": chart_df["volume"].fillna(0).astype(float).round(4).tolist(),
        "buy_points": buy_points or [],
        "sell_points": sell_points or [],
        "indicator_lines": indicator_lines or [],
        "advice_entries": advice_entries or [],
    }


def _build_optimized_signal_frame(
    source_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    ma_periods: list[int] | None = None,
) -> pd.DataFrame:
    full_df = source_df.copy()
    full_df["date"] = pd.to_datetime(full_df["date"])
    full_df = full_df.sort_values("date").reset_index(drop=True)

    fast_period = int(config.get("fast") or (ma_periods[0] if ma_periods else 8))
    slow_period = int(
        config.get("slow")
        or (ma_periods[1] if ma_periods and len(ma_periods) > 1 else max(fast_period * 3, 20))
    )

    full_df["fast_ma"] = full_df["close"].rolling(window=fast_period, min_periods=1).mean()
    full_df["slow_ma"] = full_df["close"].rolling(window=slow_period, min_periods=1).mean()
    full_df["prev_fast_ma"] = full_df["fast_ma"].shift(1)
    full_df["prev_close"] = full_df["close"].shift(1)
    full_df["momentum_3d"] = full_df["close"].pct_change(3)
    full_df["momentum_5d"] = full_df["close"].pct_change(5)
    full_df["turn_ma20"] = full_df["turn"].rolling(window=20, min_periods=5).mean()
    full_df["recent_high_20"] = full_df["close"].rolling(window=20, min_periods=5).max()
    full_df["recent_low_20"] = full_df["close"].rolling(window=20, min_periods=5).min()
    full_df["up_days_4"] = (
        (full_df["close"] > full_df["close"].shift(1)).astype(int).rolling(window=4, min_periods=4).sum()
    )
    full_df["close_below_fast_2d"] = (
        (full_df["close"] < full_df["fast_ma"]).astype(int).rolling(window=2, min_periods=2).sum()
    )

    filtered_dates = pd.to_datetime(filtered_df["date"])
    work_df = full_df.set_index("date").reindex(filtered_dates).reset_index().rename(
        columns={"index": "date"}
    )
    work_df[["open", "high", "low", "close", "volume", "turn"]] = filtered_df[
        ["open", "high", "low", "close", "volume", "turn"]
    ].reset_index(drop=True)
    if "ex_right_close" in filtered_df.columns:
        work_df["ex_right_close"] = filtered_df["ex_right_close"].reset_index(drop=True)
    return work_df


_NEXT_TRADE_ACTION_LABELS = {
    "buy": "偏买入",
    "sell": "偏卖出",
    "hold": "偏持有",
    "watch_buy": "观察买点",
    "observe": "继续观察",
}
_NEXT_TRADE_ACTION_SUMMARIES = {
    "buy": "当前更适合准备买入，优先等确认后执行。",
    "sell": "当前更适合准备卖出，优先控制回撤或落袋。",
    "hold": "当前更适合继续持有，等待更明确的退出信号。",
    "watch_buy": "当前接近买点，先观察确认，不要抢跑。",
    "observe": "当前没有明确买卖信号，继续观察即可。",
}


def _normalize_trade_plan_position(current_position: str) -> str:
    normalized = str(current_position or "").strip().lower()
    alias_map = {
        "": "auto",
        "auto": "auto",
        "empty": "empty",
        "flat": "empty",
        "none": "empty",
        "hold": "hold",
        "holding": "hold",
        "position": "hold",
    }
    return alias_map.get(normalized, "auto")


def _rewrite_trade_plan_for_position(
    action: str,
    reason: str,
    current_position: str,
) -> tuple[str, str]:
    normalized_position = _normalize_trade_plan_position(current_position)
    if normalized_position == "auto":
        return action, reason

    if normalized_position == "empty":
        if action == "sell":
            return "observe", "当前实际空仓，卖出信号无需执行，继续观察下一次买点。"
        if action == "hold":
            return "observe", "当前实际空仓，不执行持有建议，继续观察即可。"
        if action == "observe":
            return "observe", "当前实际空仓，暂时没有明确买点，继续观察即可。"
        return action, reason

    if action == "sell":
        return action, reason
    if action == "buy":
        return "hold", "当前实际持仓，买入信号可作为加仓参考，默认继续持有观察。"
    if action == "watch_buy":
        return "hold", "当前实际持仓，观察买点不作为新开仓信号，继续持有观察。"
    if action == "observe":
        return "hold", "当前实际持仓，暂无明确卖点，继续持有观察。"
    return action, reason


def extract_next_trade_plan_from_chart_data(
    chart_data: dict[str, Any] | None,
    current_position: str = "auto",
) -> dict[str, Any]:
    if not isinstance(chart_data, dict):
        return {}

    advice_entries = chart_data.get("advice_entries") or []
    if not advice_entries:
        return {}

    latest_entry = advice_entries[-1]
    raw_action = str(latest_entry.get("action", "")).strip().lower()
    if not raw_action:
        return {}
    raw_reason = str(latest_entry.get("reason", "")).strip()
    raw_summary = str(latest_entry.get("summary", "")).strip()
    action, reason = _rewrite_trade_plan_for_position(
        raw_action,
        raw_reason,
        current_position=current_position,
    )

    display_action = _NEXT_TRADE_ACTION_LABELS.get(action, action)
    latest_date = str(latest_entry.get("date", "")).strip()
    latest_summary = (
        raw_summary
        if action == raw_action and raw_summary
        else _NEXT_TRADE_ACTION_SUMMARIES.get(action, raw_summary)
    )

    summary_prefix = "基于最新趋势结构，"
    if latest_date:
        summary_prefix = f"基于 {latest_date} 收盘后的趋势结构，"

    return {
        "as_of_date": latest_date,
        "session_label": "下一交易日",
        "action": action,
        "display_action": display_action,
        "title": f"下一交易日{display_action}",
        "summary": summary_prefix + latest_summary if latest_summary else summary_prefix,
        "reason": reason,
    }


def build_empty_entry_timing_plan(
    source_df: pd.DataFrame,
    config: dict[str, Any],
    ma_periods: list[int] | None = None,
) -> dict[str, Any]:
    filtered_df = filter_backtest_data(
        source_df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    )
    if filtered_df.empty:
        return {}

    work_df = _build_optimized_signal_frame(
        source_df=source_df,
        filtered_df=filtered_df,
        config=config,
        ma_periods=ma_periods,
    )
    if work_df.empty:
        return {}

    latest_row = work_df.iloc[-1]
    close_price = float(latest_row["close"])
    fast_ma = latest_row["fast_ma"]
    slow_ma = latest_row["slow_ma"]
    prev_fast_ma = latest_row["prev_fast_ma"]
    momentum_3d = latest_row["momentum_3d"]
    turn_ma20 = latest_row["turn_ma20"]
    recent_high_20 = latest_row["recent_high_20"]
    recent_low_20 = latest_row["recent_low_20"]
    up_days_4 = latest_row["up_days_4"]

    bullish_trend = (
        pd.notna(fast_ma)
        and pd.notna(slow_ma)
        and close_price > float(slow_ma)
        and float(fast_ma) >= float(slow_ma)
        and (pd.isna(prev_fast_ma) or float(fast_ma) >= float(prev_fast_ma))
    )
    momentum_ok = (
        (pd.notna(momentum_3d) and float(momentum_3d) >= 0.01)
        or (pd.notna(up_days_4) and float(up_days_4) >= 3)
    )
    liquidity_ok = pd.isna(turn_ma20) or float(latest_row["turn"]) >= float(turn_ma20) * 0.8
    pullback_ok = (
        pd.notna(recent_high_20)
        and close_price <= float(recent_high_20) * 0.98
    ) or (
        pd.notna(fast_ma) and close_price <= float(fast_ma) * 1.015
    )
    chase_too_far = (
        pd.notna(recent_low_20)
        and float(recent_low_20) > 0
        and (close_price / float(recent_low_20) - 1) > 0.18
    )

    reference_parts: list[str] = []
    if pd.notna(fast_ma):
        reference_parts.append(f"快线参考位 {float(fast_ma):.2f}")
    if pd.notna(slow_ma):
        reference_parts.append(f"慢线防守位 {float(slow_ma):.2f}")
    fast_value_text = f"{float(fast_ma):.2f}" if pd.notna(fast_ma) else "N/A"
    slow_value_text = f"{float(slow_ma):.2f}" if pd.notna(slow_ma) else "N/A"
    ma_status_text = f"当日快线={fast_value_text}、慢线={slow_value_text}。"

    if bullish_trend and momentum_ok and liquidity_ok and pullback_ok and not chase_too_far:
        label = "可考虑试探建仓"
        summary = (
            f"{ma_status_text}"
            f"明日若价格继续站在慢线 {float(slow_ma):.2f} 上方，"
            f"且没有明显高开脱离快线 {float(fast_ma):.2f}，可考虑分批试探建仓。"
        )
    elif bullish_trend and momentum_ok and liquidity_ok and chase_too_far:
        label = "等待回踩再建仓"
        pullback_price = None
        if pd.notna(fast_ma):
            pullback_price = float(fast_ma) * 1.015
        if pd.notna(recent_high_20):
            candidate = float(recent_high_20) * 0.98
            pullback_price = min(pullback_price, candidate) if pullback_price else candidate
        pullback_text = f"{pullback_price:.2f}" if pullback_price else "快线附近"
        summary = (
            f"{ma_status_text}"
            "趋势和动能都不差，但当前位置偏高。"
            f"明日优先等回踩到 {pullback_text} 一带，再考虑建仓，不追高。"
        )
    elif bullish_trend and not momentum_ok:
        label = "等待动能确认"
        summary = (
            f"{ma_status_text}"
            "长线趋势已转暖，但短线动能还不够。"
            "明日优先等近 4 日上涨天数达到 3 天以上，或 3 日动能转正到 1% 附近后，再考虑建仓。"
        )
    elif bullish_trend and momentum_ok and not liquidity_ok:
        label = "等待量能恢复"
        turn_reference = float(turn_ma20) * 0.8 if pd.notna(turn_ma20) else None
        turn_text = f"{turn_reference:.2f}" if turn_reference is not None else "20 日均量附近"
        summary = (
            f"{ma_status_text}"
            "趋势条件接近满足，但量能/换手偏弱。"
            f"明日优先等换手回到 {turn_text} 以上，再考虑建仓。"
        )
    else:
        label = "等待趋势翻多"
        slow_text = f"{float(slow_ma):.2f}" if pd.notna(slow_ma) else "慢线之上"
        summary = (
            "当前均线结构还没完全转强。"
            f"{ma_status_text}"
            f"明日优先观察收盘重新站上 {slow_text}，且快线不弱于慢线后，再考虑建仓。"
        )

    return {
        "label": label,
        "summary": summary,
        "reference": "；".join(reference_parts),
    }


def build_optimized_trade_chart_data(
    source_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    config: dict[str, Any],
    indicator_lines: list[dict[str, Any]] | None = None,
    ma_periods: list[int] | None = None,
) -> dict[str, Any]:
    if filtered_df.empty:
        return {}
    stop_loss_pct = float(config.get("stop_loss_pct", 0.1))
    optimized_stop_loss_pct = min(max(stop_loss_pct * 0.8, 0.04), 0.12)
    optimized_trailing_stop_pct = min(max(stop_loss_pct * 0.7, 0.05), 0.12)
    work_df = _build_optimized_signal_frame(
        source_df=source_df,
        filtered_df=filtered_df,
        config=config,
        ma_periods=ma_periods,
    )

    buy_points: list[list[Any]] = []
    sell_points: list[list[Any]] = []
    advice_entries: list[dict[str, Any]] = []

    holding = False
    entry_price: float | None = None
    highest_close_since_entry: float | None = None

    for index, row in work_df.iterrows():
        date_text = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
        close_price = float(row["close"])
        ex_right_close = row.get("ex_right_close")
        fast_ma = row["fast_ma"]
        slow_ma = row["slow_ma"]
        prev_fast_ma = row["prev_fast_ma"]
        momentum_3d = row["momentum_3d"]
        turn_ma20 = row["turn_ma20"]
        recent_high_20 = row["recent_high_20"]
        recent_low_20 = row["recent_low_20"]
        up_days_4 = row["up_days_4"]
        close_below_fast_2d = row["close_below_fast_2d"]

        bullish_trend = (
            pd.notna(fast_ma)
            and pd.notna(slow_ma)
            and close_price > float(slow_ma)
            and float(fast_ma) >= float(slow_ma)
            and (pd.isna(prev_fast_ma) or float(fast_ma) >= float(prev_fast_ma))
        )
        momentum_ok = (
            (pd.notna(momentum_3d) and float(momentum_3d) >= 0.01)
            or (pd.notna(up_days_4) and float(up_days_4) >= 3)
        )
        liquidity_ok = pd.isna(turn_ma20) or float(row["turn"]) >= float(turn_ma20) * 0.8
        pullback_ok = (
            pd.notna(recent_high_20)
            and close_price <= float(recent_high_20) * 0.98
        ) or (
            pd.notna(fast_ma) and close_price <= float(fast_ma) * 1.015
        )
        chase_too_far = (
            pd.notna(recent_low_20)
            and float(recent_low_20) > 0
            and (close_price / float(recent_low_20) - 1) > 0.18
        )

        if not holding and bullish_trend and momentum_ok and liquidity_ok and pullback_ok and not chase_too_far:
            buy_points.append([date_text, round(close_price, 4)])
            advice_entries.append(
                {
                    "date": date_text,
                    "action": "buy",
                    "title": "优化买入",
                    "price": f"{close_price:.2f}",
                    "ex_right_price": (
                        f"{float(ex_right_close):.2f}" if pd.notna(ex_right_close) else "-"
                    ),
                    "summary": "趋势确认后择机低吸，不追高。",
                    "reason": (
                        f"快线 {float(fast_ma):.2f} 站上慢线 {float(slow_ma):.2f}，"
                        f"近 4 天上涨天数={int(up_days_4) if pd.notna(up_days_4) else 0}，"
                        "同时价格没有脱离短线太远，适合执行优化买入。"
                    ),
                    "is_signal": True,
                }
            )
            holding = True
            entry_price = close_price
            highest_close_since_entry = close_price
            continue

        if holding:
            highest_close_since_entry = max(highest_close_since_entry or close_price, close_price)
            stop_loss_price = (entry_price or close_price) * (1 - optimized_stop_loss_pct)
            trailing_stop_price = (highest_close_since_entry or close_price) * (1 - optimized_trailing_stop_pct)
            trend_break = (
                (pd.notna(close_below_fast_2d) and float(close_below_fast_2d) >= 2)
                or (pd.notna(fast_ma) and pd.notna(slow_ma) and float(fast_ma) < float(slow_ma))
            )
            take_profit_soft = (
                entry_price is not None
                and close_price >= entry_price * 1.15
                and pd.notna(momentum_3d)
                and float(momentum_3d) < 0
            )

            sell_reason: str | None = None
            if close_price <= stop_loss_price:
                sell_reason = (
                    f"跌破优化止损价 {stop_loss_price:.2f}，"
                    "优先控制回撤。"
                )
            elif close_price <= trailing_stop_price and close_price > (entry_price or close_price):
                sell_reason = (
                    f"从持仓高点回撤超过 {optimized_trailing_stop_pct * 100:.1f}%，"
                    "建议先落袋。"
                )
            elif trend_break:
                sell_reason = "短线趋势走弱，连续跌破快线或快慢线重新转弱，建议退出。"
            elif take_profit_soft:
                sell_reason = "已有明显浮盈且短线动能转弱，适合先兑现收益。"

            if sell_reason:
                sell_points.append([date_text, round(close_price, 4)])
                advice_entries.append(
                    {
                        "date": date_text,
                        "action": "sell",
                        "title": "优化卖出",
                        "price": f"{close_price:.2f}",
                        "ex_right_price": (
                            f"{float(ex_right_close):.2f}" if pd.notna(ex_right_close) else "-"
                        ),
                        "summary": "优先保护利润和回撤，不恋战。",
                        "reason": sell_reason,
                        "is_signal": True,
                    }
                )
                holding = False
                entry_price = None
                highest_close_since_entry = None

    latest_row = work_df.iloc[-1]
    latest_date = pd.Timestamp(latest_row["date"]).strftime("%Y-%m-%d")
    latest_close = float(latest_row["close"])
    latest_ex_right_close = latest_row.get("ex_right_close")
    if holding:
        advice_entries.append(
            {
                "date": latest_date,
                "action": "hold",
                "title": "优化持有",
                "price": f"{latest_close:.2f}",
                "ex_right_price": (
                    f"{float(latest_ex_right_close):.2f}" if pd.notna(latest_ex_right_close) else "-"
                ),
                "summary": "趋势尚未破坏，继续持有观察。",
                "reason": "当前优化规则下仍未触发止损、回撤保护或趋势转弱卖点。",
                "is_signal": False,
            }
        )
    elif (
        pd.notna(latest_row["fast_ma"])
        and pd.notna(latest_row["slow_ma"])
        and float(latest_row["close"]) > float(latest_row["slow_ma"])
    ):
        advice_entries.append(
            {
                "date": latest_date,
                "action": "watch_buy",
                "title": "优化观察",
                "price": f"{latest_close:.2f}",
                "ex_right_price": (
                    f"{float(latest_ex_right_close):.2f}" if pd.notna(latest_ex_right_close) else "-"
                ),
                "summary": "趋势转暖，但仍需等更好的入场点。",
                "reason": "长线趋势不差，但当前还没同时满足低吸位置与动量确认，先观察。",
                "is_signal": True,
            }
        )
    else:
        advice_entries.append(
            {
                "date": latest_date,
                "action": "observe",
                "title": "继续观察",
                "price": f"{latest_close:.2f}",
                "ex_right_price": (
                    f"{float(latest_ex_right_close):.2f}" if pd.notna(latest_ex_right_close) else "-"
                ),
                "summary": "当前不主动开仓。",
                "reason": "趋势与位置尚未形成高质量买点，保持等待更稳妥。",
                "is_signal": False,
            }
        )

    return build_kline_chart_data(
        filtered_df,
        buy_points=buy_points,
        sell_points=sell_points,
        indicator_lines=indicator_lines,
        advice_entries=advice_entries,
    )


def build_enhanced_trade_chart_data(
    filtered_df: pd.DataFrame,
    optimized_chart_data: dict[str, Any] | None,
    external_context: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(optimized_chart_data, dict):
        return {}
    if filtered_df.empty:
        return optimized_chart_data

    advice_entries = list(optimized_chart_data.get("advice_entries") or [])
    if not advice_entries:
        return optimized_chart_data

    latest_entry = dict(advice_entries[-1])
    base_plan = extract_next_trade_plan_from_chart_data(
        {"advice_entries": [latest_entry]},
        current_position="auto",
    )
    enhancement_patch = build_strategy_enhancement_patch(
        base_plan=base_plan,
        external_context=external_context,
    )
    if not enhancement_patch:
        return optimized_chart_data

    latest_entry["enhancement_action"] = enhancement_patch.get("action")
    latest_entry["enhancement_title"] = enhancement_patch.get("title")
    latest_entry["enhancement_summary"] = enhancement_patch.get("summary")
    latest_entry["enhancement_reason"] = enhancement_patch.get("reason")
    latest_entry["enhancement_display_action"] = enhancement_patch.get("display_action")
    latest_entry["enhancement_score"] = enhancement_patch.get("enhancement_score")
    latest_entry["enhancement_label"] = enhancement_patch.get("enhancement_label")
    latest_entry["news_sentiment_label"] = enhancement_patch.get("news_sentiment_label")
    latest_entry["fund_flow_label"] = enhancement_patch.get("fund_flow_label")
    latest_entry["financial_label"] = enhancement_patch.get("financial_label")
    advice_entries[-1] = latest_entry

    return {
        **optimized_chart_data,
        "advice_entries": advice_entries,
    }


def _build_external_context_summary_items(
    external_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(external_context, dict):
        return []

    items: list[dict[str, Any]] = []
    news_context = external_context.get("news", {})
    fund_context = external_context.get("fund_flow", {})
    financial_context = external_context.get("financials", {})

    if isinstance(news_context, dict) and news_context.get("status") == "ok":
        items.append(
            {
                "label": "新闻情绪",
                "value": news_context.get("aggregate_sentiment_label", "中性"),
            }
        )
        theme_tags = news_context.get("theme_tags") or []
        if theme_tags:
            items.append(
                {
                    "label": "新闻主题",
                    "value": "、".join(str(item) for item in theme_tags[:3]),
                }
            )

    if isinstance(fund_context, dict) and fund_context.get("status") == "ok":
        items.append(
            {
                "label": "资金面判断",
                "value": _describe_external_flow_label(fund_context),
            }
        )

    if isinstance(financial_context, dict) and financial_context.get("status") == "ok":
        items.append(
            {
                "label": "财报面判断",
                "value": financial_context.get("bias_label") or "中性",
            }
        )

    return items


def _describe_external_flow_label(fund_context: dict[str, Any]) -> str:
    main_inflow_5d = fund_context.get("main_net_inflow_5d")
    main_ratio_today = fund_context.get("main_net_inflow_ratio_today_pct")
    try:
        main_inflow_5d = float(main_inflow_5d) if main_inflow_5d is not None else None
    except (TypeError, ValueError):
        main_inflow_5d = None
    try:
        main_ratio_today = float(main_ratio_today) if main_ratio_today is not None else None
    except (TypeError, ValueError):
        main_ratio_today = None

    if main_inflow_5d is None and main_ratio_today is None:
        return "中性"
    if (main_inflow_5d or 0) > 0 and (main_ratio_today or 0) >= 0:
        return "偏流入"
    if (main_inflow_5d or 0) < 0 and (main_ratio_today or 0) <= 0:
        return "偏流出"
    return "分化"


def build_next_trade_plan(
    source_df: pd.DataFrame,
    config: dict[str, Any],
    indicator_lines: list[dict[str, Any]] | None = None,
    ma_periods: list[int] | None = None,
    current_position: str = "auto",
) -> dict[str, Any]:
    filtered_df = filter_backtest_data(
        source_df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    )
    if filtered_df.empty:
        return {}

    optimized_chart_data = build_optimized_trade_chart_data(
        source_df=source_df,
        filtered_df=filtered_df,
        config=config,
        indicator_lines=indicator_lines,
        ma_periods=ma_periods,
    )
    return extract_next_trade_plan_from_chart_data(
        optimized_chart_data,
        current_position=current_position,
    )


def build_next_trade_plan_by_position(
    source_df: pd.DataFrame,
    config: dict[str, Any],
    indicator_lines: list[dict[str, Any]] | None = None,
    ma_periods: list[int] | None = None,
) -> dict[str, dict[str, Any]]:
    return {
        "empty": build_next_trade_plan(
            source_df=source_df,
            config=config,
            indicator_lines=indicator_lines,
            ma_periods=ma_periods,
            current_position="empty",
        ),
        "hold": build_next_trade_plan(
            source_df=source_df,
            config=config,
            indicator_lines=indicator_lines,
            ma_periods=ma_periods,
            current_position="hold",
        ),
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

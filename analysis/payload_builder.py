from __future__ import annotations

from typing import Any

import pandas as pd


def _to_builtin(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_builtin(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_builtin(item) for item in value]
    if isinstance(value, tuple):
        return [_to_builtin(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item"):
        return value.item()
    return value


def _filter_df(
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


def _safe_pct_change(df: pd.DataFrame, periods: int) -> float | None:
    if len(df) <= periods:
        return None
    base_price = float(df["close"].iloc[-periods - 1])
    latest_price = float(df["close"].iloc[-1])
    if base_price == 0:
        return None
    return round((latest_price / base_price - 1) * 100, 4)


def _safe_mean(series: pd.Series, window: int) -> float | None:
    if series.empty:
        return None
    value = series.tail(window).mean()
    if pd.isna(value):
        return None
    return round(float(value), 4)


def _safe_std(series: pd.Series, window: int) -> float | None:
    if series.empty:
        return None
    value = series.tail(window).std(ddof=0)
    if pd.isna(value):
        return None
    return round(float(value), 4)


def build_single_stock_analysis_payload(
    config: dict[str, Any],
    summary: dict[str, Any],
    df: pd.DataFrame,
) -> dict[str, Any]:
    filtered_df = _filter_df(df, config.get("from_date"), config.get("to_date"))
    if filtered_df.empty:
        raise ValueError("用于大模型分析的股票数据为空")

    latest_row = filtered_df.iloc[-1]
    close_series = filtered_df["close"].astype(float)
    turn_series = filtered_df["turn"].astype(float)
    volume_series = filtered_df["volume"].astype(float)
    daily_returns = close_series.pct_change().dropna() * 100

    payload = {
        "task_type": "single_stock_backtest_analysis",
        "strategy": {
            "report_name": config.get("report_name"),
            "strategy_name": config.get("strategy_name"),
            "strategy_brief": config.get("strategy_brief"),
            "parameters": {
                key: value
                for key, value in config.items()
                if key
                in {
                    "fast",
                    "slow",
                    "lookback",
                    "entry_z",
                    "exit_z",
                    "stop_z",
                    "pair_stop_loss_pct",
                    "max_holding_days",
                    "buy_trigger_multiplier",
                    "buy_trigger_window",
                    "buy_rise_window",
                    "buy_rise_days_required",
                    "sell_trigger_multiplier",
                    "stop_loss_pct",
                    "cash",
                    "adjust_flag",
                }
            },
        },
        "asset": {
            "code": config.get("code"),
            "benchmark_code": config.get("benchmark_code", ""),
            "from_date": config.get("from_date"),
            "to_date": config.get("to_date"),
            "bars": len(filtered_df),
            "as_of_date": pd.Timestamp(latest_row["date"]).strftime("%Y-%m-%d"),
        },
        "performance_summary": summary,
        "market_snapshot": {
            "latest_close": round(float(latest_row["close"]), 4),
            "latest_turn": round(float(latest_row["turn"]), 4),
            "latest_volume": round(float(latest_row["volume"]), 4),
            "return_5d_pct": _safe_pct_change(filtered_df, 5),
            "return_20d_pct": _safe_pct_change(filtered_df, 20),
            "return_60d_pct": _safe_pct_change(filtered_df, 60),
            "avg_turn_20d": _safe_mean(turn_series, 20),
            "avg_volume_20d": _safe_mean(volume_series, 20),
            "volatility_20d_pct": _safe_std(daily_returns, 20),
            "ma20": _safe_mean(close_series, 20),
            "ma60": _safe_mean(close_series, 60),
            "price_position_20d_pct": _build_price_position_pct(close_series, 20),
            "price_position_60d_pct": _build_price_position_pct(close_series, 60),
        },
    }
    return _to_builtin(payload)


def _build_price_position_pct(close_series: pd.Series, window: int) -> float | None:
    recent = close_series.tail(window)
    if recent.empty:
        return None
    highest = float(recent.max())
    lowest = float(recent.min())
    latest = float(recent.iloc[-1])
    if highest == lowest:
        return 50.0
    return round((latest - lowest) / (highest - lowest) * 100, 4)


def build_pair_analysis_payload(
    config: dict[str, Any],
    summary: dict[str, Any],
    spread_price_df: pd.DataFrame,
    pair_label: str,
    pair_quality: dict[str, Any] | None,
) -> dict[str, Any]:
    filtered_df = _filter_df(
        spread_price_df,
        config.get("from_date"),
        config.get("to_date"),
    )
    if filtered_df.empty:
        raise ValueError("用于大模型分析的配对数据为空")

    latest_row = filtered_df.iloc[-1]
    payload = {
        "task_type": "pair_backtest_analysis",
        "strategy": {
            "report_name": config.get("report_name"),
            "strategy_name": config.get("strategy_name"),
            "strategy_brief": config.get("strategy_brief"),
            "parameters": {
                key: value
                for key, value in config.items()
                if key
                in {
                    "lookback",
                    "entry_z",
                    "exit_z",
                    "stop_z",
                    "selection_window",
                    "selection_min_correlation",
                    "selection_min_zero_crossings",
                    "selection_max_half_life",
                    "pair_stop_loss_pct",
                    "max_holding_days",
                    "cash",
                    "adjust_flag",
                }
            },
        },
        "asset": {
            "code": config.get("code"),
            "pair_label": pair_label,
            "from_date": config.get("from_date"),
            "to_date": config.get("to_date"),
            "bars": len(filtered_df),
            "as_of_date": pd.Timestamp(latest_row["date"]).strftime("%Y-%m-%d"),
        },
        "performance_summary": summary,
        "pair_snapshot": {
            "latest_spread_close": round(float(latest_row["close"]), 6),
            "latest_spread_zscore": _round_or_none(latest_row.get("spread_zscore"), 4),
            "latest_rolling_corr": _round_or_none(latest_row.get("rolling_corr"), 4),
            "spread_mean": _round_or_none(latest_row.get("spread_mean"), 6),
            "spread_upper": _round_or_none(latest_row.get("spread_upper"), 6),
            "spread_lower": _round_or_none(latest_row.get("spread_lower"), 6),
            "signal_quality": pair_quality or {},
        },
    }
    return _to_builtin(payload)


def build_batch_analysis_payload(
    strategy_id: str,
    strategy_name: str,
    batch_results: list[dict[str, Any]],
) -> dict[str, Any]:
    if not batch_results:
        raise ValueError("批量分析结果不能为空")

    ranked_results = sorted(
        batch_results,
        key=lambda item: (
            item.get("annual_return_pct") if item.get("annual_return_pct") is not None else float("-inf"),
            item.get("sharpe_ratio") if item.get("sharpe_ratio") is not None else float("-inf"),
            -(
                item.get("max_drawdown_pct")
                if item.get("max_drawdown_pct") is not None
                else float("inf")
            ),
        ),
        reverse=True,
    )
    summary_stats = {
        "sample_size": len(batch_results),
        "positive_return_count": sum(
            1 for item in batch_results if (item.get("total_return_pct") or 0) > 0
        ),
        "positive_annual_return_count": sum(
            1 for item in batch_results if (item.get("annual_return_pct") or 0) > 0
        ),
        "best_annual_return_pct": ranked_results[0].get("annual_return_pct"),
        "worst_annual_return_pct": ranked_results[-1].get("annual_return_pct"),
    }
    top_candidates = [
        {
            "code": item.get("code"),
            "annual_return_pct": item.get("annual_return_pct"),
            "total_return_pct": item.get("total_return_pct"),
            "max_drawdown_pct": item.get("max_drawdown_pct"),
            "sharpe_ratio": item.get("sharpe_ratio"),
            "win_rate_pct": item.get("win_rate_pct"),
            "net_profit": item.get("net_profit"),
            "trades_total": item.get("trades_total"),
        }
        for item in ranked_results[:10]
    ]
    payload = {
        "task_type": "batch_backtest_analysis",
        "strategy": {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
        },
        "batch_summary": summary_stats,
        "candidates": top_candidates,
    }
    return _to_builtin(payload)


def _round_or_none(value: Any, digits: int) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


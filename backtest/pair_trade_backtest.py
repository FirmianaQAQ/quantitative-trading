from __future__ import annotations

"""
统计套利配对交易

核心逻辑：
1. 从行业属性接近、历史走势相关性较高的股票里选择交易对。
2. 当两只股票的价格比值明显偏离历史均值时，买入相对便宜的一侧，卖出相对偏贵的一侧。
3. 当价差回归正常区间，或持仓超时、触发止损时，双边一起平仓。
"""

import sys
import math
import os
from pathlib import Path
from typing import Any

import backtrader as bt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest import (
    add_analyzers,
    build_data_feed,
    create_cerebro,
)
from analysis.service import maybe_generate_pair_analysis
from utils.backtest_report import html as generate_backtest_html
from utils.backtest_report_builder import (
    build_backtrader_report_payload,
    build_returns_series,
    filter_backtest_data,
    format_summary_metrics,
    summarize_result,
)
from utils.h_strategy import HStrategy
from utils.path_utils import ensure_dir
from utils.project_utils import load_daily_data
from utils.a_share_costs import validate_a_share_cost_config


TEST_CASES = [
    {
        "code": "pair_000100_001308",
        "label": "TCL科技 / 康冠科技）",
        "required_codes": ["sz.000100", "sz.001308"],
    },
    {
        "code": "pair_000100_000725",
        "label": "TCL科技 / 京东方A）",
        "required_codes": ["sz.000100", "sz.000725"],
    },
    {
        "code": "pair_000725_001308",
        "label": "京东方A / 康冠科技）",
        "required_codes": ["sz.000725", "sz.001308"],
    },
]

PAIR_CASE_MAP = {item["code"]: item for item in TEST_CASES}

CONFIG: dict[str, Any] = {
    "code": "pair_000100_001308",
    "adjust_flag": "hfq",
    "from_date": "2020-01-01",
    "to_date": None,
    "data_from_date": "2019-01-01",
    "cash": 100000.0,
    "commission": 0.0001,
    "stamp_duty": 0.0005,
    "transfer_fee": 0.00001,
    "min_commission": 5.0,
    "lot_size": 100,
    "gross_exposure_ratio": 0.9,
    "lookback": 50,
    "entry_z": 1.6,
    "exit_z": 0.2,
    "stop_z": 3.0,
    "selection_window": 500,
    "selection_min_correlation": 0.75,
    "selection_min_zero_crossings": 6,
    "selection_max_half_life": 60,
    "pair_stop_loss_pct": 0.04,
    "max_holding_days": 20,
    "print_log": True,
    "plot": True,
    "report_dir": "logs/backtest",
    "report_name": "pair_trade_backtest",
    "strategy_name": "统计套利配对交易",
    "strategy_brief": "比值 zscore 穿越回归",
    "enable_llm_analysis": False,
}


def get_pair_case(pair_code: str) -> dict[str, Any]:
    case = PAIR_CASE_MAP.get(pair_code)
    if case is not None:
        return case

    if pair_code.startswith("pair_auto|"):
        parts = pair_code.split("|")
        if len(parts) == 3:
            code_a, code_b = parts[1], parts[2]
            return {
                "code": pair_code,
                "label": f"{code_a} / {code_b}（本地均值回归候选）",
                "required_codes": [code_a, code_b],
            }

    available = ", ".join(PAIR_CASE_MAP.keys())
    raise ValueError(f"未知交易对: {pair_code}，可选值: {available}")


def _align_pair_data(
    code_a: str,
    code_b: str,
    adjust_flag: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_a = load_daily_data(code_a, adjust_flag).rename(
        columns={
            "open": "open_a",
            "high": "high_a",
            "low": "low_a",
            "close": "close_a",
            "volume": "volume_a",
            "turn": "turn_a",
        }
    )
    df_b = load_daily_data(code_b, adjust_flag).rename(
        columns={
            "open": "open_b",
            "high": "high_b",
            "low": "low_b",
            "close": "close_b",
            "volume": "volume_b",
            "turn": "turn_b",
        }
    )

    merged = pd.merge(df_a, df_b, on="date", how="inner").copy()
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged.sort_values("date").reset_index(drop=True)

    aligned_a = merged[
        ["date", "open_a", "high_a", "low_a", "close_a", "volume_a", "turn_a"]
    ].rename(
        columns={
            "open_a": "open",
            "high_a": "high",
            "low_a": "low",
            "close_a": "close",
            "volume_a": "volume",
            "turn_a": "turn",
        }
    )
    aligned_b = merged[
        ["date", "open_b", "high_b", "low_b", "close_b", "volume_b", "turn_b"]
    ].rename(
        columns={
            "open_b": "open",
            "high_b": "high",
            "low_b": "low",
            "close_b": "close",
            "volume_b": "volume",
            "turn_b": "turn",
        }
    )

    return aligned_a, aligned_b, merged


def _build_spread_price_frame(
    merged_df: pd.DataFrame,
    lookback: int,
) -> pd.DataFrame:
    frame = merged_df.copy()
    frame["open"] = frame["open_a"] / frame["open_b"]
    frame["close"] = frame["close_a"] / frame["close_b"]
    frame["high"] = frame["high_a"] / frame["low_b"]
    frame["low"] = frame["low_a"] / frame["high_b"]
    frame["volume"] = 0.0
    frame["rolling_corr"] = frame["close_a"].pct_change(fill_method=None).rolling(
        window=lookback,
        min_periods=lookback,
    ).corr(frame["close_b"].pct_change(fill_method=None))
    frame["spread_mean"] = frame["close"].rolling(window=lookback, min_periods=lookback).mean()
    spread_std = frame["close"].rolling(window=lookback, min_periods=lookback).std(ddof=0)
    frame["spread_std"] = spread_std
    frame["spread_upper"] = frame["spread_mean"] + spread_std * 2
    frame["spread_lower"] = frame["spread_mean"] - spread_std * 2
    frame["spread_zscore"] = (frame["close"] - frame["spread_mean"]).div(
        spread_std.where(spread_std > 0)
    )
    return frame[
        [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "rolling_corr",
            "spread_mean",
            "spread_std",
            "spread_upper",
            "spread_lower",
            "spread_zscore",
        ]
    ]


def calculate_half_life(spread_series: pd.Series) -> float | None:
    valid_spread = spread_series.dropna()
    if len(valid_spread) < 30:
        return None

    lagged_spread = valid_spread.shift(1)
    spread_delta = valid_spread - lagged_spread
    regression_frame = pd.concat(
        [lagged_spread.rename("lag"), spread_delta.rename("delta")],
        axis=1,
    ).dropna()
    if len(regression_frame) < 30:
        return None

    centered_lag = regression_frame["lag"] - regression_frame["lag"].mean()
    denominator = float((centered_lag * centered_lag).sum())
    if denominator <= 0:
        return None

    theta = float((centered_lag * regression_frame["delta"]).sum() / denominator)
    if theta >= 0:
        return None
    return float(math.log(2) / (-theta))


def evaluate_pair_signal_quality(
    signal_frame: pd.DataFrame,
    selection_window: int,
    min_correlation: float,
    min_zero_crossings: int,
    max_half_life: float,
) -> dict[str, float] | None:
    recent_frame = signal_frame.tail(selection_window).copy()
    valid_frame = recent_frame.dropna(subset=["rolling_corr", "spread_zscore", "close"])
    if len(valid_frame) < 60:
        return None

    rolling_corr = float(valid_frame["rolling_corr"].median())
    zero_crossings = int(
        ((valid_frame["spread_zscore"] * valid_frame["spread_zscore"].shift(1)) < 0).sum()
    )
    half_life = calculate_half_life(valid_frame["close"])

    if rolling_corr < min_correlation:
        return None
    if zero_crossings < min_zero_crossings:
        return None
    if half_life is None or half_life > max_half_life:
        return None

    score = rolling_corr * zero_crossings / max(half_life, 1.0)
    return {
        "score": float(score),
        "rolling_corr": rolling_corr,
        "zero_crossings": float(zero_crossings),
        "half_life": float(half_life),
    }


class PairTradingStrategy(HStrategy):
    params = (
        ("p", None),
        ("printlog", True),
    )

    def __init__(self) -> None:
        super().__init__(allow_log=self.params.printlog)

        self.param = self.p.p or {}
        self.data_a = self.datas[0]
        self.data_b = self.datas[1]
        self.ratio = self.data_a.close / self.data_b.close
        lookback = int(self.param.get("lookback", 60))
        self.ratio_ma = bt.indicators.SimpleMovingAverage(self.ratio, period=lookback)
        self.ratio_std = bt.indicators.StandardDeviation(self.ratio, period=lookback)
        from_date = self.param.get("from_date")
        self.from_date = pd.Timestamp(from_date).to_pydatetime() if from_date else None

        self.pending_order_refs: set[int] = set()
        self.buy_markers: list[tuple[pd.Timestamp, float]] = []
        self.sell_markers: list[tuple[pd.Timestamp, float]] = []
        self.current_pair_direction = 0
        self.pending_pair_direction = 0
        self.entry_in_progress = False
        self.exit_in_progress = False
        self.pair_entry_bar: int | None = None
        self.pair_entry_value: float | None = None
        self.position_days_total = 0
        self.idle_cash_days_total = 0
        self.has_completed_sell = False
        self.pair_round_trips_total = 0
        self.pair_round_trips_won = 0
        self.pair_round_trips_lost = 0
        self.pair_realized_pnls: list[float] = []
        self.last_zscore: float | None = None

    def _has_pair_position(self) -> bool:
        return any(
            abs(self.getposition(data).size) > 0 for data in (self.data_a, self.data_b)
        )

    def _get_ratio_value(self) -> float:
        price_b = float(self.data_b.close[0])
        if price_b == 0:
            return 0.0
        return float(self.data_a.close[0]) / price_b

    def _get_zscore(self) -> float | None:
        std = float(self.ratio_std[0]) if self.ratio_std[0] is not None else 0.0
        if std <= 0:
            return None
        return (float(self.ratio[0]) - float(self.ratio_ma[0])) / std

    def _add_pending_order(self, order: bt.Order | None) -> None:
        if order is not None:
            self.pending_order_refs.add(order.ref)

    def _calculate_pair_sizes(self) -> tuple[int, int]:
        lot_size = max(int(self.param.get("lot_size", 100)), 1)
        gross_exposure_ratio = float(self.param.get("gross_exposure_ratio", 0.9))
        capital = self.broker.getvalue() * gross_exposure_ratio
        side_cash = capital / 2

        price_a = float(self.data_a.close[0])
        price_b = float(self.data_b.close[0])
        if price_a <= 0 or price_b <= 0:
            return 0, 0

        size_a = int(side_cash / price_a)
        size_b = int(side_cash / price_b)
        size_a = (size_a // lot_size) * lot_size
        size_b = (size_b // lot_size) * lot_size
        return max(size_a, 0), max(size_b, 0)

    def _submit_entry(self, direction: int, zscore: float) -> bool:
        size_a, size_b = self._calculate_pair_sizes()
        if size_a <= 0 or size_b <= 0:
            self.log("配对开仓失败，可用资金不足")
            return False

        dt = pd.Timestamp(self.datas[0].datetime.date(0))
        ratio_value = self._get_ratio_value()
        self.buy_markers.append((dt, ratio_value))
        self.entry_in_progress = True
        self.pending_pair_direction = direction
        action_text = (
            f"价差偏离过大，买入较便宜的 {self.data_a._name}，卖出较贵的 {self.data_b._name}"
            if direction > 0
            else f"价差偏离过大，卖出较贵的 {self.data_a._name}，买入较便宜的 {self.data_b._name}"
        )
        self.log(
            f"{action_text}，开始统计套利开仓 zscore={zscore:.2f} "
            f"{self.data_a._name}数量={size_a} {self.data_b._name}数量={size_b}"
        )

        if direction > 0:
            self._add_pending_order(self.buy(data=self.data_a, size=size_a))
            self._add_pending_order(self.sell(data=self.data_b, size=size_b))
        else:
            self._add_pending_order(self.sell(data=self.data_a, size=size_a))
            self._add_pending_order(self.buy(data=self.data_b, size=size_b))
        return True

    def _submit_exit(self, reason: str) -> bool:
        if not self._has_pair_position():
            return False

        dt = pd.Timestamp(self.datas[0].datetime.date(0))
        ratio_value = self._get_ratio_value()
        self.sell_markers.append((dt, ratio_value))
        self.exit_in_progress = True
        self.log(f"配对平仓 原因={reason}")

        for data in (self.data_a, self.data_b):
            position = self.getposition(data)
            if position.size == 0:
                continue
            self._add_pending_order(self.close(data=data))
        return True

    def notify_order(self, order: bt.Order) -> None:
        if order.status in [order.Submitted, order.Accepted]:
            return

        data_name = getattr(order.data, "_name", "data")
        if order.status == order.Completed:
            action = "买入" if order.isbuy() else "卖出"
            self.log(
                f"{data_name} 成交 {action}价={order.executed.price:.4f} "
                f"数量={abs(order.executed.size):.0f} 手续费={order.executed.comm:.2f}"
            )
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_map = {
                order.Canceled: "已取消",
                order.Margin: "资金不足",
                order.Rejected: "已拒绝",
            }
            self.log(
                f"{data_name} 订单失败 状态={status_map.get(order.status, order.getstatusname())}"
            )

        self.pending_order_refs.discard(order.ref)
        if self.pending_order_refs:
            return

        if self.entry_in_progress:
            self.entry_in_progress = False
            if self._has_pair_position():
                self.current_pair_direction = self.pending_pair_direction
                self.pair_entry_bar = len(self)
                self.pair_entry_value = self.broker.getvalue()
        elif self.exit_in_progress:
            self.exit_in_progress = False
            if not self._has_pair_position() and self.pair_entry_value is not None:
                pnl = self.broker.getvalue() - self.pair_entry_value
                self.pair_realized_pnls.append(pnl)
                self.pair_round_trips_total += 1
                if pnl >= 0:
                    self.pair_round_trips_won += 1
                else:
                    self.pair_round_trips_lost += 1
                self.log(f"配对完成 净收益={pnl:.2f}")
                self.current_pair_direction = 0
                self.pair_entry_bar = None
                self.pair_entry_value = None
                self.has_completed_sell = True

    def next(self) -> None:
        if self.from_date and self.datas[0].datetime.date(0) < self.from_date.date():
            return

        if self._has_pair_position():
            self.position_days_total += 1
        elif self.has_completed_sell:
            self.idle_cash_days_total += 1

        if self.pending_order_refs:
            return

        zscore = self._get_zscore()
        if zscore is None:
            self.last_zscore = zscore
            return

        if self._has_pair_position():
            holding_days = (
                len(self) - self.pair_entry_bar if self.pair_entry_bar is not None else 0
            )
            pair_stop_loss_pct = float(self.param.get("pair_stop_loss_pct", 0.06))
            stop_z = float(self.param.get("stop_z", 3.2))
            pnl_pct = 0.0
            if self.pair_entry_value:
                pnl_pct = (self.broker.getvalue() - self.pair_entry_value) / self.pair_entry_value

            if abs(zscore) <= float(self.param.get("exit_z", 0.35)):
                self._submit_exit(f"价差回归正常区间 zscore={zscore:.2f}")
            elif self.current_pair_direction > 0 and zscore <= -stop_z:
                self._submit_exit(f"zscore 止损 zscore={zscore:.2f}")
            elif self.current_pair_direction < 0 and zscore >= stop_z:
                self._submit_exit(f"zscore 止损 zscore={zscore:.2f}")
            elif holding_days >= int(self.param.get("max_holding_days", 30)):
                self._submit_exit(f"持仓超时 {holding_days}天")
            elif pnl_pct <= -pair_stop_loss_pct:
                self._submit_exit(f"组合止损 pnl={pnl_pct * 100:.2f}%")
            self.last_zscore = zscore
            return

        entry_z = float(self.param.get("entry_z", 2.0))
        if self.last_zscore is not None and self.last_zscore > -entry_z and zscore <= -entry_z:
            self._submit_entry(direction=1, zscore=zscore)
        elif self.last_zscore is not None and self.last_zscore < entry_z and zscore >= entry_z:
            self._submit_entry(direction=-1, zscore=zscore)
        self.last_zscore = zscore

    def stop(self) -> None:
        self.log(
            f"回测结束 lookback={self.param.get('lookback')} "
            f"entry_z={self.param.get('entry_z')} 期末资产={self.broker.getvalue():.2f}"
        )


def validate_config(config: dict[str, Any]) -> None:
    get_pair_case(config["code"])
    if config["cash"] <= 0:
        raise ValueError("初始资金 cash 必须大于 0")
    validate_a_share_cost_config(config)
    if int(config["lot_size"]) <= 0:
        raise ValueError("lot_size 必须大于 0")
    if float(config["gross_exposure_ratio"]) <= 0 or float(config["gross_exposure_ratio"]) > 1:
        raise ValueError("gross_exposure_ratio 必须大于 0 且小于等于 1")
    if int(config["lookback"]) <= 1:
        raise ValueError("lookback 必须大于 1")
    if float(config["entry_z"]) <= 0:
        raise ValueError("entry_z 必须大于 0")
    if float(config["exit_z"]) < 0:
        raise ValueError("exit_z 不能小于 0")
    if float(config["entry_z"]) <= float(config["exit_z"]):
        raise ValueError("entry_z 必须大于 exit_z")
    if float(config["stop_z"]) <= float(config["entry_z"]):
        raise ValueError("stop_z 必须大于 entry_z")
    if int(config["selection_window"]) < int(config["lookback"]):
        raise ValueError("selection_window 不能小于 lookback")
    if float(config["selection_min_correlation"]) < 0 or float(config["selection_min_correlation"]) > 1:
        raise ValueError("selection_min_correlation 必须在 0 到 1 之间")
    if int(config["selection_min_zero_crossings"]) <= 0:
        raise ValueError("selection_min_zero_crossings 必须大于 0")
    if float(config["selection_max_half_life"]) <= 0:
        raise ValueError("selection_max_half_life 必须大于 0")
    if float(config["pair_stop_loss_pct"]) <= 0 or float(config["pair_stop_loss_pct"]) >= 1:
        raise ValueError("pair_stop_loss_pct 必须大于 0 且小于 1")
    if int(config["max_holding_days"]) <= 0:
        raise ValueError("max_holding_days 必须大于 0")


def _build_summary(strategy: PairTradingStrategy, initial_value: float) -> dict[str, Any]:
    summary = summarize_result(strategy, initial_value)
    total_closed = strategy.pair_round_trips_total
    win_rate_pct = (
        strategy.pair_round_trips_won / total_closed * 100 if total_closed else 0.0
    )
    net_profit = sum(strategy.pair_realized_pnls) if strategy.pair_realized_pnls else 0.0
    avg_trade_profit = (
        net_profit / total_closed if total_closed else 0.0
    )
    summary.update(
        {
            "trades_total": total_closed,
            "trades_won": strategy.pair_round_trips_won,
            "trades_lost": strategy.pair_round_trips_lost,
            "win_rate_pct": round(win_rate_pct, 2),
            "net_profit": round(net_profit, 2),
            "avg_trade_profit": round(avg_trade_profit, 2),
        }
    )
    return summary


def _print_summary(summary: dict[str, Any], config: dict[str, Any], pair_label: str) -> None:
    print("回测结果:")
    print(f"  交易对: {pair_label}")
    print(f"  lookback: {config['lookback']}")
    print(f"  entry_z: {config['entry_z']:.2f}")
    print(f"  exit_z: {config['exit_z']:.2f}")
    print(f"  stop_z: {config['stop_z']:.2f}")
    print(f"  组合止损: {config['pair_stop_loss_pct'] * 100:.2f}%")
    print(f"  最大持仓天数: {config['max_holding_days']}")
    print(f"  初始资金: {summary['initial_value']:.2f}")
    print(f"  期末资产: {summary['final_value']:.2f}")
    print(f"  总收益率: {summary['total_return_pct']:.2f}%")
    annual_return_pct = summary["annual_return_pct"]
    print(
        f"  年化收益率: {annual_return_pct:.2f}%"
        if annual_return_pct is not None
        else "  年化收益率: N/A"
    )
    max_drawdown_pct = summary["max_drawdown_pct"]
    print(
        f"  最大回撤: {max_drawdown_pct:.2f}%"
        if max_drawdown_pct is not None
        else "  最大回撤: N/A"
    )
    max_drawdown_amount = summary["max_drawdown_amount"]
    print(
        f"  最大回撤金额: {max_drawdown_amount:.2f}"
        if max_drawdown_amount is not None
        else "  最大回撤金额: N/A"
    )
    print(f"  最大回撤持续周期数: {summary['drawdown_max_len']}")
    sharpe_ratio = summary["sharpe_ratio"]
    print(
        f"  夏普比率: {sharpe_ratio:.2f}"
        if sharpe_ratio is not None
        else "  夏普比率: N/A"
    )
    print(f"  总交易次数: {summary['trades_total']}")
    print(f"  盈利次数: {summary['trades_won']}")
    print(f"  亏损次数: {summary['trades_lost']}")
    print(f"  胜率: {summary['win_rate_pct']:.2f}%")
    net_profit = summary["net_profit"]
    print(f"  净利润: {net_profit:.2f}" if net_profit is not None else "  净利润: N/A")
    avg_trade_profit = summary["avg_trade_profit"]
    print(
        f"  平均每笔净利润: {avg_trade_profit:.2f}"
        if avg_trade_profit is not None
        else "  平均每笔净利润: N/A"
    )
    print(f"  资金占用天数: {summary['position_days_total']}")
    print(f"  资金空闲天数: {summary['idle_cash_days_total']}")


def _build_pair_report_data(
    strategy: PairTradingStrategy,
    config: dict[str, Any],
    spread_price_df: pd.DataFrame,
    pair_label: str,
    summary: dict[str, Any],
) -> list[dict[str, Any]]:
    returns_series = build_returns_series(strategy)
    if returns_series.empty:
        return []

    total_days = summary["position_days_total"] + summary["idle_cash_days_total"]
    summary_metrics = format_summary_metrics(
        [
            {"label": "交易对", "value": pair_label},
            {"label": "策略名称", "value": config.get("strategy_name", "统计套利配对交易")},
            {"label": "初始资金", "value": summary["initial_value"], "kind": "number"},
            {"label": "期末资产", "value": summary["final_value"], "kind": "number"},
            {"label": "总收益率", "value": summary["total_return_pct"], "kind": "percent"},
            {"label": "年化收益率", "value": summary["annual_return_pct"], "kind": "percent"},
            {"label": "最大回撤", "value": summary["max_drawdown_pct"], "kind": "percent"},
            {"label": "最大回撤金额", "value": summary["max_drawdown_amount"], "kind": "number"},
            {"label": "最大回撤周期", "value": summary["drawdown_max_len"]},
            {"label": "夏普比率", "value": summary["sharpe_ratio"], "kind": "number"},
            {"label": "总交易次数", "value": summary["trades_total"]},
            {"label": "盈利次数", "value": summary["trades_won"]},
            {"label": "亏损次数", "value": summary["trades_lost"]},
            {"label": "胜率", "value": summary["win_rate_pct"], "kind": "percent"},
            {"label": "净利润", "value": summary["net_profit"], "kind": "number"},
            {"label": "平均每笔净利润", "value": summary["avg_trade_profit"], "kind": "number"},
            {"label": "资金占用天数", "value": summary["position_days_total"]},
            {
                "label": "资金占用天数占比",
                "value": (summary["position_days_total"] / total_days * 100) if total_days > 0 else 0,
                "kind": "percent",
            },
            {"label": "资金空闲天数", "value": summary["idle_cash_days_total"]},
            {
                "label": "资金空闲天数占比",
                "value": (summary["idle_cash_days_total"] / total_days * 100) if total_days > 0 else 0,
                "kind": "percent",
            },
        ]
    )

    filtered_df = filter_backtest_data(
        spread_price_df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    ).copy()
    indicator_lines = [
        {
            "name": "SpreadMean",
            "data": filtered_df["spread_mean"].round(4).tolist(),
        },
        {
            "name": "UpperBand",
            "data": filtered_df["spread_upper"].round(4).tolist(),
        },
        {
            "name": "LowerBand",
            "data": filtered_df["spread_lower"].round(4).tolist(),
        },
    ]

    return build_backtrader_report_payload(
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
        benchmark_returns=None,
        strategy_name="本策略",
        benchmark_name="基准",
        asset_name="价差比值",
        buy_sell_subtitle=f"{pair_label} 配对价差",
    )


def _generate_html_report(
    report_data: list[dict[str, Any]],
    config: dict[str, Any],
    pair_label: str,
    log_lines: list[str] | None = None,
    ai_report_path: Path | None = None,
) -> None:
    if not report_data:
        print("没有可用的回测数据来生成报告")
        return
    report_dir = ensure_dir(PROJECT_ROOT / config["report_dir"])
    html_report_path = report_dir / f"{config['report_name']}-{config['code']}.html"
    title = f"{pair_label} {config.get('strategy_name', '配对交易')} 回测报告"
    ai_report_link = None
    if ai_report_path is not None:
        ai_report_link = Path(
            os.path.relpath(ai_report_path, start=html_report_path.parent)
        ).as_posix()
    generate_backtest_html(
        report_data,
        str(html_report_path),
        [],
        title,
        log_lines=log_lines,
        current_position=str(config.get("current_position", "auto")),
        ai_report_link=ai_report_link,
    )
    print(f"HTML 回测报告: {html_report_path}")


def run_backtest(config: dict[str, Any], df: pd.DataFrame | None = None) -> dict[str, Any]:
    del df
    validate_config(config)
    pair_case = get_pair_case(config["code"])
    code_a, code_b = pair_case["required_codes"]
    pair_label = str(pair_case.get("label", config["code"]))
    aligned_a, aligned_b, merged_df = _align_pair_data(code_a, code_b, config["adjust_flag"])
    spread_price_df = _build_spread_price_frame(merged_df, int(config.get("lookback", 60)))

    cerebro = create_cerebro(config)
    cerebro.addstrategy(
        PairTradingStrategy,
        printlog=config["print_log"],
        p=config,
    )
    cerebro.adddata(
        build_data_feed(aligned_a, config.get("data_from_date"), config.get("to_date")),
        name=code_a,
    )
    cerebro.adddata(
        build_data_feed(aligned_b, config.get("data_from_date"), config.get("to_date")),
        name=code_b,
    )
    add_analyzers(cerebro)

    initial_value = cerebro.broker.getvalue()
    print(f"开始回测: 交易对={pair_label}，初始资金={initial_value:.2f}")
    strategy = cerebro.run()[0]
    summary = _build_summary(strategy, initial_value)
    pair_quality = evaluate_pair_signal_quality(
        signal_frame=spread_price_df,
        selection_window=int(config["selection_window"]),
        min_correlation=float(config["selection_min_correlation"]),
        min_zero_crossings=int(config["selection_min_zero_crossings"]),
        max_half_life=float(config["selection_max_half_life"]),
    )
    summary.update(
        {
            "pair_code_a": code_a,
            "pair_code_b": code_b,
            "pair_quality": pair_quality,
        }
    )
    _print_summary(summary, config, pair_label)

    ai_report_path = maybe_generate_pair_analysis(
        config=config,
        summary=summary,
        spread_price_df=spread_price_df,
        pair_label=pair_label,
        pair_quality=pair_quality,
    )

    if config["plot"]:
        report_data = _build_pair_report_data(
            strategy,
            config,
            spread_price_df,
            pair_label,
            summary,
        )
        _generate_html_report(
            report_data,
            config,
            pair_label,
            strategy.log_messages,
            ai_report_path=ai_report_path,
        )

    return summary


def main(config: dict[str, Any]) -> None:
    run_backtest(config, None)


if __name__ == "__main__":
    main(CONFIG)

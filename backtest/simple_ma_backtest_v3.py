from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest_v2 import (
    CONFIG as V2_CONFIG,
    TEST_CASES,
    SimpleMovingAverageStrategyV2,
)
from backtest.simple_ma_backtest import (
    add_analyzers,
    build_data_feed,
    create_cerebro,
    generate_html_report,
    print_summary,
)
from utils.backtest_report_builder import (
    build_backtest_report_data,
    summarize_result,
)
from utils.project_utils import load_daily_data


CONFIG: dict[str, Any] = dict(V2_CONFIG)
CONFIG.update(
    {
        # V3 在 V2 稳定底座上，显式融合：
        # 1. 趋势跟踪：新增金叉突破跟随机制
        # 2. 均值回归：保留 V2 的回踩观察 + 反弹买入
        # 3. 动量确认：买入前看短期修复，卖出时看动量衰减
        "trend_slope_window": 5,
        "trend_min_slope_pct": 0.0,
        "trend_entry_buffer_pct": 0.01,
        "breakout_lookback": 15,
        "breakout_confirm_buffer_pct": 0.002,
        "breakout_min_return_pct": 0.02,
        "breakout_max_extension_pct": 0.03,
        "momentum_entry_window": 3,
        "momentum_entry_up_days_required": 1,
        "momentum_entry_min_return_pct": 0.005,
        "trend_exit_confirm_bars": 2,
        "profit_protect_activation_pct": 0.12,
        "trailing_stop_pct": 0.06,
        "momentum_exit_profit_floor_pct": 0.08,
        "momentum_exit_return_pct": 0.03,
        "report_name": "simple_ma_backtest_v3",
        "strategy_name": "普通双均线V3",
        "strategy_brief": "趋势突破融合版",
    }
)


def _safe_pct_delta(new_value: float, base_value: float) -> float:
    if not base_value:
        return 0.0
    return (new_value - base_value) / base_value


def evaluate_v3_breakout_entry(
    *,
    current_close: float,
    previous_close: float,
    fast_ma: float,
    slow_ma: float,
    slow_ma_prev: float,
    recent_breakout_high: float,
    momentum_return_pct: float,
    crossover_signal: float,
    params: dict[str, Any],
) -> tuple[bool, dict[str, float | bool]]:
    trend_slope_pct = _safe_pct_delta(slow_ma, slow_ma_prev)
    trend_ok = (
        fast_ma >= slow_ma
        and trend_slope_pct >= float(params["trend_min_slope_pct"])
        and current_close >= slow_ma * (1 - float(params["trend_entry_buffer_pct"]))
    )
    breakout_gap_pct = _safe_pct_delta(current_close, recent_breakout_high)
    breakout_extension_pct = _safe_pct_delta(current_close, fast_ma)
    momentum_ok = momentum_return_pct >= float(params["breakout_min_return_pct"])
    breakout_ok = (
        trend_ok
        and crossover_signal > 0
        and current_close > previous_close
        and momentum_ok
        and breakout_gap_pct >= -float(params["breakout_confirm_buffer_pct"])
        and breakout_extension_pct <= float(params["breakout_max_extension_pct"])
    )
    info: dict[str, float | bool] = {
        "trend_ok": trend_ok,
        "trend_slope_pct": trend_slope_pct,
        "breakout_gap_pct": breakout_gap_pct,
        "breakout_extension_pct": breakout_extension_pct,
        "momentum_ok": momentum_ok,
        "momentum_return_pct": momentum_return_pct,
        "breakout_ok": breakout_ok,
    }
    return breakout_ok, info


def evaluate_v3_protective_exit(
    *,
    current_close: float,
    fast_ma: float,
    slow_ma: float,
    last_buy_price: float,
    highest_close_since_entry: float,
    recent_down_days: int,
    momentum_return_pct: float,
    params: dict[str, Any],
) -> tuple[str | None, dict[str, float | bool]]:
    profit_pct = _safe_pct_delta(current_close, last_buy_price)
    trailing_drawdown_pct = _safe_pct_delta(highest_close_since_entry, current_close)
    trend_break_hit = (
        fast_ma < slow_ma
        and current_close < fast_ma
        and recent_down_days >= int(params["trend_exit_confirm_bars"])
    )
    trailing_stop_hit = (
        profit_pct >= float(params["profit_protect_activation_pct"])
        and trailing_drawdown_pct >= float(params["trailing_stop_pct"])
        and momentum_return_pct <= 0
    )
    momentum_fade_hit = (
        profit_pct >= float(params["momentum_exit_profit_floor_pct"])
        and current_close < fast_ma
        and momentum_return_pct <= -float(params["momentum_exit_return_pct"])
    )
    info: dict[str, float | bool] = {
        "profit_pct": profit_pct,
        "trailing_drawdown_pct": trailing_drawdown_pct,
        "trend_break_hit": trend_break_hit,
        "trailing_stop_hit": trailing_stop_hit,
        "momentum_fade_hit": momentum_fade_hit,
        "recent_down_days": float(recent_down_days),
        "momentum_return_pct": momentum_return_pct,
    }
    if trend_break_hit:
        return "trend_break", info
    if trailing_stop_hit:
        return "trailing_stop", info
    if momentum_fade_hit:
        return "momentum_fade", info
    return None, info


class SimpleMovingAverageStrategyV3(SimpleMovingAverageStrategyV2):
    def __init__(self) -> None:
        super().__init__()
        self.highest_close_since_entry: float | None = None

    def _recent_high(self, window: int, *, include_current: bool) -> float | None:
        if window <= 0:
            return None
        start = 0 if include_current else 1
        values: list[float] = []
        for offset in range(start, start + window):
            try:
                values.append(float(self.data.high[-offset]))
            except IndexError:
                break
        if not values:
            return None
        return max(values)

    def _recent_return_pct(self, window: int) -> float:
        if window <= 0 or len(self.data) <= window:
            return 0.0
        current_close = float(self.data.close[0])
        base_close = float(self.data.close[-window])
        return _safe_pct_delta(current_close, base_close)

    def _recent_up_days(self, window: int) -> int:
        count = 0
        available = min(window, max(len(self.data) - 1, 0))
        for offset in range(available):
            if float(self.data.close[-offset]) > float(self.data.close[-offset - 1]):
                count += 1
        return count

    def _recent_down_days(self, window: int) -> int:
        count = 0
        available = min(window, max(len(self.data) - 1, 0))
        for offset in range(available):
            if float(self.data.close[-offset]) < float(self.data.close[-offset - 1]):
                count += 1
        return count

    def _trend_regime_ok(self) -> bool:
        window = int(self.param.get("trend_slope_window", 1))
        if len(self.data) <= window:
            return False
        current_close = float(self.data.close[0])
        fast_ma = float(self.fast_ma[0])
        slow_ma = float(self.slow_ma[0])
        slow_ma_prev = float(self.slow_ma[-window])
        return (
            fast_ma >= slow_ma
            and _safe_pct_delta(slow_ma, slow_ma_prev)
            >= float(self.param.get("trend_min_slope_pct", 0.0))
            and current_close
            >= slow_ma * (1 - float(self.param.get("trend_entry_buffer_pct", 0.0)))
        )

    def next(self) -> None:
        if self.position:
            current_close = float(self.data.close[0])
            if self.highest_close_since_entry is None:
                self.highest_close_since_entry = current_close
            else:
                self.highest_close_since_entry = max(
                    self.highest_close_since_entry, current_close
                )
        else:
            self.highest_close_since_entry = None
        super().next()

    def _check_and_buy(self) -> None:
        if self.position:
            return

        breakout_window = int(self.param.get("breakout_lookback", 0))
        if len(self.data) > max(
            breakout_window,
            int(self.param.get("trend_slope_window", 1)),
            int(self.param.get("momentum_entry_window", 1)),
        ):
            recent_breakout_high = self._recent_high(
                breakout_window,
                include_current=False,
            )
            if recent_breakout_high is not None:
                breakout_ok, info = evaluate_v3_breakout_entry(
                    current_close=float(self.data.close[0]),
                    previous_close=float(self.data.close[-1]),
                    fast_ma=float(self.fast_ma[0]),
                    slow_ma=float(self.slow_ma[0]),
                    slow_ma_prev=float(
                        self.slow_ma[-int(self.param["trend_slope_window"])]
                    ),
                    recent_breakout_high=recent_breakout_high,
                    momentum_return_pct=self._recent_return_pct(
                        int(self.param["momentum_entry_window"])
                    ),
                    crossover_signal=float(self.crossover[0]),
                    params=self.param,
                )
                if breakout_ok:
                    size = self.calculate_buy_size()
                    if size > 0:
                        self.log(
                            "V3 趋势突破买入"
                            f" | 收盘价={float(self.data.close[0]):.2f}"
                            f" | 慢线斜率={float(info['trend_slope_pct']) * 100:.2f}%"
                            f" | 突破距前高={float(info['breakout_gap_pct']) * 100:.2f}%"
                            f" | 距快线偏离={float(info['breakout_extension_pct']) * 100:.2f}%"
                            f" | 近{int(self.param['momentum_entry_window'])}日收益={float(info['momentum_return_pct']) * 100:.2f}%"
                            f" | 数量={size}"
                        )
                        self.order = self.buy(size=size)
                        self.reset_buy_setup()
                        return

        if not self._trend_regime_ok():
            if self.buy_trigger_active and float(self.data.close[0]) < float(self.slow_ma[0]):
                self.log("V3 趋势过滤失败，价格重新跌回慢线下方，取消本轮回踩观察")
                self.reset_buy_setup()
            return

        if self.buy_trigger_active:
            momentum_window = int(self.param.get("momentum_entry_window", 1))
            momentum_up_days = self._recent_up_days(momentum_window)
            momentum_return_pct = self._recent_return_pct(momentum_window)
            if (
                momentum_up_days < int(self.param.get("momentum_entry_up_days_required", 1))
                and momentum_return_pct < float(self.param.get("momentum_entry_min_return_pct", 0.0))
            ):
                return

        return super()._check_and_buy()

    def _check_and_sell(self) -> None:
        if self.position and self.last_buy_price is not None:
            exit_name, exit_info = evaluate_v3_protective_exit(
                current_close=float(self.data.close[0]),
                fast_ma=float(self.fast_ma[0]),
                slow_ma=float(self.slow_ma[0]),
                last_buy_price=float(self.last_buy_price),
                highest_close_since_entry=self.highest_close_since_entry
                or float(self.data.close[0]),
                recent_down_days=self._recent_down_days(
                    int(self.param.get("trend_exit_confirm_bars", 1))
                ),
                momentum_return_pct=self._recent_return_pct(
                    int(self.param.get("momentum_entry_window", 1))
                ),
                params=self.param,
            )
            if exit_name is not None:
                self.log(
                    "V3 保护性卖出"
                    f" | 原因={exit_name}"
                    f" | 收盘价={float(self.data.close[0]):.2f}"
                    f" | 本轮收益={float(exit_info['profit_pct']) * 100:.2f}%"
                    f" | 浮盈回撤={float(exit_info['trailing_drawdown_pct']) * 100:.2f}%"
                    f" | 近{int(self.param['trend_exit_confirm_bars'])}日下跌天数={int(exit_info['recent_down_days'])}"
                    f" | 近{int(self.param['momentum_entry_window'])}日收益={float(exit_info['momentum_return_pct']) * 100:.2f}%"
                    f" | 数量={abs(self.position.size)}"
                )
                self.order = self.close()
                self.reset_sell_setup()
                return

        return super()._check_and_sell()


def validate_config(config: dict[str, Any]) -> None:
    from backtest.simple_ma_backtest import validate_config as validate_base_config

    validate_base_config(config)
    positive_int_keys = [
        "trend_slope_window",
        "breakout_lookback",
        "momentum_entry_window",
        "trend_exit_confirm_bars",
    ]
    for key in positive_int_keys:
        if int(config[key]) <= 0:
            raise ValueError(f"{key} 必须大于 0")

    ratio_keys = [
        "trend_min_slope_pct",
        "trend_entry_buffer_pct",
        "breakout_confirm_buffer_pct",
        "breakout_min_return_pct",
        "breakout_max_extension_pct",
        "momentum_entry_min_return_pct",
        "profit_protect_activation_pct",
        "trailing_stop_pct",
        "momentum_exit_profit_floor_pct",
        "momentum_exit_return_pct",
    ]
    for key in ratio_keys:
        if float(config[key]) < 0:
            raise ValueError(f"{key} 不能小于 0")

    if int(config["momentum_entry_up_days_required"]) <= 0:
        raise ValueError("momentum_entry_up_days_required 必须大于 0")
    if int(config["momentum_entry_up_days_required"]) > int(
        config["momentum_entry_window"]
    ):
        raise ValueError(
            "momentum_entry_up_days_required 不能大于 momentum_entry_window"
        )


def run_backtest(config: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    cerebro = create_cerebro(config)
    cerebro.addstrategy(
        SimpleMovingAverageStrategyV3,
        fast_period=config["fast"],
        slow_period=config["slow"],
        printlog=config["print_log"],
        p=config,
        df=df,
    )
    cerebro.adddata(build_data_feed(df, config["data_from_date"], config["to_date"]))
    add_analyzers(cerebro)

    initial_value = cerebro.broker.getvalue()
    print(f"开始回测: 股票={config['code']}，初始资金={initial_value:.2f}")
    strategies = cerebro.run()
    strategy = strategies[0]
    summary = summarize_result(strategy, initial_value)
    summary.update(
        {
            "fast_period": strategy.params.fast_period,
            "slow_period": strategy.params.slow_period,
        }
    )
    print_summary(summary)

    if config["plot"]:
        report_data = build_backtest_report_data(
            strategy,
            config,
            [config["fast"], config["slow"]],
        )
        generate_html_report(report_data, config, getattr(strategy, "log_messages", []))

    return summary


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

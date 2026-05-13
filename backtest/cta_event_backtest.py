from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import backtrader as bt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest import (
    TEST_CASES,
    add_analyzers,
    build_data_feed,
    create_cerebro,
)
from utils.backtest_report_builder import (
    build_backtest_report_data,
    summarize_result,
)
from utils.project_utils import load_daily_data
from utils.backtest_report import html as generate_backtest_html
from utils.path_utils import ensure_dir
from utils.h_strategy import HStrategy


CONFIG: dict[str, Any] = {
    "code": "sz.002594",
    "adjust_flag": "hfq",
    "from_date": "2020-01-01",
    "to_date": None,
    "data_from_date": "2019-01-01",
    "cash": 100000.0,
    "commission": 0.0001,
    "lot_size": 100,
    "position_ratio": 0.95,
    "buy_price_buffer": 1.02,
    "breakout_lookback": 30,
    "long_trend_period": 180,
    "volume_ma_period": 20,
    "volume_spike_multiplier": 2.6,
    "stop_loss_pct": 0.08,
    "take_profit_pct": 0.18,
    "max_holding_days": 15,
    "print_log": True,
    "plot": True,
    "benchmark_code": "",
    "report_dir": "logs/backtest",
    "report_name": "cta_event_backtest",
    "strategy_name": "CTA事件驱动策略",
}


class CTAEventDrivenStrategy(HStrategy):
    params = (
        ("p", None),
        ("printlog", True),
    )

    def __init__(self) -> None:
        super().__init__(allow_log=self.params.printlog)
        self.param = self.p.p or {}
        self.order = None
        self.buy_markers: list[tuple[pd.Timestamp, float]] = []
        self.sell_markers: list[tuple[pd.Timestamp, float]] = []
        self.position_days_total = 0
        self.idle_cash_days_total = 0
        self.has_completed_sell = False
        self.last_buy_price: float | None = None
        self.entry_bar: int | None = None

        self.breakout_lookback = int(self.param.get("breakout_lookback", 20))
        self.long_trend_period = int(self.param.get("long_trend_period", 120))
        self.volume_ma_period = int(self.param.get("volume_ma_period", 20))
        self.long_trend_ma = bt.indicators.SimpleMovingAverage(
            self.data.close,
            period=self.long_trend_period,
        )
        self.volume_ma = bt.indicators.SimpleMovingAverage(
            self.data.volume,
            period=self.volume_ma_period,
        )

        from_date = self.param.get("from_date")
        self.from_date = pd.Timestamp(from_date).to_pydatetime() if from_date else None

    def _calculate_buy_size(self) -> int:
        lot_size = max(int(self.param.get("lot_size", 100)), 1)
        position_ratio = float(self.param.get("position_ratio", 0.95))
        available_cash = self.broker.getcash() * position_ratio
        estimated_price = max(
            float(self.data.open[0]),
            float(self.data.close[0]),
            float(self.data.high[0]),
        ) * float(self.param.get("buy_price_buffer", 1.02))
        if estimated_price <= 0:
            return 0
        size = int(available_cash / estimated_price)
        size = (size // lot_size) * lot_size
        return max(size, 0)

    def _highest_recent_high(self) -> float | None:
        if len(self) <= self.breakout_lookback:
            return None
        return max(float(self.data.high[-i]) for i in range(1, self.breakout_lookback + 1))

    def notify_order(self, order: bt.Order) -> None:
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status == order.Completed:
            executed_at = pd.Timestamp(self.datas[0].datetime.date(0))
            if order.isbuy():
                self.last_buy_price = float(order.executed.price)
                self.entry_bar = len(self)
                self.buy_markers.append((executed_at, float(order.executed.price)))
                self.log(
                    f"成交！ 事件买入价={order.executed.price:.2f} "
                    f"数量={order.executed.size:.0f} "
                    f"手续费={order.executed.comm:.2f}"
                )
            else:
                self.last_buy_price = None
                self.entry_bar = None
                self.has_completed_sell = True
                self.sell_markers.append((executed_at, float(order.executed.price)))
                self.log(
                    f"成交！ 事件卖出价={order.executed.price:.2f} "
                    f"数量={abs(order.executed.size):.0f} "
                    f"手续费={order.executed.comm:.2f}"
                )
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_map = {
                order.Canceled: "已取消",
                order.Margin: "资金不足",
                order.Rejected: "已拒绝",
            }
            self.log(f"订单失败 状态={status_map.get(order.status, order.getstatusname())}")
        self.order = None

    def next(self) -> None:
        if self.from_date and self.datas[0].datetime.date(0) < self.from_date.date():
            return
        if self.order is not None:
            return

        if self.position:
            self.position_days_total += 1
        elif self.has_completed_sell:
            self.idle_cash_days_total += 1

        current_close = float(self.data.close[0])

        if not self.position:
            recent_high = self._highest_recent_high()
            if recent_high is None:
                return

            is_trend_up = current_close > float(self.long_trend_ma[0])
            volume_ratio = (
                float(self.data.volume[0]) / float(self.volume_ma[0])
                if float(self.volume_ma[0]) > 0
                else 0.0
            )
            is_breakout = current_close >= recent_high
            is_event_day = volume_ratio >= float(self.param.get("volume_spike_multiplier", 1.8))
            if is_trend_up and is_breakout and is_event_day:
                size = self._calculate_buy_size()
                if size <= 0:
                    self.log("事件触发成功，但资金不足，放弃开仓")
                    return
                self.log(
                    f"事件触发买入 收盘价={current_close:.2f} "
                    f"突破前高={recent_high:.2f} 成交量放大={volume_ratio:.2f}倍 "
                    f"数量={size}"
                )
                self.order = self.buy(size=size)
            return

        if self.last_buy_price is None:
            return

        holding_days = len(self) - self.entry_bar if self.entry_bar is not None else 0
        stop_loss_price = self.last_buy_price * (1 - float(self.param.get("stop_loss_pct", 0.1)))
        take_profit_price = self.last_buy_price * (1 + float(self.param.get("take_profit_pct", 0.25)))
        trend_break = current_close < float(self.long_trend_ma[0])
        stop_loss_hit = current_close <= stop_loss_price
        take_profit_hit = current_close >= take_profit_price
        timeout = holding_days >= int(self.param.get("max_holding_days", 30))

        if stop_loss_hit or take_profit_hit or trend_break or timeout:
            reason = "趋势跌破"
            if stop_loss_hit:
                reason = "止损"
            elif take_profit_hit:
                reason = "止盈"
            elif timeout:
                reason = "超时"
            self.log(
                f"事件策略卖出 原因={reason} "
                f"收盘价={current_close:.2f} 持仓天数={holding_days}"
            )
            self.order = self.close()

    def stop(self) -> None:
        self.log(
            f"回测结束 事件窗口={self.breakout_lookback} "
            f"期末资产={self.broker.getvalue():.2f}"
        )


def validate_config(config: dict[str, Any]) -> None:
    if float(config["cash"]) <= 0:
        raise ValueError("cash 必须大于 0")
    if float(config["commission"]) < 0:
        raise ValueError("commission 不能小于 0")
    if int(config["lot_size"]) <= 0:
        raise ValueError("lot_size 必须大于 0")
    if float(config["position_ratio"]) <= 0 or float(config["position_ratio"]) > 1:
        raise ValueError("position_ratio 必须大于 0 且小于等于 1")
    if float(config["buy_price_buffer"]) < 1:
        raise ValueError("buy_price_buffer 不能小于 1")
    if int(config["breakout_lookback"]) <= 1:
        raise ValueError("breakout_lookback 必须大于 1")
    if int(config["long_trend_period"]) <= int(config["breakout_lookback"]):
        raise ValueError("long_trend_period 必须大于 breakout_lookback")
    if int(config["volume_ma_period"]) <= 1:
        raise ValueError("volume_ma_period 必须大于 1")
    if float(config["volume_spike_multiplier"]) <= 1:
        raise ValueError("volume_spike_multiplier 必须大于 1")
    if float(config["stop_loss_pct"]) <= 0 or float(config["stop_loss_pct"]) >= 1:
        raise ValueError("stop_loss_pct 必须大于 0 且小于 1")
    if float(config["take_profit_pct"]) <= 0:
        raise ValueError("take_profit_pct 必须大于 0")
    if int(config["max_holding_days"]) <= 0:
        raise ValueError("max_holding_days 必须大于 0")


def _print_summary(summary: dict[str, Any], config: dict[str, Any]) -> None:
    print("回测结果:")
    print(f"  事件突破窗口: {config['breakout_lookback']}")
    print(f"  趋势均线: {config['long_trend_period']}")
    print(f"  放量阈值: {config['volume_spike_multiplier']:.2f}")
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


def _generate_html_report(report_data: list[dict[str, Any]], config: dict[str, Any], log_lines: list[str]) -> None:
    if not report_data:
        print("没有可用的回测数据来生成报告")
        return
    report_dir = ensure_dir(PROJECT_ROOT / config["report_dir"])
    report_path = report_dir / f"{config['report_name']}-{config['code']}.html"
    generate_backtest_html(
        report_data,
        str(report_path),
        [],
        f"{config.get('strategy_name', 'CTA事件驱动策略')} 回测报告",
        log_lines=log_lines,
    )
    print(f"HTML 回测报告: {report_path}")


def run_backtest(config: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    validate_config(config)
    cerebro = create_cerebro(config)
    cerebro.addstrategy(
        CTAEventDrivenStrategy,
        printlog=config["print_log"],
        p=config,
    )
    cerebro.adddata(build_data_feed(df, config.get("data_from_date"), config.get("to_date")))
    add_analyzers(cerebro)

    initial_value = cerebro.broker.getvalue()
    print(f"开始回测: 股票={config['code']}，初始资金={initial_value:.2f}")
    strategy = cerebro.run()[0]
    summary = summarize_result(strategy, initial_value)
    _print_summary(summary, config)

    if config["plot"]:
        report_data = build_backtest_report_data(
            strategy,
            config,
            [config["long_trend_period"]],
        )
        _generate_html_report(report_data, config, getattr(strategy, "log_messages", []))
    return summary


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])
    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

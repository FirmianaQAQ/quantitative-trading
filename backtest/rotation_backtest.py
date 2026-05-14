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
    add_analyzers,
    build_data_feed,
    create_cerebro,
)
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
from utils.a_share_costs import estimate_max_buy_size, validate_a_share_cost_config


TEST_CASES = [
    {
        "code": "rotation_core5",
        "label": "核心五股轮动",
        "required_codes": [
            "sz.000100",
            "sz.000725",
            "sz.001308",
            "sz.002594",
            "sh.600580",
        ],
    },
    {
        "code": "rotation_panel3",
        "label": "光学三股轮动",
        "required_codes": [
            "sz.000100",
            "sz.000725",
            "sz.001308",
        ],
    },
    {
        "code": "rotation_growth3",
        "label": "成长三股轮动",
        "required_codes": [
            "sz.001308",
            "sz.002594",
            "sh.600580",
        ],
    },
]

ROTATION_CASE_MAP = {item["code"]: item for item in TEST_CASES}

CONFIG: dict[str, Any] = {
    "code": "rotation_core5",
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
    "investment_ratio": 0.95,
    "rebalance_period": 20,
    "momentum_short_period": 10,
    "momentum_long_period": 40,
    "trend_ma_period": 120,
    "volatility_period": 20,
    "momentum_short_weight": 0.35,
    "momentum_long_weight": 0.85,
    "volatility_penalty_weight": 0.20,
    "top_n": 1,
    "stop_loss_pct": 0.12,
    "print_log": True,
    "plot": True,
    "report_dir": "logs/backtest",
    "report_name": "rotation_backtest",
    "strategy_name": "多因子轮动策略",
    "strategy_brief": "多股强弱轮动",
}


def get_rotation_case(rotation_code: str) -> dict[str, Any]:
    case = ROTATION_CASE_MAP.get(rotation_code)
    if case is None:
        available = ", ".join(ROTATION_CASE_MAP.keys())
        raise ValueError(f"未知轮动组合: {rotation_code}，可选值: {available}")
    return case


def _build_nav_price_frame(
    returns_series: pd.Series,
    initial_value: float,
) -> pd.DataFrame:
    if returns_series.empty:
        return pd.DataFrame(
            columns=["date", "open", "high", "low", "close", "volume"]
        )

    nav = (1 + returns_series.sort_index()).cumprod() * initial_value
    frame = pd.DataFrame(
        {
            "date": nav.index,
            "open": nav.values,
            "high": nav.values,
            "low": nav.values,
            "close": nav.values,
            "volume": 0.0,
        }
    )
    return frame.reset_index(drop=True)


class RotationStrategy(HStrategy):
    params = (
        ("p", None),
        ("printlog", True),
    )

    def __init__(self) -> None:
        super().__init__(allow_log=self.params.printlog)

        self.param = self.p.p or {}
        self.pending_order_refs: set[int] = set()
        self.buy_markers: list[tuple[pd.Timestamp, float]] = []
        self.sell_markers: list[tuple[pd.Timestamp, float]] = []
        self.position_days_total = 0
        self.idle_cash_days_total = 0
        self.has_completed_sell = False

        self.current_target_data: bt.LineIterator | None = None
        self.pending_entry_data: bt.LineIterator | None = None
        self.current_entry_price: float | None = None
        self.last_rebalance_bar: int | None = None

        from_date = self.param.get("from_date")
        self.from_date = pd.Timestamp(from_date).to_pydatetime() if from_date else None

        self.momentum_short_period = int(self.param.get("momentum_short_period", 20))
        self.momentum_long_period = int(self.param.get("momentum_long_period", 60))
        self.trend_ma_period = int(self.param.get("trend_ma_period", 120))
        self.volatility_period = int(self.param.get("volatility_period", 20))
        self.min_history_period = max(
            self.momentum_short_period,
            self.momentum_long_period,
            self.trend_ma_period,
            self.volatility_period + 1,
        )

        self.indicators_by_data: dict[bt.LineIterator, dict[str, bt.Indicator]] = {}
        for data in self.datas:
            daily_return = (data.close / data.close(-1)) - 1
            self.indicators_by_data[data] = {
                "trend_ma": bt.indicators.SimpleMovingAverage(
                    data.close,
                    period=self.trend_ma_period,
                ),
                "volatility": bt.indicators.StandardDeviation(
                    daily_return,
                    period=self.volatility_period,
                ),
            }

    def _has_position(self) -> bool:
        return any(abs(self.getposition(data).size) > 0 for data in self.datas)

    def _current_nav(self) -> float:
        return float(self.broker.getvalue())

    def _add_pending_order(self, order: bt.Order | None) -> None:
        if order is not None:
            self.pending_order_refs.add(order.ref)

    def _score_data(self, data: bt.LineIterator) -> float | None:
        if len(data) <= self.min_history_period:
            return None

        trend_ma = float(self.indicators_by_data[data]["trend_ma"][0])
        current_close = float(data.close[0])
        if trend_ma <= 0 or current_close <= trend_ma:
            return None

        past_short_close = float(data.close[-self.momentum_short_period])
        past_long_close = float(data.close[-self.momentum_long_period])
        if past_short_close <= 0 or past_long_close <= 0:
            return None

        momentum_short = current_close / past_short_close - 1
        momentum_long = current_close / past_long_close - 1
        volatility = float(self.indicators_by_data[data]["volatility"][0])

        return (
            momentum_long * float(self.param.get("momentum_long_weight", 0.85))
            + momentum_short * float(self.param.get("momentum_short_weight", 0.35))
            - volatility * float(self.param.get("volatility_penalty_weight", 0.20))
        )

    def _pick_target_data(self) -> bt.LineIterator | None:
        scored: list[tuple[float, bt.LineIterator]] = []
        for data in self.datas:
            score = self._score_data(data)
            if score is None:
                continue
            scored.append((score, data))

        if not scored:
            return None

        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    def _calculate_buy_size(self, data: bt.LineIterator) -> int:
        price = float(data.close[0])
        return estimate_max_buy_size(
            available_cash=self.broker.getcash(),
            price=price,
            lot_size=max(int(self.param.get("lot_size", 100)), 1),
            cash_usage_ratio=float(self.param.get("investment_ratio", 0.95)),
            config=self.param,
        )

    def _submit_entry(self, data: bt.LineIterator) -> bool:
        size = self._calculate_buy_size(data)
        if size <= 0:
            self.log(f"{data._name} 轮动开仓失败，可用资金不足")
            return False

        dt = pd.Timestamp(self.datas[0].datetime.date(0))
        nav_value = self._current_nav()
        self.buy_markers.append((dt, nav_value))
        self.log(
            f"轮动买入 {data._name} 数量={size} "
            f"价格={float(data.close[0]):.2f}"
        )
        self._add_pending_order(self.buy(data=data, size=size))
        return True

    def _submit_exit_all(self, reason: str) -> bool:
        if not self._has_position():
            return False

        dt = pd.Timestamp(self.datas[0].datetime.date(0))
        nav_value = self._current_nav()
        self.sell_markers.append((dt, nav_value))
        self.log(f"轮动清仓 原因={reason}")
        for data in self.datas:
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
            if order.isbuy():
                self.current_target_data = order.data
                self.current_entry_price = float(order.executed.price)
            elif (
                self.current_target_data is order.data
                and self.getposition(order.data).size == 0
            ):
                self.current_target_data = None
                self.current_entry_price = None
                self.has_completed_sell = True
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

    def next(self) -> None:
        if self.from_date and self.datas[0].datetime.date(0) < self.from_date.date():
            return

        if self._has_position():
            self.position_days_total += 1
        elif self.has_completed_sell:
            self.idle_cash_days_total += 1

        if self.pending_order_refs:
            return

        if not self._has_position() and self.pending_entry_data is not None:
            target_data = self.pending_entry_data
            self.pending_entry_data = None
            self._submit_entry(target_data)
            return

        if self.current_target_data is not None and self.current_entry_price:
            current_close = float(self.current_target_data.close[0])
            pnl_pct = current_close / self.current_entry_price - 1
            if pnl_pct <= -float(self.param.get("stop_loss_pct", 0.12)):
                self.pending_entry_data = None
                self._submit_exit_all(f"单票止损 pnl={pnl_pct * 100:.2f}%")
                return

        should_rebalance = (
            self.last_rebalance_bar is None
            or len(self) - self.last_rebalance_bar >= int(self.param.get("rebalance_period", 20))
        )
        if not should_rebalance:
            return

        self.last_rebalance_bar = len(self)
        target_data = self._pick_target_data()
        if target_data is None:
            self.pending_entry_data = None
            self._submit_exit_all("无满足条件标的")
            return

        if self.current_target_data is target_data and self._has_position():
            self.log(f"轮动继续持有 {target_data._name}")
            return

        if self._has_position():
            self.pending_entry_data = target_data
            self._submit_exit_all(f"切换至更强标的 {target_data._name}")
            return

        self._submit_entry(target_data)

    def stop(self) -> None:
        self.log(
            f"回测结束 轮动周期={self.param.get('rebalance_period')} "
            f"期末资产={self.broker.getvalue():.2f}"
        )


def validate_config(config: dict[str, Any]) -> None:
    get_rotation_case(config["code"])
    if config["cash"] <= 0:
        raise ValueError("初始资金 cash 必须大于 0")
    validate_a_share_cost_config(config)
    if int(config["lot_size"]) <= 0:
        raise ValueError("lot_size 必须大于 0")
    if float(config["investment_ratio"]) <= 0 or float(config["investment_ratio"]) > 1:
        raise ValueError("investment_ratio 必须大于 0 且小于等于 1")
    if int(config["rebalance_period"]) <= 0:
        raise ValueError("rebalance_period 必须大于 0")
    if int(config["momentum_short_period"]) <= 1:
        raise ValueError("momentum_short_period 必须大于 1")
    if int(config["momentum_long_period"]) <= int(config["momentum_short_period"]):
        raise ValueError("momentum_long_period 必须大于 momentum_short_period")
    if int(config["trend_ma_period"]) <= 1:
        raise ValueError("trend_ma_period 必须大于 1")
    if int(config["volatility_period"]) <= 1:
        raise ValueError("volatility_period 必须大于 1")
    if int(config["top_n"]) != 1:
        raise ValueError("当前版本 top_n 仅支持 1")
    if float(config["stop_loss_pct"]) <= 0 or float(config["stop_loss_pct"]) >= 1:
        raise ValueError("stop_loss_pct 必须大于 0 且小于 1")


def _print_summary(summary: dict[str, Any], config: dict[str, Any], basket_label: str) -> None:
    print("回测结果:")
    print(f"  组合: {basket_label}")
    print(f"  轮动周期: {config['rebalance_period']}")
    print(f"  短动量周期: {config['momentum_short_period']}")
    print(f"  长动量周期: {config['momentum_long_period']}")
    print(f"  趋势均线: {config['trend_ma_period']}")
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


def _build_report_data(
    strategy: RotationStrategy,
    config: dict[str, Any],
    basket_label: str,
    initial_value: float,
    summary: dict[str, Any],
) -> list[dict[str, Any]]:
    returns_series = build_returns_series(strategy)
    if returns_series.empty:
        return []

    nav_price_df = _build_nav_price_frame(returns_series, initial_value)
    filtered_df = filter_backtest_data(
        nav_price_df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    ).copy()
    total_days = summary["position_days_total"] + summary["idle_cash_days_total"]
    summary_metrics = format_summary_metrics(
        [
            {"label": "组合", "value": basket_label},
            {"label": "策略名称", "value": config.get("strategy_name", "多因子轮动策略")},
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
        indicator_lines=[],
        benchmark_returns=None,
        strategy_name="本策略",
        benchmark_name="基准",
        asset_name="组合净值",
        buy_sell_subtitle=f"{basket_label} 轮动信号",
    )


def _generate_html_report(
    report_data: list[dict[str, Any]],
    config: dict[str, Any],
    basket_label: str,
    log_lines: list[str] | None = None,
) -> None:
    if not report_data:
        print("没有可用的回测数据来生成报告")
        return
    report_dir = ensure_dir(PROJECT_ROOT / config["report_dir"])
    html_report_path = report_dir / f"{config['report_name']}-{config['code']}.html"
    title = f"{basket_label} {config.get('strategy_name', '多因子轮动策略')} 回测报告"
    generate_backtest_html(
        report_data,
        str(html_report_path),
        [],
        title,
        log_lines=log_lines,
        current_position=str(config.get("current_position", "auto")),
    )
    print(f"HTML 回测报告: {html_report_path}")


def run_backtest(config: dict[str, Any], df: pd.DataFrame | None = None) -> dict[str, Any]:
    del df
    validate_config(config)
    rotation_case = get_rotation_case(config["code"])
    basket_codes = [str(code) for code in rotation_case["required_codes"]]
    basket_label = str(rotation_case.get("label", config["code"]))

    cerebro = create_cerebro(config)
    cerebro.addstrategy(
        RotationStrategy,
        printlog=config["print_log"],
        p=config,
    )
    for stock_code in basket_codes:
        data_df = load_daily_data(stock_code, config["adjust_flag"])
        cerebro.adddata(
            build_data_feed(data_df, config.get("data_from_date"), config.get("to_date")),
            name=stock_code,
        )
    add_analyzers(cerebro)

    initial_value = cerebro.broker.getvalue()
    print(f"开始回测: 组合={basket_label}，初始资金={initial_value:.2f}")
    strategy = cerebro.run()[0]
    summary = summarize_result(strategy, initial_value)
    summary["basket_codes"] = basket_codes
    _print_summary(summary, config, basket_label)

    if config["plot"]:
        report_data = _build_report_data(
            strategy,
            config,
            basket_label,
            initial_value,
            summary,
        )
        _generate_html_report(report_data, config, basket_label, strategy.log_messages)

    return summary


def main(config: dict[str, Any]) -> None:
    run_backtest(config, None)


if __name__ == "__main__":
    main(CONFIG)

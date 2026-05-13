from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest import (
    CONFIG as BASE_CONFIG,
    TEST_CASES,
    SimpleMovingAverageStrategy,
    add_analyzers,
    build_data_feed,
    create_cerebro,
    generate_html_report,
    print_summary,
    validate_config,
)
from utils.backtest_report_builder import (
    build_backtest_report_data,
    summarize_result,
)
from utils.project_utils import load_daily_data


CONFIG: dict[str, Any] = dict(BASE_CONFIG)
CONFIG.update(
    {
        # V2 改成多标的稳健版，优先提升样本整体平均年化收益率。
        "buy_cash_ratio": 0.99,
        "buy_trigger_multiplier": 1.04,
        "buy_trigger_window": 8,
        "buy_rise_window": 4,
        "buy_rise_days_required": 1,
        "sell_trigger_multiplier": 0.95,
        "stop_loss_pct": 0.1,
        "fast": 8,
        "slow": 238,
        # 保留突破模式，但把阈值收得更稳，避免单票过拟合。
        "breakout_power_threshold": 0.6,
        "breakout_buy_limit_multiplier": 1.0,
        "benchmark_code": "",
        "report_name": "simple_ma_backtest_v2",
        "strategy_name": "普通双均线V2",
    }
)


class SimpleMovingAverageStrategyV2(SimpleMovingAverageStrategy):
    def _check_and_buy(self) -> None:
        if self.position:
            return False

        highest_price_previous = self.get_last_minmax_price(weighted=True)
        lowest_price_previous = self.get_last_lowest_price_weighted()
        if lowest_price_previous is None:
            return False

        current_close = float(self.data.close[0])
        buy_trigger_price = lowest_price_previous * float(
            self.param.get("buy_trigger_multiplier")
        )
        if not self.buy_trigger_active and current_close <= buy_trigger_price:
            self.buy_trigger_active = True
            self.buy_trigger_days_seen = 0
            self.log(
                f"价格触发买入观察窗口 收盘价={current_close:.2f} "
                f"上次最低价={lowest_price_previous:.2f} 阈值={buy_trigger_price:.2f}"
            )

        if self.buy_trigger_active:
            self.buy_trigger_days_seen += 1

            if highest_price_previous is None:
                self.log("不知道之前的最高价，心里没底，不买了")
                return False

            lowest_price = self.get_now_minmax_price(False) or lowest_price_previous
            power = self.get_power()
            breakout_power_threshold = float(
                self.param.get("breakout_power_threshold", 0.6)
            )
            breakout_buy_limit_multiplier = float(
                self.param.get("breakout_buy_limit_multiplier", 1.02)
            )
            is_breakout_mode = self.now_is_up_day and power >= breakout_power_threshold

            limit_multiplier = (
                breakout_buy_limit_multiplier
                if is_breakout_mode
                else float(self.param.get("sell_trigger_multiplier"))
            )
            buy_limit = (
                highest_price_previous - lowest_price
            ) * limit_multiplier + lowest_price
            if current_close >= buy_limit and not is_breakout_mode:
                self.log(f"价格接近上周期最高价 {highest_price_previous:.2f}，不买了")
                return False

            rise_window = int(self.param.get("buy_rise_window"))
            rise_days_required = int(self.param.get("buy_rise_days_required"))
            if self.now_is_up_day:
                if power > 0.5:
                    rise_days_required -= 1
                elif power < 0.5:
                    rise_days_required += 1
            else:
                if power < 0:
                    rise_days_required -= 2
                elif power < 0.5:
                    rise_days_required -= 1
                elif power > 0.8:
                    rise_days_required += 1
            rise_days_required = max(rise_days_required, 1)

            self.log(f"power={power}")

            if len(self.buy_trigger_up_days) == rise_window:
                rise_days_num = sum(self.buy_trigger_up_days)
                price_change = (
                    (current_close - lowest_price) / lowest_price if lowest_price else 0
                )
                plan_b = (
                    rise_days_num > (rise_days_required - 1) and price_change >= 0.03
                )
                if rise_days_num >= rise_days_required or plan_b:
                    size = self.calculate_buy_size()
                    if size <= 0:
                        self.log(
                            f"买点已满足，但可用资金不足 当前现金={self.broker.getcash():.2f}"
                        )
                        self.reset_buy_setup()
                        return False
                    if is_breakout_mode and current_close >= buy_limit:
                        self.log(
                            "强趋势突破模式买入，允许价格略高于上一轮高点 "
                            f"power={power:.2f} 收盘价={current_close:.2f} "
                            f"突破上限={buy_limit:.2f}"
                        )
                    self.log(
                        "买点满足"
                        + (
                            "(差1天，但上涨金额够)"
                            if plan_b
                            else f"({rise_window}日{rise_days_required}涨power={power})，"
                        )
                        + f"下单买入 收盘价={current_close:.2f} "
                        f"数量={size} 当前现金={self.broker.getcash():.2f} "
                        f"上一个最低价={lowest_price_previous:.2f} "
                        f"买入价不高于 {highest_price_previous:.2f} * {limit_multiplier:.2f} = {buy_limit:.2f}"
                    )
                    self.order = self.buy(size=size)
                    self.reset_buy_setup()
                    return True

            if self.buy_trigger_days_seen >= int(self.param.get("buy_trigger_window")):
                self.log(
                    f"买入观察窗口到期，未满足"
                    f"{rise_window}日{rise_days_required}涨，放弃本次买入"
                )
                self.reset_buy_setup()
        return False


def run_backtest(config: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    cerebro = create_cerebro(config)
    cerebro.addstrategy(
        SimpleMovingAverageStrategyV2,
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

r"""

Versatile回测使用说明
-----------------
基本描述
  - Versatile回测是一个专为震荡市设计的回测框架，旨在帮助交易者学会在震荡市场中生存和盈利。它提供了丰富的工具和功能，使交易者能够更好地理解市场行为，优化交易策略，并提高交易绩效。
  - Versatile回测的核心理念是：真正的交易高手，不是预测趋势，而是学会和震荡市相处。通过使用Versatile回测，交易者可以学习如何在震荡市场中识别机会，管理风险，并实现稳定的盈利。
  Versatile回测的主要功能包括：
  1. **多样化的市场环境模拟**：Versatile回测能够模拟不同类型的市场环境，包括震荡市、趋势市和混合市，帮助交易者适应不同的市场条件。
  2. **丰富的技术指标和工具**：Versatile回测提供了大量的技术指标和分析工具，帮助交易者更好地理解市场行为，并优化交易策略。
  3. **灵活的策略开发和测试**：交易者可以使用Versatile回测开发和测试各种交易策略，包括趋势跟踪、均值回归、突破策略等，找到最适合震荡市场的策略。
  4. **风险管理和绩效分析**：Versatile回测提供了全面的风险管理工具和绩效分析功能，帮助交易者评估策略的风险和回报，并做出明智的交易决策。

基础配置逻辑
  1. 回测参数都放在本文件顶部的 CONFIG 变量里，不再使用命令行传参。
  2. 单次回测时：
      - 把 CONFIG["optimize"] 设为 False
      - 设置 code、from_date、to_date、fast、slow 等参数
  3. 优化回测参数时：
      - 把 CONFIG["optimize"] 设为 True  
      - 设置 optimize_params 里的opt_fast 和 opt_slow等参数范围，格式为 start:end:step，例如 5:20:5
  4. 运行方式：
      - venv\Scripts\python.exe backtest\versatile.py
  5. 常用参数：
      - code: 股票代码，例如 sh.000001
      - optimize: 是否进行参数优化，例如 True 或 False
      - optimize_params: 参数优化的范围，例如 {"opt_fast": "5:20:5", "opt_slow": "20:50:10"}  
      - adjust_flag: 复权类型，例如 cq、qfq、hfq、dypre
      - from_date/to_date: 回测时间范围，格式 YYYY-MM-DD
      - cash: 初始资金
      - commission: 券商佣金率，例如万分之 0.854 就是 0.0000854
      - stamp_duty: 卖出印花税率，A 股当前默认 0.0005
      - transfer_fee: 双边过户费率，A 股当前默认 0.00001
      - min_commission: 单笔最低佣金，A 股当前默认 5 元
      - buy_cash_ratio: 买入时使用现金的比例，给跳空和手续费留缓冲
      - buy_price_buffer: 按更高的估算成交价计算仓位，避免次日高开导致资金不足
      - lot_size: 每次买入按多少股的整数倍下单，A 股通常为 100
      - print_log: 是否打印交易日志
      - plot: 是否绘图
      - fast: 快速移动平均线周期，例如 5
      - slow: 慢速移动平均线周期，例如 20
      - opt_fast/opt_slow: 参数优化范围
      - top: 参数优化结果显示前几名
  6. 输出指标包括：
      - 总收益率、年化收益率、最大回撤、最大回撤金额、夏普比率
      - 总交易次数、盈利次数、亏损次数、胜率、净利润、平均每笔净利润
  7. 参考补丁：
      - 参考目录 /backtest/patches/* 下的补丁，了解如何使用 Versatile 回测框架进行策略开发和测试
      - 结合补丁中的示例代码，开发适合震荡市场的交易策略，优化参数，提升交易绩效  
      - 优先使用dypre.py和atr.py的补丁指标，避免过拟合和数据泄露  
      - 结合数据历史和市场行为，调整参数范围，找到适合震荡市的策略  
      - 可以参考 /utils/default_stocks.py 中的默认股票列表，选择适合震荡市的股票进行回测
      - 通过分析回测结果，识别策略的优势和不足，持续优化策略，适应不断变化的市场环境
      - 在回测过程中，注意风险管理，设置合理的止损和止盈水平，避免过度交易和情绪化决策
      - 可以参考/backtest/backtest_v1.py的参数配置、数据字段读取及回测逻辑，但是不要直接使用，结合Versatile回测的特点，进行策略开发和测试
      - 在回测结束后，仔细分析回测结果，识别策略的优势和不足，持续优化策略，适应不断变化的市场环境
策略实现
  - Versatile回测的核心策略是基于均线交叉的趋势跟踪策略，结合震荡市场的特点，调整了参数和逻辑，使其更适合在震荡市场中生存和盈利。
  - 主要的策略逻辑包括：
      1. 买入条件：当快速均线（例如 13 日）上穿慢速均线（例如 144 日）时，且满足震荡市场的特定条件（例如价格在一定范围内波动，或者近期没有明显的趋势），则触发买入信号。
      2. 卖出条件：当快速均线下穿慢速均线时，或者价格跌破某个止损水平，或者达到某个止盈水平时，触发卖出信号。
      3. 震荡市场的特定条件：可以根据价格的波动范围、近期的价格行为、技术指标的状态等因素来判断当前是否处于震荡市场，并调整买入和卖出的条件，使策略更适合在震荡市场中运行。
  - 通过调整均线的周期、买入和卖出的条件，以及震荡市场的判断逻辑，Versatile回测能够帮助交易者找到适合震荡市场的交易策略，实现稳定的盈利。
  - 在策略实现过程中，建议结合补丁中的示例代码，开发适合震荡市场的交易策略，优化参数，提升交易绩效。同时，在回测过程中，注意风险管理，设置合理的止损和止盈水平，避免过度交易和情绪化决策。 
"""

from __future__ import annotations

from typing import Any

from backtest.backtest_v1 import (
    run_backtest,
    run_optimization,
    validate_config as validate_base_config,
)
from utils.default_stocks import (
    DEFAULT_BASE_STRATEGY_ID,
    DEFAULT_BASE_STRATEGY_NAME,
    DEFAULT_PRIMARY_STOCK_CODE,
    build_default_stock_test_cases,
)
from utils.project_utils import load_daily_data


CONFIG: dict[str, Any] = {
    # 股票代码，例如 sh.000001 或 sz.000725
    "code": DEFAULT_PRIMARY_STOCK_CODE,
    # Versatile 默认使用动态前复权，信号更平滑，适合震荡区间观察结构变化
    "adjust_flag": "dypre",
    # 回测时间范围
    "from_date": "2020-01-01",
    "to_date": None,
    # 给慢线预留足够预热区间，避免震荡识别刚启动时失真
    "data_from_date": "2019-01-01",
    # 初始资金和 A 股费用模型
    "cash": 100000.0,
    "commission": 0.0000854,
    "stamp_duty": 0,
    "transfer_fee": 0,
    "min_commission": 5.0,
    # 震荡市默认更保守，留更多现金给二次确认
    "buy_cash_ratio": 0.25,
    "buy_price_buffer": 1.015,
    "lot_size": 100,
    # 买入观察窗口：更早关注低位，但要求更明确的回暖确认
    "buy_trigger_multiplier": 1.02,
    "buy_trigger_window": 10,
    "buy_rise_window": 6,
    "buy_rise_days_required": 3,
    # 卖出阈值略收紧，减少震荡利润回吐
    "sell_trigger_multiplier": 0.90,
    "stop_loss_pct": 0.12,
    "protect_profit_floor_pct": 0.03,
    "underwater_take_profit_pct": 0.06,
    "above_water_take_profit_pct": 0.16,
    "print_log": True,
    # 均线参数偏向震荡市，不再默认使用极慢的年线组合
    "fast": 13,
    "slow": 144,
    "plot": True,
    "benchmark_code": "sh.000001",
    "report_dir": "logs/backtest",
    "report_name": "base_backtest",
    "strategy_name": DEFAULT_BASE_STRATEGY_NAME,
    "strategy_brief": "震荡适配 + ATR短确认",
    "current_position": "auto",
    "enable_llm_analysis": False,
    # 默认启用 dypre 补丁做运行态校验，避免动态前复权数据异常静默通过
    "patches": ["dypre", "atr"],
    "patch_strict": False,
    # ATR 补丁继续保留，但使用短周期确认，避免用趋势突破逻辑过度拦截震荡低吸。
    "atr_period": 14,
    "atr_breakout_period": 5,
    "atr_breakout_confirm_pct": 0.0,
    "atr_exit_period": 5,
    "atr_risk_pct": 0.03,
    "atr_max_units": 2,
    "atr_add_unit_atr": 0.8,
    "atr_stop_atr_multiplier": 1.5,
    "atr_stop_loss_pct": 0.12,
    # 参数优化开关及范围
    "optimize": False,
    "opt_fast": "8:21:1",
    "opt_slow": "89:233:8",
    "top": 10,
}

STRATEGY_ID = DEFAULT_BASE_STRATEGY_ID

# 测试用例跟随全局默认股票池；以后只维护 default_stocks 即可。
TEST_CASES = build_default_stock_test_cases()


def validate_config(config: dict[str, Any]) -> None:
    validate_base_config(config)

    patches = {str(name).strip().lower() for name in config.get("patches", [])}
    if "atr" not in patches:
        return

    if int(config.get("atr_period", 0)) <= 0:
        raise ValueError("atr_period 必须大于 0")
    if int(config.get("atr_breakout_period", 0)) <= 0:
        raise ValueError("atr_breakout_period 必须大于 0")
    if int(config.get("atr_exit_period", 0)) <= 0:
        raise ValueError("atr_exit_period 必须大于 0")
    if int(config["atr_exit_period"]) > int(config["atr_breakout_period"]):
        raise ValueError("atr_exit_period 不能大于 atr_breakout_period")
    if float(config.get("atr_breakout_confirm_pct", 0)) < 0:
        raise ValueError("atr_breakout_confirm_pct 不能小于 0")
    if float(config.get("atr_risk_pct", 0)) <= 0 or float(
        config["atr_risk_pct"]
    ) >= 1:
        raise ValueError("atr_risk_pct 必须大于 0 且小于 1")
    if int(config.get("atr_max_units", 0)) <= 0:
        raise ValueError("atr_max_units 必须大于 0")
    if float(config.get("atr_add_unit_atr", 0)) <= 0:
        raise ValueError("atr_add_unit_atr 必须大于 0")
    if float(config.get("atr_stop_atr_multiplier", 0)) <= 0:
        raise ValueError("atr_stop_atr_multiplier 必须大于 0")
    if float(config.get("atr_stop_loss_pct", 0)) <= 0 or float(
        config["atr_stop_loss_pct"]
    ) >= 1:
        raise ValueError("atr_stop_loss_pct 必须大于 0 且小于 1")


def main(config: dict[str, Any]) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])

    if config.get("optimize"):
        run_optimization(config, df)
        return

    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

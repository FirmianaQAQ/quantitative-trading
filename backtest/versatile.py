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
      - 设置 optimize_params 里的 opt_fast、opt_slow、opt_buy_limit_position_pct、opt_protect_profit_floor_pct、opt_sell_trigger_multiplier 等参数范围
      - 整数参数格式为 start:end:step，例如 5:20:5
      - 小数参数格式同样支持 start:end:step，例如 0.75:0.95:0.05
  4. 运行方式：
      - venv\Scripts\python.exe backtest\versatile.py
  5. 常用参数：
      - code: 股票代码，例如 sh.000001
      - optimize: 是否进行参数优化，例如 True 或 False
      - optimize_params: 参数优化的范围，例如 {"opt_fast": "5:20:5", "opt_slow": "20:50:10", "opt_buy_limit_position_pct": "0.75:0.95:0.05", "opt_protect_profit_floor_pct": "0.02:0.05:0.01", "opt_sell_trigger_multiplier": "0.80:0.95:0.05"}  
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
      - opt_fast/opt_slow: 均线参数优化范围
      - opt_buy_limit_position_pct: 买入封顶区间位置优化范围
      - opt_protect_profit_floor_pct: 利润保底线优化范围
      - opt_sell_trigger_multiplier: 卖出触发系数优化范围
      - opt_score_annual_weight: 综合评分中年化收益的权重
      - opt_score_drawdown_weight: 综合评分中最大回撤的扣分权重
      - opt_score_sharpe_weight: 综合评分中夏普比率的加分权重
      - opt_score_trade_penalty_weight: 综合评分中交易次数的扣分权重
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
from utils.project_utils import load_daily_data, normalize_adjust_flag_name


# 调参指南（先看这里，再改 CONFIG）：
# 1. 想“更容易买到”：
#    - 调大 buy_trigger_multiplier
#    - 调小 buy_rise_days_required
#    - 调大 buy_limit_position_pct
# 2. 想“更谨慎，不乱追高”：
#    - 调小 buy_cash_ratio
#    - 调小 buy_limit_position_pct
#    - 调大 buy_rise_days_required
# 3. 想“拿得更久”：
#    - 调大 sell_trigger_multiplier
#    - 调大 above_water_take_profit_pct
#    - 调大 atr_stop_atr_multiplier
# 4. 想“止盈止损更快”：
#    - 调小 sell_trigger_multiplier
#    - 调小 underwater_take_profit_pct / above_water_take_profit_pct
#    - 调小 stop_loss_pct / atr_stop_atr_multiplier
# 5. 推荐优先调这 5 个：
#    - fast / slow：决定策略节奏，是最核心的结构参数
#    - buy_limit_position_pct：决定是否追高
#    - sell_trigger_multiplier：决定止盈是否果断
#    - stop_loss_pct：决定容忍回撤的上限
#    - buy_cash_ratio：决定单次下注力度
# 6. 常见联动关系：
#    - fast 调快后，通常要同步调小 buy_trigger_window，否则信号会变多但确认太慢
#    - slow 调慢后，通常要把 data_from_date 再往前拉，避免预热不足
#    - buy_rise_window 调大后，buy_rise_days_required 通常也要跟着调大
#    - 开启 atr 补丁后，stop_loss_pct 与 atr_stop_loss_pct 最好保持同一风险级别
# 7. 震荡市实战建议：
#    - 先从 buy_limit_position_pct、sell_trigger_multiplier、fast/slow 开始
#    - 不要一口气同时改 5 个以上参数，否则很难判断到底是谁起作用
# 8. 预设使用方法：
#    - 直接修改 ACTIVE_CONFIG_PRESET 为 conservative / balanced / aggressive
#    - 如需在预设基础上小改，只改 PRESET_OVERRIDES，不要直接散改整段参数
BASE_CONFIG: dict[str, Any] = {
    # 默认主标的代码。改这里会同时影响回测、同步数据和默认测试样本的首位。
    "code": DEFAULT_PRIMARY_STOCK_CODE,
    # 复权口径。dypre 表示信号用前复权、成交与持仓估值用不复权。
    "adjust_flag": "dypre",
    # 回测起始日期。
    "from_date": "2020-01-01",
    # 回测结束日期。None 表示取到最新数据。
    "to_date": None,
    # 预热数据起始日期。要早于 from_date，避免均线和窗口指标失真。
    "data_from_date": "2019-01-01",
    # 初始资金。
    "cash": 100000.0,
    # 券商佣金率，按成交额双边收取。
    "commission": 0.0000854,
    # 卖出印花税率。
    "stamp_duty": 0,
    # 双边过户费率。
    "transfer_fee": 0,
    # 单笔最低佣金。
    "min_commission": 5.0,
    # 买入时使用的现金比例。越小越保守，越大越激进；如果经常满仓后被套，这里先往下调。
    "buy_cash_ratio": 0.25,
    # 买入仓位计算时的价格缓冲，防止次日高开导致资金不足；如果经常下单失败可适当调大。
    "buy_price_buffer": 1.015,
    # 每次下单的最小股数单位，A 股通常是 100。
    "lot_size": 100,
    # 买入触发阈值。越大越容易启动观察窗口，越小越强调“真低位”。
    "buy_trigger_multiplier": 1.02,
    # 买入观察窗口长度，单位是交易日。越大越愿意等确认，越小越偏快进快出。
    "buy_trigger_window": 10,
    # 最近多少个交易日参与上涨天数统计。越大越看中连续性，越小越看重短促反弹。
    "buy_rise_window": 6,
    # 观察窗口内至少需要多少个上涨日才允许买入。越大越严格，越小越容易出手。
    "buy_rise_days_required": 3,
    # 买入封顶位置百分比。取值越小越不追高，越大越允许追到区间高位附近。
    "buy_limit_position_pct": 0.90,
    # 卖出触发阈值。越小越容易提前落袋，越大越愿意拿利润去博更高空间。
    "sell_trigger_multiplier": 0.90,
    # 相对买入价的止损跌幅。越小止损越快，越大越能扛波动但回撤也会变大。
    "stop_loss_pct": 0.12,
    # 持仓时的保底盈利线。越大越容易把浮盈锁住，但也更容易被正常波动洗出去。
    "protect_profit_floor_pct": 0.03,
    # 股价仍在水下时的止盈阈值。建议比水上止盈更小，避免弱势反弹利润回吐。
    "underwater_take_profit_pct": 0.06,
    # 股价已经在水上时的止盈阈值。越大越偏趋势持有，越小越偏震荡止盈。
    "above_water_take_profit_pct": 0.16,
    # 是否打印交易日志。
    "print_log": True,
    # 快均线周期。越小越敏感、信号越多；越大越钝化、信号越少。
    "fast": 13,
    # 慢均线周期。越大越偏中长期结构，越小越贴近中短线节奏。
    "slow": 144,
    # 是否生成 HTML 图表。
    "plot": True,
    # 绘图基准指数代码。空字符串表示不显示基准曲线。
    "benchmark_code": "sh.000001",
    # 回测报告输出目录。
    "report_dir": "logs/backtest",
    # 回测报告文件名前缀。
    "report_name": "base_backtest",
    # 报告里展示的策略名称。
    "strategy_name": DEFAULT_BASE_STRATEGY_NAME,
    # 报告里的策略简述。
    "strategy_brief": "震荡适配 + ATR短确认",
    # 当前持仓状态。auto / empty / hold。
    "current_position": "auto",
    # 是否启用大模型分析。
    "enable_llm_analysis": False,
    # 启用的补丁列表。dypre 负责数据校验，atr 负责波动率风控。
    "patches": ["dypre", "atr"],
    # 补丁严格模式。True 时补丁缺失或执行失败会直接报错。
    "patch_strict": False,
    # ATR 周期。越小越跟着短期波动走，越大越平滑。
    "atr_period": 14,
    # ATR 突破确认周期。越短越敏感，越长越保守；震荡市通常不建议太大。
    "atr_breakout_period": 5,
    # 突破确认百分比，0 表示收盘价直接高于突破线即可。
    "atr_breakout_confirm_pct": 0.0,
    # ATR 退出周期。越短越容易退出，越长越能容忍回踩。
    "atr_exit_period": 5,
    # ATR 风险预算占账户总资产比例。越大单笔仓位可能越重，回撤也更大。
    "atr_risk_pct": 0.03,
    # ATR 补丁最多允许加仓几次。震荡市不建议太高，否则容易越涨越追。
    "atr_max_units": 2,
    # 每次加仓使用的 ATR 倍数。越小越容易加仓，越大越要等更明显的扩展。
    "atr_add_unit_atr": 0.8,
    # ATR 止损倍数。越小越紧，越大越宽；和 atr_risk_pct 一起决定整体风险。
    "atr_stop_atr_multiplier": 1.5,
    # ATR 补丁对应的固定止损比例。建议与 stop_loss_pct 保持同一量级。
    "atr_stop_loss_pct": 0.12,
    # ATR 突破未确认时，是否继续保留当前买入观察窗口，而不是立刻当作失败。
    "patch_retry_on_breakout_block": False,
    # 是否执行参数优化。
    "optimize": False,
    # 优化时的快线取值范围，格式 start:end:step。
    "opt_fast": "8:21:1",
    # 优化时的买入封顶区间位置范围，小数参数同样支持 start:end:step。
    "opt_buy_limit_position_pct": "0.85:0.95:0.05",
    # 优化时的利润保底线范围，小数参数同样支持 start:end:step。
    "opt_protect_profit_floor_pct": "0.02:0.05:0.01",
    # 优化时的卖出触发系数范围，小数参数同样支持 start:end:step。
    "opt_sell_trigger_multiplier": "0.88:0.96:0.04",
    # 优化时的慢线取值范围，格式 start:end:step。
    "opt_slow": "89:233:8",
    # 综合评分：年化收益加分权重。
    "opt_score_annual_weight": 1.0,
    # 综合评分：最大回撤扣分权重。
    "opt_score_drawdown_weight": 1.1,
    # 综合评分：夏普比率加分权重。
    "opt_score_sharpe_weight": 10.0,
    # 综合评分：交易次数扣分权重。越大越不鼓励高频交易。
    "opt_score_trade_penalty_weight": 0.05,
    # 综合评分：补丁阻止买入次数扣分权重。越大越不鼓励基础信号和补丁节奏严重错位。
    "opt_score_blocked_buy_penalty_weight": 0.02,
    # 优化结果展示前几名。
    "top": 10,
}

# 预设参数库：
# - 这里只放“风格差异化参数”，没有出现的字段继续沿用 BASE_CONFIG
# - 三套预设的关系不是谁更高级，而是分别对应不同的风险偏好和行情阶段
# - 推荐流程：
#   1. 先选最接近当前行情的预设
#   2. 再通过 PRESET_OVERRIDES 做少量局部修正
#   3. 不要把所有个性化参数都塞回预设里，否则后面会越来越难维护
CONFIG_PRESETS: dict[str, dict[str, Any]] = {
    # conservative
    # 适用行情：
    # - 震荡偏弱
    # - 反弹不持续
    # - 你更在意先控制回撤
    # 核心取舍：
    # - 更少出手
    # - 更不追高
    # - 更快止盈止损
    # - 宁可错过一段，也尽量不在错误位置重仓
    "conservative": {
        # 报告里显示的预设名称。
        "strategy_brief": "保守震荡版",
        # 仓位控制：单次下注更轻，优先保护净值曲线。
        "buy_cash_ratio": 0.18,
        # 入场触发：更接近前低才启动观察，不抢模糊位置。
        "buy_trigger_multiplier": 1.01,
        # 观察窗口：确认时间更短，等不到就放弃。
        "buy_trigger_window": 8,
        # 反弹确认：看更长窗口，要求更扎实的回暖。
        "buy_rise_window": 7,
        "buy_rise_days_required": 4,
        # 追高限制：只接受区间偏低位置的买点。
        "buy_limit_position_pct": 0.82,
        # 出场节奏：更早落袋，不恋战。
        "sell_trigger_multiplier": 0.84,
        # 固定风控：止损更紧，回撤容忍度更低。
        "stop_loss_pct": 0.08,
        # 利润保护：小幅盈利也尽快锁住。
        "protect_profit_floor_pct": 0.02,
        "underwater_take_profit_pct": 0.04,
        "above_water_take_profit_pct": 0.12,
        # 均线节奏：快线更慢、慢线更长，减少假突破信号。
        "fast": 15,
        "slow": 169,
        # ATR 补丁：整体更保守，不鼓励频繁追单和层层加仓。
        "atr_breakout_period": 6,
        "atr_exit_period": 4,
        "atr_risk_pct": 0.02,
        "atr_max_units": 1,
        "atr_add_unit_atr": 1.0,
        "atr_stop_atr_multiplier": 1.2,
        "atr_stop_loss_pct": 0.08,
    },
    # balanced
    # 适用行情：
    # - 普通震荡市
    # - 反弹有延续，但强度一般
    # - 你还不确定当前该更保守还是更激进
    # 核心取舍：
    # - 风险收益尽量平衡
    # - 尽量避免明显短板
    # - 作为默认起点最合适
    "balanced": {
        # 报告里显示的预设名称。
        "strategy_brief": "均衡震荡版",
        # 仓位控制：不过轻也不过重。
        "buy_cash_ratio": 0.25,
        # 入场触发：既要接近低位，也允许适度提前观察。
        "buy_trigger_multiplier": 1.02,
        # 观察窗口：给信号正常确认空间，不追求极快也不拖太久。
        "buy_trigger_window": 10,
        # 反弹确认：对连续性要求中等。
        "buy_rise_window": 6,
        "buy_rise_days_required": 3,
        # 追高限制：不明显追高，但也不过度苛刻。
        "buy_limit_position_pct": 0.90,
        # 出场节奏：止盈节奏中性。
        "sell_trigger_multiplier": 0.90,
        # 固定风控：允许正常震荡，但不容忍深套。
        "stop_loss_pct": 0.12,
        # 利润保护：已有盈利后开始逐步锁盈。
        "protect_profit_floor_pct": 0.03,
        "underwater_take_profit_pct": 0.06,
        "above_water_take_profit_pct": 0.16,
        # 均线节奏：兼顾灵敏度和稳定性，是当前默认推荐参数。
        "fast": 13,
        "slow": 144,
        # ATR 补丁：保留波动率风控，但不过度压制震荡低吸。
        "atr_breakout_period": 5,
        "atr_exit_period": 5,
        "atr_risk_pct": 0.03,
        "atr_max_units": 2,
        "atr_add_unit_atr": 0.8,
        "atr_stop_atr_multiplier": 1.5,
        "atr_stop_loss_pct": 0.12,
    },
    # aggressive
    # 适用行情：
    # - 强势反弹初期
    # - 波动放大但资金回流明显
    # - 你愿意承担更大回撤换更高弹性
    # 核心取舍：
    # - 更早出手
    # - 更高仓位
    # - 更能容忍波动
    # - 更容易抓住第一波，但也更容易买早买高
    "aggressive": {
        # 报告里显示的预设名称。
        "strategy_brief": "激进抢反弹版",
        # 仓位控制：单次仓位更重，放大收益也放大回撤。
        "buy_cash_ratio": 0.35,
        # 入场触发：更容易启动观察，争取抢到第一波反弹。
        "buy_trigger_multiplier": 1.04,
        # 观察窗口：允许给更长时间等待反弹延续。
        "buy_trigger_window": 12,
        # 反弹确认：更看重短期转强，不等太久。
        "buy_rise_window": 5,
        "buy_rise_days_required": 2,
        # 追高限制：允许买到更靠近区间高位的位置。
        "buy_limit_position_pct": 0.96,
        # 出场节奏：更愿意让利润继续跑。
        "sell_trigger_multiplier": 0.95,
        # 固定风控：止损更宽，接受更大波动。
        "stop_loss_pct": 0.16,
        # 利润保护：锁盈更晚，给趋势更多空间。
        "protect_profit_floor_pct": 0.05,
        "underwater_take_profit_pct": 0.08,
        "above_water_take_profit_pct": 0.22,
        # 均线节奏：快线更敏感、慢线更短，更偏中短线抢反弹。
        "fast": 8,
        "slow": 89,
        # ATR 补丁：响应更快、容忍更宽，并允许更多层级加仓。
        "atr_breakout_period": 4,
        "atr_exit_period": 4,
        "atr_risk_pct": 0.04,
        "atr_max_units": 3,
        "atr_add_unit_atr": 0.6,
        "atr_stop_atr_multiplier": 2.0,
        "atr_stop_loss_pct": 0.16,
    },
}

# 当前启用的预设：
# - conservative: 更稳、更少交易
# - balanced: 默认推荐
# - aggressive: 更积极抢反弹
ACTIVE_CONFIG_PRESET = "balanced"

# 预设覆盖项：只放“想在当前预设基础上额外修改”的参数。
# 例如：
# PRESET_OVERRIDES = {
#     "code": "sz.000725",
#     "from_date": "2022-01-01",
#     "plot": False,
# }
PRESET_OVERRIDES: dict[str, Any] = {}


def build_config_from_preset(
    preset_name: str,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_name = str(preset_name or "").strip().lower()
    if normalized_name not in CONFIG_PRESETS:
        choices_text = ", ".join(sorted(CONFIG_PRESETS))
        raise ValueError(f"未知预设: {preset_name}，可选值: {choices_text}")
    return {
        **BASE_CONFIG,
        **CONFIG_PRESETS[normalized_name],
        **(overrides or {}),
    }


CONFIG: dict[str, Any] = build_config_from_preset(
    ACTIVE_CONFIG_PRESET,
    PRESET_OVERRIDES,
)

STRATEGY_ID = DEFAULT_BASE_STRATEGY_ID

# 测试用例跟随全局默认股票池；以后只维护 default_stocks 即可。
TEST_CASES = build_default_stock_test_cases()


def validate_config(config: dict[str, Any]) -> None:
    validate_base_config(config)

    raw_adjust_flag = str(config.get("adjust_flag", "")).strip().lower()
    normalized_adjust_flag = (
        "dypre"
        if raw_adjust_flag == "dypre"
        else normalize_adjust_flag_name(raw_adjust_flag)
    )
    if normalized_adjust_flag == "hfq":
        raise ValueError("versatile 不支持后复权（hfq），请使用 dypre、qfq 或 cq")

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

from __future__ import annotations

r"""

简单均线回测使用说明

1. 回测参数都放在本文件顶部的 CONFIG 变量里，不再使用命令行传参。
2. 单次回测时：
   - 把 CONFIG["optimize"] 设为 False
   - 设置 code、from_date、to_date、fast、slow 等参数
3. 参数优化时：
   - 把 CONFIG["optimize"] 设为 True
   - 设置 opt_fast 和 opt_slow，格式为 start:end:step，例如 5:20:5
4. 运行方式：
   - venv\Scripts\python.exe backtest\simple_ma_backtest.py
5. 常用参数：
   - code: 股票代码，例如 sh.000001
   - adjust_flag: 复权类型，例如 hfq、qfq
   - from_date/to_date: 回测时间范围，格式 YYYY-MM-DD
   - cash: 初始资金
   - commission: 手续费率
   - buy_cash_ratio: 买入时使用现金的比例，给跳空和手续费留缓冲
   - buy_price_buffer: 按更高的估算成交价计算仓位，避免次日高开导致资金不足
   - lot_size: 每次买入按多少股的整数倍下单，A 股通常为 100
   - print_log: 是否打印交易日志
   - plot: 是否绘图
   - fast/slow: 单次回测使用的快慢均线周期
   - opt_fast/opt_slow: 参数优化范围
   - top: 参数优化结果显示前几名
6. 输出指标包括：
   - 总收益率、年化收益率、最大回撤、最大回撤金额、夏普比率
   - 总交易次数、盈利次数、亏损次数、胜率、净利润、平均每笔净利润
"""

import sys
from pathlib import Path
from typing import Any
import json

import backtrader as bt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.path_utils import ensure_dir
from utils.backtest_report import html as generate_backtest_html
from utils.backtest_report_builder import (
    summarize_result,
    build_backtest_report_data,
)
from utils.project_utils import load_daily_data
from utils.h_strategy import HStrategy
from utils.chip_distribution import ChipDistribution

CONFIG: dict[str, Any] = {
    # 股票代码，例如 sh.000001 或 sz.000100 sz.000725
    "code": "sh.600580",
    # 复权类型，例如 hfq、qfq
    "adjust_flag": "hfq",
    # 回测时间范围，to_date 设为 None 表示取到数据末尾
    "from_date": "2020-01-01",
    "to_date": None,
    # 数据提前多久预热，如果用了年线，这个时间要比 from_date 提前 250 个交易日。
    # 如果把这个设为空，则会计算完整数据，性能慢一点
    "data_from_date": "2018-01-01",
    # 初始资金和手续费率
    "cash": 100000.0,
    "commission": 0.0001,
    # 买入时使用现金的比例，给跳空和手续费留缓冲
    "buy_cash_ratio": 0.95,
    # 按更高的估算成交价计算仓位，避免次日高开导致资金不足
    "buy_price_buffer": 1.01,
    # 每次买入按多少股的整数倍下单，A 股通常为 100
    "lot_size": 100,
    # 买入触发阈值：价格 <= X * buy_trigger_multiplier
    "buy_trigger_multiplier": 1.05,
    # 价格触发后，最多等待多少个交易日寻找买点
    "buy_trigger_window": 10,
    # 连续观察窗口长度，例如 5 表示统计最近 5 个交易日
    "buy_rise_window": 5,
    # 连续观察窗口内至少多少个上涨日才买入
    "buy_rise_days_required": 4,
    # 卖出阈值的加权值
    "sell_trigger_multiplier": 0.9,
    # 相对买入价的止损跌幅，例如 0.1 表示跌 10% 止损
    "stop_loss_pct": 0.1,
    "print_log": True,
    # 单次回测使用的均线周期
    "fast": 8,  # 这个试过5，感觉还是很容易反复切割年线
    "slow": 250,
    # 是否绘图、是否打印交易日志
    "plot": True,
    # 基准，绘图时把这个作为基准。改为空字符串则不显示基准
    "benchmark_code": "sh.000001",
    # 报告的配置
    "report_dir": "logs/backtest",
    "report_name": "simple_ma_backtest",
    "strategy_name": "普通双均线",
    "strategy_brief": "基础版",
}

# 如需找到最合适的均线周期，启用这里的参数
CONFIG_OPTIMIZE = {
    # False 表示单次回测，True 表示参数优化
    "optimize": True,
    # 参数范围，格式 start:end:step，将遍历这里面的每对参数组合，打印结果排序
    "opt_fast": "5:20:5",
    "opt_slow": "20:60:10",
    # 参数优化结果展示前几名
    "top": 10,
    # 参数优化模式下不支持视图
    "plot": False,
}
# CONFIG.update(CONFIG_OPTIMIZE)

# 测试用例，记录着对每只股票的指标最低要求
TEST_CASES = [
    {
        # TCL科技
        "code": "sz.000100",
        "expect": "",
    },
    {
        # 京东方A
        "code": "sz.000725",
        "expect": "",
    },
    {
        # 康冠科技，跟上面两个一样是光学光电子的，走向很像
        "code": "sz.001308",
        "expect": "",
    },
    {
        # 比亚迪
        "code": "sz.002594",
        "expect": "",
    },
    {
        # 卧龙电驱
        "code": "sh.600580",
        "expect": "",
    },
]


class State:
    """
    todo
    这个策略会进入一些持续好几天的状态，用这个类来记录当前状态，方便梳理逻辑。
    感觉这个不着急，先搞定筹码吧。
    """

    # 满足初步购买条件
    buy_trigger_active = False


class SimpleMovingAverageStrategy(HStrategy):
    """
    主要交易策略
    """

    params = (
        ("p", None),
        ("df", None),
        ("fast_period", 10),
        ("slow_period", 30),
        ("printlog", True),
    )

    def __init__(self) -> None:
        super().__init__(allow_log=self.params.printlog)

        self.order = None
        self.buy_markers: list[tuple[pd.Timestamp, float]] = []
        self.sell_markers: list[tuple[pd.Timestamp, float]] = []
        self.fast_ma = bt.indicators.SimpleMovingAverage(
            self.data.close,
            period=self.params.fast_period,
        )
        self.slow_ma = bt.indicators.SimpleMovingAverage(
            self.data.close,
            period=self.params.slow_period,
        )
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

        self.state = State()

        self.param = self.p.p or {}
        self.chip = ChipDistribution(self.p.df, period=120, bins=70)
        # 回测开始的日期
        from_date = self.param.get("from_date")
        self.from_date = pd.Timestamp(from_date).to_pydatetime() if from_date else None

        self.buy_trigger_active = False
        self.buy_trigger_days_seen = 0
        self.buy_trigger_up_days: list[int] = []
        self.last_buy_price: float | None = None
        self.last_trade_bar: int | None = None

        self.sell_trigger_active = False
        self.sell_trigger_days_seen = 0

        # 记录每次在水上和水下的天数，短线在长线上面就是水上
        self.now_is_up_day = None
        # todo 准备删了
        self.days_above_water_history: list[int] = [0]
        self.days_under_water_history: list[int] = [0]

        # 根据进出水面区域分割点，记录每个区域的最高价最低价，持续天数。
        self.water_surface_areas: list[dict] = []

        # 计算资金占用和空闲时长
        self.position_days_total = 0
        self.idle_cash_days_total = 0
        self.has_completed_sell = False

    def calculate_buy_size(self) -> int:
        """计算本次买入的数量，考虑可用资金、手续费、跳空风险等因素"""
        available_cash = self.broker.getcash()
        commission = self.broker.getcommissioninfo(self.data).p.commission
        estimated_price = max(
            float(self.data.open[0]),
            float(self.data.close[0]),
            float(self.data.high[0]),
        ) * float(self.param.get("buy_price_buffer"))
        max_cost_per_share = estimated_price * (1 + commission)

        raw_size = int(
            (available_cash * self.param.get("buy_cash_ratio")) / max_cost_per_share
        )
        lot_size = max(int(self.param.get("lot_size")), 1)
        if lot_size > 1:
            raw_size = (raw_size // lot_size) * lot_size
        return max(raw_size, 0)

    def notify_order(self, order: bt.Order) -> None:
        # 接收并处理订单（Order）状态变化的通知。
        # 当你在策略中通过 self.buy()、self.sell() 或 self.close() 发出交易指令后，
        # 这些指令并不会立即成交，而是会生成一个订单对象，并提交给模拟券商（Broker）。
        # notify_order 方法就是用来跟踪这个订单从创建到最终成交、取消或被拒绝的整个生命周期。

        # 订单状态为 Submitted（已提交）或 Accepted（已接受）时，表示订单正在等待执行，
        # 此时不需要做任何处理，直接返回即可。
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status == order.Completed:
            executed_at = pd.Timestamp(self.datas[0].datetime.date(0))
            trade_interval_msg = ""
            current_bar = len(self)
            if self.last_trade_bar is not None:
                trade_interval_days = current_bar - self.last_trade_bar
                trade_interval_msg = f" 距离上次买卖间隔={trade_interval_days}个交易日"

            if order.isbuy():
                self.last_buy_price = float(order.executed.price)
                self.buy_markers.append((executed_at, float(order.executed.price)))
                self.log(
                    f"成交！ 买入价格={order.executed.price:.2f} "
                    f"数量={order.executed.size:.0f} "
                    f"手续费={order.executed.comm:.2f}"
                    f"{trade_interval_msg}"
                )
            else:
                self.last_buy_price = None
                self.has_completed_sell = True
                self.sell_markers.append((executed_at, float(order.executed.price)))
                self.log(
                    f"成交！ 卖出价格={order.executed.price:.2f} "
                    f"数量={abs(order.executed.size):.0f} "
                    f"手续费={order.executed.comm:.2f}"
                    f"{trade_interval_msg}"
                )
            self.last_trade_bar = current_bar
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_map = {
                order.Canceled: "已取消",
                order.Margin: "资金不足",
                order.Rejected: "已拒绝",
            }
            self.log(
                f"订单失败 状态={status_map.get(order.status, order.getstatusname())}"
            )
        # 重置为 None，以便策略可以发出新的交易信号
        self.order = None

    def notify_trade(self, trade: bt.Trade) -> None:
        if not trade.isclosed:
            return
        self.log(
            f"-交易结束- 毛收益={trade.pnl:.2f}"
            f" 净收益={trade.pnlcomm:.2f}"
            f" 持续时间={trade.barlen} 天）"
            f" 利润率={(trade.pnl / trade.price) * 100:.2f}%"
            f" 现金余额={self.broker.getcash():.2f}"
        )

    def reset_buy_setup(self) -> None:
        self.buy_trigger_active = False
        self.buy_trigger_days_seen = 0

    def reset_sell_setup(self) -> None:
        self.sell_trigger_active = False
        self.sell_trigger_days_seen = 0

    def record_water_up_down_days(self) -> str:
        """
        每天执行这个方法，
        1. 会记录每个区间（水上和水下区间）的天数
        2. 会记录每个区间的最高价和最低价
        这个方法要在 self.is_above_water 之前执行，否则 self.is_above_water 方法算得不准
        """
        current_fast_ma = float(self.fast_ma[0])
        current_slow_ma = float(self.slow_ma[0])
        # current_high = float(self.data.high[0])
        # current_low = float(self.data.low[0])
        is_up = current_fast_ma > current_slow_ma

        if self.now_is_up_day is None or is_up != self.now_is_up_day:
            # 每次切换水上水下时进入该逻辑
            self.now_is_up_day = is_up
            
            if len(self.water_surface_areas) > 1:
                last = self.water_surface_areas[-1]
                last['day_num_weight'] = last["day_num"]
            
            # 计算加权值is_above_weight： 如果上一次持续时间小于上上次的10%，则把上一次的 is_above_weight 改为跟上上次一样
            if len(self.water_surface_areas) > 2:
                last = self.water_surface_areas[-1]
                l_last = self.water_surface_areas[-2]
                if last['day_num'] < l_last['day_num_weight'] * 0.1:
                    last['is_above_weight'] = l_last['is_above_weight']
                if last['is_above_weight'] == l_last['is_above_weight']:
                    # 这个分支不能跟上面合并，因为有可能这次时间长，同时又跟上面是同一个方向的，此时不会进上面的分支。
                    last['day_num_weight'] = l_last['day_num_weight'] + last["day_num"]
                # self.log(f'water_surface_areas={json.dumps(self.water_surface_areas)}')
            self.water_surface_areas.append(
                {
                    "is_above": is_up,
                    "is_above_weight": is_up, # 加权计算后，这个应该在上面还是下面
                    "day_num": 0,
                    "day_num_weight": 0, # 加权计算后，本区间应与上一个合并，则这个值应是合并后的总天数，否则等于本区间的天数
                    "highest_price": current_fast_ma,
                    "lowest_price": current_fast_ma,
                    "trust_price": True,  # 是否信任这个最高最低价，如果根据这个价格买入或卖出导致触发逃生，就把它设为不可信
                }
            )
                
        area = self.water_surface_areas[-1]
        area["day_num"] += 1
        area["day_num_weight"] += 1
        if is_up:
            area["highest_price"] = max(area["highest_price"], current_fast_ma)
        else:
            area["lowest_price"] = min(area["lowest_price"], current_fast_ma)

    def get_power(self, is_above=None) -> float:
        """
        返回维持在当前状态（在水上或在水下）的力量百分比，最大1，最小负无穷。力量越大，越不容易改变状态，力量会随时间减弱。

        返回百分比，例如0.5表示还有50%的能量，为负数就表示即将反转。

        能量这个概念的根据：主力吸筹和获利都是需要时间的，吸够了才会拉升，卖完了才会跌。
        中途的涨跌，恐怕是因为升跌太快，散户害怕导致的，主力会负责稳住。
        这个能量就是指主力维持在水上或水下的意愿有多强烈

        这个函数目前没用到，后面如果要优化 is_above_water 的算法，可以考虑用这个函数。
        现在 is_above_water 的算法比较简单，就是直接把天数当力量值了。

        todo 现在是无脑加上两次的水上和水下时长，其实应该按照筹码峰来分的，不过现在还没有这个数据。
        todo 可以考虑加上成交量，把每天的成交量加上。
        """
        # 现在是无脑加上N次的水上和水下时长，这个次数其实不那么准的。
        need_num = 2  # 水上水下分别取多少个区间，因为两个区间一定是间隔，所以肯定要取相同的个数
        above_power = 0
        below_power = 0
        
        areas = self._get_areas_by_weighted(need_num)
        for i, area in enumerate(areas):
            # 离现在越远的权重越低
            # 这是第几个，从1开始
            weight = 1
            if i >= 4:
                # 第5、6个权重
                weight = 0.2
            elif i >= 2:
                # 第3、4个权重
                weight = 0.6
                
            _is_above = area["is_above_weight"]
            if _is_above:
                above_power += area["day_num_weight"] * weight
            else:
                below_power += area["day_num_weight"] * weight

        # self.log(
        #     f"计算力量值：水上时长={above_power}天，"
        #     f"水下时长={below_power}天"
        # )
        if self.water_surface_areas[-1]['is_above'] if is_above is None else is_above:
            if below_power <= 0:
                below_power = 1
            return (below_power - above_power) / below_power
        else:
            if above_power <= 0:
                above_power = 1
            return (above_power - below_power) / above_power
        
    def _get_areas_by_weighted(self, need_num: int):
        """
        根据权重获取区间，可以避免快线频繁切割慢线导致的零碎区间
        :param need_num: 水上水下分别取多少个区间，因为两个区间一定是间隔，所以肯定要取相同的个数
        :return: 返回的区域顺序是越靠近现在的在前面
        """
        l = len(self.water_surface_areas)
        areas = []
        prev_is_above = None
        # 最后一个是当前区域，肯定要纳入计算的，所以是从l-1开始遍历
        for i in range(l - 1, -1, -1):
            area = self.water_surface_areas[i]

            _is_above = area["is_above_weight"]
            if i < l - 1:
                if prev_is_above != _is_above:
                    prev_is_above = _is_above
                    areas.append({**area})
                else:
                    # 如果刚刚遍历那个跟现在这个是同方向的，认为它们是一个区间，合并数据。
                    areas[-1]['day_num'] += area['day_num']
                    # day_num_weight 不合并，是因为刚刚那个就是根据这个加权过的最准确的值，不需要再算
                    if area['trust_price']:
                        # 这个区域的价格如果不可信，就不算它了
                        areas[-1]['highest_price'] = max(area['highest_price'], areas[-1]['highest_price'])
                        areas[-1]['lowest_price'] = min(area['lowest_price'], areas[-1]['lowest_price'])
            else:
                prev_is_above = _is_above
                areas.append({**area})
                
            if len(areas) >= need_num * 2:
                break
        return areas
        

    def get_power_by_chip(self, is_above=None):
        """
        根据筹码计算维持当前状态（水上还是水下）的能量，这个返回范围是0-1
        横盘震荡时，下面筹码占比60%就会开始拉升，上面筹码占80%时就会下降。
        这个不太行，感觉这个在大行情的时候，判断不了买卖时机。只能横盘震荡有用，以后如果有办法判断是大行情还是横盘震荡再说
        """
        dt = self.datas[0].datetime.date(0)
        price_grid, chips = self.chip.compute(dt)
        # 在年线上的筹码占比多少
        up_chips = 0
        down_chips = 0
        current_slow_ma = float(self.slow_ma[0])
        for i in range(len(price_grid)):
            price = price_grid[i]
            chip_val = chips[i]
            if price > current_slow_ma:
                up_chips += chip_val
            else:
                down_chips += chip_val

        if self.now_is_up_day if is_above is None else is_above:
            # 如果价格在年线上面，上面筹码越多，越容易下降
            return down_chips
        else:
            return up_chips

    def get_now_minmax_price(self, is_highest=True):
        """
        查询当前区域的最高价或最低价
        """
        num = len(self.water_surface_areas)
        for i in range(len(self.water_surface_areas)):
            area = self.water_surface_areas[num - 1 - i]
            if area["is_above"] != is_highest:
                continue
            return area["highest_price"] if is_highest else area["lowest_price"]
        return None

    def get_recent_area_days(
        self,
        is_above: bool,
        weighted: bool = False,
        include_current: bool = False,
    ) -> int | None:
        """
        获取最近一个指定方向区间的持续天数。

        Args:
            is_above: True 表示水上区间，False 表示水下区间
            weighted: 是否按加权后的区间方向和时长获取
            include_current: 是否允许返回当前正在进行中的区间
        """
        if not self.water_surface_areas:
            return None

        index_key = "is_above_weight" if weighted else "is_above"
        days_key = "day_num_weight" if weighted else "day_num"
        start_index = len(self.water_surface_areas) - 1 if include_current else len(self.water_surface_areas) - 2
        for i in range(start_index, -1, -1):
            area = self.water_surface_areas[i]
            if area[index_key] != is_above:
                continue
            return area[days_key]
        return None
    
    def get_last_lowest_price_weighted(self):
        """
        根据权重过滤后找到上一次在水下的区间（可能含假水上组成），返回它们的最低值
        ps 最高价不能按这个算法来，因为根据经验，长周期大涨大跌后，只有最低价是可靠的，最高价都是虚高
        """
        if len(self.water_surface_areas) < 2:
            return None
        prices = []
        for i in range(len(self.water_surface_areas) - 2, -1, -1):
            # 最后一个是当前价，不用遍历，所以是-2
            area = self.water_surface_areas[i]
            if prices and area["is_above_weight"]:
                # prices 有值就代表找到一些了，只要遇到一个不是水下的，就认为没有了。
                break
            if area["is_above_weight"] or not area["trust_price"]:
                continue
            prices.append(area['lowest_price'])
        if not prices:
            return None
        return min(prices)
        

    def get_last_minmax_price(self, is_highest=True, weighted=False):
        # 权重计算方法：找到最近N个价，每个价占比由它们所在区域持续的天数为准
        # todo 但这有个问题，大趋势后的价格应该是按照小趋势走，因为大趋势持续很久，所以这个方法会把大趋势的权重拉高，估计还要加入一些权重
        if len(self.water_surface_areas) < 2:
            return None
        # 取最后N次最高价
        total_days = 0
        need_num = 3
        target_areas = []
        for i in range(len(self.water_surface_areas) - 2, -1, -1):
            # 最后一个是当前价，不用遍历，所以是-2
            area = self.water_surface_areas[i]
            if area["is_above"] != is_highest or not area["trust_price"]:
                continue
            if not weighted:
                # 不需要带权重的，直接返回最新值即可。
                return area["highest_price"] if is_highest else area["lowest_price"]
            target_areas.append(area)
            total_days += area["day_num"]
            if len(target_areas) >= need_num:
                break
        value = 0
        for area in target_areas:
            v = area["highest_price"] if is_highest else area["lowest_price"]
            value += v * area["day_num"] / total_days
        return value

    def clear_last_lowest_price(self) -> None:
        # 触发止损时，表示上次买入时参照的最低价已经失真，要清掉，防止后续买入时继续参照这个失真的最低价
        # todo 现在最高价最低价搞了权重，似乎没办法不信，后面看看吧
        for i in range(len(self.water_surface_areas) - 2, -1, -1):
            # 最后一个是当前价，不用遍历，所以是-2
            area = self.water_surface_areas[i]
            if area["is_above"]:
                continue
            area["trust_price"] = False

    def is_above_water(self) -> bool:
        """
        加权计算今天是否算是在水上。
        现在的加权算法还行吧，虽然没办法抹平长时间的大起大落，但起码短时间频繁起落能抹平。后面再看看要不要优化。
        ps. self.now_is_up_day 是客观上在水上，但是容易抖动。该函数会避免这种抖动。
        只能在买卖决策中使用。
        """
        power = self.get_power()
        if power > 0:
            return self.now_is_up_day
        else:
            return not self.now_is_up_day

    def next(self) -> None:
        """
        回测或实盘交易的每一个时间步（例如每一根K线结束时）被自动调用
        notify_order 方法会在每个数据周期（bar）的 next() 方法被调用之前执行。
        这意味着你可以在 next() 中根据最新的订单状态来做出新的交易决策
        """

        if self.from_date and self.datas[0].datetime.date(0) < self.from_date.date():
            # self.log(f"预热中，当前日期 {self.datas[0].datetime.date(0)}，正式回测从 {self.from_date} 开始")
            return
        # 这个方法必须每天都执行的。放在预热完成之前执行也行，就是嫌它数据会很多。
        self.record_water_up_down_days()

        # 判断今天是否上涨（今收>昨收），并更新连续上涨日的记录。上涨日的数量不需要清空，一直记录最近5天的就好，
        # 因为有可能今天决定买，当天就发现过去几天都是涨的
        is_up_day = len(self.data) > 1 and float(self.data.close[0]) > float(
            self.data.close[-1]
        )
        rise_window = int(self.param.get("buy_rise_window"))
        self.buy_trigger_up_days.append(1 if is_up_day else 0)
        if len(self.buy_trigger_up_days) > rise_window:
            self.buy_trigger_up_days = self.buy_trigger_up_days[-rise_window:]

        # 只有在金叉死叉发生的当天 self.crossover < 0, self.crossover > 0 才会有一个为真，否则都为假
        if self.crossover < 0:
            highest_price_prev = self.get_last_minmax_price(weighted=True)
            if highest_price_prev is not None:
                self.log(
                    f"切到水下，上次最高价为 {highest_price_prev:.2f}，"
                    f"能量值={self.get_power():.2f}"
                )

            # 遇到死叉了，看看真假，真的就取消买入了。
            # 又感觉这种死叉不会太真，要不还是先注释吧。
            # if self.buy_trigger_active and not self.is_above_water():
            #     self.reset_buy_setup()
            #     self.log(
            #         f"遇到真死叉，取消购买"
            #     )
        elif self.crossover > 0:
            # lowest_price_previous = self.get_last_minmax_price(False, weighted=True)
            lowest_price_previous = self.get_last_lowest_price_weighted()
            if lowest_price_previous is not None:
                previous_under_days = self.get_recent_area_days(
                    is_above=False,
                    weighted=False,
                )
                previous_under_days_weighted = self.get_recent_area_days(
                    is_above=False,
                    weighted=True,
                )
                water_msg = (
                    f"{previous_under_days_weighted} 天"
                    if previous_under_days_weighted is not None
                    else "未知"
                )
                self.log(
                    f"切到水上，上次最低价为 {lowest_price_previous:.2f}，"
                    f"买入阈值为 {lowest_price_previous * float(self.param.get('buy_trigger_multiplier')):.2f}"
                    f"在长线下方待了 {previous_under_days or 0} 天，加权后 {water_msg}，"
                    f"能量值={self.get_power():.2f}"
                )

        if self.position:
            self.position_days_total += 1
        elif self.has_completed_sell:
            self.idle_cash_days_total += 1

        if self.order is not None:
            return
        self._check_and_buy()
        self._check_and_sell()

    def _check_and_buy(self) -> None:
        if self.position:
            # 当前已持有，不买
            return False
        # if  self.now_is_up_day:
        #     # 金叉后不适合买入规则1，所以要重置规则1的所有状态
        #     self.buy_trigger_active = False
        #     self.buy_trigger_days_seen = 0
        #     self.log(
        #         f"金叉后不适合买入规则1，所以要重置规则1的所有状态"
        #     )

        highest_price_previous = self.get_last_minmax_price(weighted=True)
        # lowest_price_previous = self.get_last_minmax_price(False, weighted=True)
        lowest_price_previous = self.get_last_lowest_price_weighted()
        if lowest_price_previous is None:
            # 还没有上一个最低价，无法启动买入规则1
            return False

        current_close = float(self.data.close[0])

        # 买入逻辑：价格<=上一个X*1.1后10个交易日内，价格<Y*0.9且连续5个交易日有4个都是涨的就买入。
        buy_trigger_price = lowest_price_previous * float(
            self.param.get("buy_trigger_multiplier")
        )
        if not self.buy_trigger_active and current_close <= buy_trigger_price:
            # 价格低于触发价，启动买入观察窗口
            self.buy_trigger_active = True
            self.buy_trigger_days_seen = 0
            self.log(
                f"价格触发买入观察窗口 收盘价={current_close:.2f} "
                f"上次最低价={lowest_price_previous:.2f} 阈值={buy_trigger_price:.2f}"
            )

        if self.buy_trigger_active:
            self.buy_trigger_days_seen += 1

            # 购买价不能高于这个
            if highest_price_previous is None:
                self.log(f"不知道之前的最高价，心里没底，不买了")
                return

            # 最近的最低价
            lowest_price = self.get_now_minmax_price(False) or lowest_price_previous

            # 计算买入价封顶价格时，采用区间百分比的方法。
            buy_limit = (highest_price_previous - lowest_price) * float(
                self.param.get("sell_trigger_multiplier")
            ) + lowest_price
            if current_close >= buy_limit:
                self.log(f"价格接近上周期最高价 {highest_price_previous:.2f}，不买了")
                return

            rise_window = int(self.param.get("buy_rise_window"))
            rise_days_required = int(self.param.get("buy_rise_days_required"))
            # 下面这个过于谨慎
            # rise_days_required = rise_window
            power = self.get_power()
            if self.now_is_up_day:
                # 在水上想买，要慎重，力量强才敢买
                if power > 0.5:
                    rise_days_required -= 1
                elif power < 0.5:
                    # +1 是很难买了
                    rise_days_required += 1
            else:
                if power < 0:
                    rise_days_required -= 2
                elif power < 0.5:
                    rise_days_required -= 1
                elif power > 0.8:
                    rise_days_required += 1
            self.log(f'power={power}')

            if len(self.buy_trigger_up_days) == rise_window:
                # B计划允许少一天上涨，但总的上涨在靠谱的范围内，也可以买入
                rise_days_num = sum(self.buy_trigger_up_days)
                price_change = (
                    (current_close - lowest_price) / lowest_price if lowest_price else 0
                )
                plan_b = (
                    rise_days_num > (rise_days_required - 1) and price_change >= 0.03
                )
                if rise_days_num >= rise_days_required or plan_b:
                    # 上涨天数够，或者差1天，但上涨金额够
                    size = self.calculate_buy_size()
                    if size <= 0:
                        self.log(
                            f"买点已满足，但可用资金不足 当前现金={self.broker.getcash():.2f}"
                        )
                        self.reset_buy_setup()
                        return
                    self.log(
                        "买点满足"
                        + (
                            f"(差1天，但上涨金额够)"
                            if plan_b
                            else f"({rise_window}日{rise_days_required}涨power={power})，"
                        )
                        + f"下单买入 收盘价={current_close:.2f} "
                        f"数量={size} 当前现金={self.broker.getcash():.2f} "
                        f"上一个最低价={lowest_price_previous:.2f} "
                        f"买入价不高于 {highest_price_previous:.2f} * {float(self.param.get('sell_trigger_multiplier')):.1f} = {buy_limit:.2f}"
                    )
                    self.order = self.buy(size=size)
                    self.reset_buy_setup()
                    return

            # if self.now_is_up_day:
            #     # 遇到最低价，且有金叉，买入
            #     size = self.calculate_buy_size()
            #     if size <= 0:
            #         self.log(
            #             f"窗口内遇到金叉，但可用资金不足 当前现金={self.broker.getcash():.2f}"
            #         )
            #         self.reset_buy_setup()
            #         return
            #     self.log(
            #         f"窗口内遇到金叉，准备买入 收盘价={current_close:.2f} "
            #         f"数量={size} 当前现金={self.broker.getcash():.2f} "
            #         f"上一个最低价={lowest_price_previous:.2f}"
            #     )
            #     self.order = self.buy(size=size)
            #     self.reset_buy_setup()

            if self.buy_trigger_days_seen >= int(self.param.get("buy_trigger_window")):
                self.log(
                    f"买入观察窗口到期，未出现窗口内金叉，或未满足"
                    f"价格<Y*{float(self.param.get('sell_trigger_multiplier')):.1f}且"
                    f"{rise_window}日{rise_days_required}涨，放弃本次买入"
                )
                self.reset_buy_setup()

    def _check_and_sell(self) -> None:
        # 卖出逻辑
        if not self.position:
            # 当前未持有，不卖
            return False

        highest_price_previous = self.get_last_minmax_price(weighted=True)
        current_close = float(self.data.close[0])

        # 止损
        if self.last_buy_price is not None:
            # todo 如果 self.sell_trigger_active 为 true，应该更早止损
            stop_loss_price = self.last_buy_price * (
                1 - float(self.param.get("stop_loss_pct"))
            )
            if current_close <= stop_loss_price:
                self.log(
                    f"触发止损，准备卖出 收盘价={current_close:.2f} "
                    f"买入价={self.last_buy_price:.2f} 止损价={stop_loss_price:.2f} "
                    "并清除最低点，等待下次重新记录"
                )
                # 这里是跌超10%了，要清除最低价，防止再按照这个来买，看看有什么逻辑可以替代
                self.clear_last_lowest_price()
                self.order = self.close()
                return

        # 股价>=上次最高价与买入价的90%位置后卖出。
        if not self.sell_trigger_active and highest_price_previous is not None:
            earn_from_buy = (current_close - self.last_buy_price) / self.last_buy_price
            sell_trigger_price = (highest_price_previous - self.last_buy_price) * float(
                self.param.get("sell_trigger_multiplier")
            ) + self.last_buy_price
            # 判断是否需要谨慎
            current_above_days = self.get_recent_area_days(
                is_above=True,
                weighted=False,
                include_current=True,
            ) or 0
            previous_under_days = self.get_recent_area_days(
                is_above=False,
                weighted=False,
            ) or 0
            need_prudent = (
                self.now_is_up_day
                and previous_under_days < current_above_days * 2
            )
            if (
                current_close >= sell_trigger_price
                or need_prudent
                and earn_from_buy >= 0.1
            ):
                # 进入待售状态
                reason = (
                    f"股价接近上次高点（{highest_price_previous:.2f},加权后={sell_trigger_price:.2f}）"
                    if current_close >= sell_trigger_price
                    else "金叉且盈利超过10%"
                )
                self._active_sell_trigger(reason)

        # 涨10%就可以考虑是不是卖出了
        if current_close > self.last_buy_price * 1.1:
            power = self.get_power()
            if not self.water_surface_areas[-1]['is_above']:
                # 在水下，且能量仍不是负数时，且获利10%后卖出。
                # 这种大概率是长跌后，估计触底反弹时，才会出现在水下就能涨这么多的。
                # todo 这个估计涨5%就可以考虑卖出了，10%有点少见
                if power > 0:
                    reason = (
                        f"股价仍在水下，但获利已有{((current_close-self.last_buy_price)/self.last_buy_price*100):.2f}%，"
                        f"power={power}仍为正，认为还有买入时机，故先获利"
                    )
                    self._active_sell_trigger(reason)
            elif current_close > self.last_buy_price * 1.2:
                # 在水上获利超过20%，且power小于0.5
                if power < 0.5:
                    # self.log(
                    #     f"获利{((current_close-self.last_buy_price)/self.last_buy_price*100):.2f}% power={self.get_power():.2f} "
                    # )
                    reason = (
                        f"股价在水上，获利已有{((current_close-self.last_buy_price)/self.last_buy_price*100):.2f}%，"
                        f"power={power}仍小于0.5，认为已经蛮高了，先获利，稳一波"
                    )
                    self._active_sell_trigger(reason)
                
        

        if self.sell_trigger_active:
            self.sell_trigger_days_seen += 1
            fall_days = 3
            up_days = sum(self.buy_trigger_up_days[-fall_days:])  # 最近3天的上涨日数量
            highest_price = self.get_now_minmax_price() or highest_price_previous
            fall_from_hight = (current_close - highest_price) / highest_price
            sell_msg = ""
            fall_from_hight_weig = 0.02
            if up_days > 0:
                if fall_from_hight > -fall_from_hight_weig:
                    if self.sell_trigger_days_seen < 10:
                        self.log(
                            f"最近{min(fall_days, len(self.buy_trigger_up_days))}天有{up_days}天上涨，"
                            f"离最高的跌幅不到{fall_from_hight_weig*100}%，可能只是震荡，继续持有观察"
                        )
                        return
                    else:
                        sell_msg = "已经等了10天"
                else:
                    sell_msg = f"离最高的跌幅超过{fall_from_hight_weig}%"
            else:
                sell_msg = f"连续{min(fall_days, len(self.buy_trigger_up_days))}天下跌"

            self.log(
                f"{sell_msg}，下单卖出 收盘价={current_close:.2f} "
                f"数量={abs(self.position.size)}"
            )
            self.order = self.close()
            self.reset_sell_setup()
            return True

        # 遇到死叉卖出，但是想了想，前面有个价格回升到最高价附近的卖出条件了，
        # 又回到死叉卖出，很可能只是震荡，不如等价格真正跌下来再卖出，所以先注释掉死叉卖出
        # if not self.now_is_up_day:
        #     self.log(
        #         f"均线死叉，全部卖出 收盘价={current_close:.2f} "
        #         f"数量={abs(self.position.size)}"
        #     )
        #     self.order = self.close()

    def _active_sell_trigger(self, reason):
            # 进入待售状态
            self.sell_trigger_active = True
            self.sell_trigger_days_seen = 0
            self.log(f"{reason}，考虑卖出，等待下跌信号 ")
        

    def stop(self) -> None:
        self.log(
            f"回测结束 快线={self.params.fast_period} 慢线={self.params.slow_period} "
            f"期末资产={self.broker.getvalue():.2f}"
        )


def build_data_feed(
    df: pd.DataFrame,
    from_date: str | None = None,
    to_date: str | None = None,
) -> bt.feeds.PandasData:
    data_kwargs = {
        "dataname": df,
        "datetime": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }
    if from_date:
        data_kwargs["fromdate"] = pd.Timestamp(from_date).to_pydatetime()
    if to_date:
        data_kwargs["todate"] = pd.Timestamp(to_date).to_pydatetime()
    return bt.feeds.PandasData(**data_kwargs)


def add_analyzers(cerebro: bt.Cerebro) -> None:
    # 添加收益率分析器，_name 参数指定分析器的名称，方便后续获取分析结果
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")
    # 添加最大回撤分析器
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    # 添加夏普比率分析器，设置时间框架为天，annualize=True 表示年化夏普比率
    cerebro.addanalyzer(
        bt.analyzers.SharpeRatio,
        _name="sharpe",
        timeframe=bt.TimeFrame.Days,
        annualize=True,
    )
    # 添加交易分析器，提供交易次数、胜率、盈亏分布等指标
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="time_return")


def print_summary(summary: dict[str, Any]) -> None:
    print("回测结果:")
    print(f"  快线周期: {summary['fast_period']}")
    print(f"  慢线周期: {summary['slow_period']}")
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


def generate_html_report(
    report_data: list,
    config: dict[str, Any],
    log_lines: list[str] | None = None,
) -> None:
    if not report_data:
        print("没有可用的回测数据来生成报告")
        return
    report_dir = ensure_dir(PROJECT_ROOT / config["report_dir"])
    html_report_path = report_dir / f"{config['report_name']}-{config['code']}.html"
    title = f"{config.get('code')} {config.get('strategy_name', '我的策略')} 回测报告"
    generate_backtest_html(
        report_data,
        str(html_report_path),
        [],
        title,
        log_lines=log_lines,
    )
    print(f"HTML 回测报告: {html_report_path}")


def create_cerebro(config: dict[str, Any]) -> bt.Cerebro:
    """
    Cerebro 作为中央控制系统，其主要职责包括：
    整合组件：收集并管理所有输入，如数据源（Data Feeds）、交易策略（Strategies）、分析器（Analyzers）和观察者（Observers）等。
    执行回测：启动并驱动整个回测或实盘交易流程。
    返回结果：在回测结束后，提供包含策略表现、交易记录等信息的执行结果。
    生成图表：为结果的可视化提供支持，可以方便地绘制资金曲线、交易信号等。
    """
    # optreturn=True（默认）
    # 👉 返回的是“轻量结果”（不是完整 strategy）
    # optreturn=False
    # 👉 返回完整 strategy（推荐你用这个）
    cerebro = bt.Cerebro(optreturn=False)
    cerebro.broker.setcash(config["cash"])
    cerebro.broker.setcommission(commission=config["commission"])
    return cerebro


def parse_range(expr: str, label: str) -> range:
    parts = [part.strip() for part in expr.split(":")]
    if len(parts) not in (2, 3):
        raise ValueError(f"{label} 格式错误，应为 start:end 或 start:end:step")

    start = int(parts[0])
    end = int(parts[1])
    step = int(parts[2]) if len(parts) == 3 else 1

    if start <= 0 or end <= 0 or step <= 0:
        raise ValueError(f"{label} 的 start、end、step 都必须大于 0")
    if start > end:
        raise ValueError(f"{label} 的 start 不能大于 end")

    return range(start, end + 1, step)


def validate_config(config: dict[str, Any]) -> None:
    if config["cash"] <= 0:
        raise ValueError("初始资金 cash 必须大于 0")
    if config["commission"] < 0:
        raise ValueError("手续费 commission 不能小于 0")
    if config["buy_cash_ratio"] <= 0 or config["buy_cash_ratio"] > 1:
        raise ValueError("buy_cash_ratio 必须大于 0 且小于等于 1")
    if config["buy_price_buffer"] < 1:
        raise ValueError("buy_price_buffer 必须大于等于 1")
    if int(config["lot_size"]) <= 0:
        raise ValueError("lot_size 必须大于 0")
    if float(config["buy_trigger_multiplier"]) <= 0:
        raise ValueError("buy_trigger_multiplier 必须大于 0")
    if int(config["buy_trigger_window"]) <= 0:
        raise ValueError("buy_trigger_window 必须大于 0")
    if int(config["buy_rise_window"]) <= 0:
        raise ValueError("buy_rise_window 必须大于 0")
    if int(config["buy_rise_days_required"]) <= 0:
        raise ValueError("buy_rise_days_required 必须大于 0")
    if int(config["buy_rise_days_required"]) > int(config["buy_rise_window"]):
        raise ValueError("buy_rise_days_required 不能大于 buy_rise_window")
    if float(config["sell_trigger_multiplier"]) <= 0:
        raise ValueError("sell_trigger_multiplier 必须大于 0")
    if float(config["stop_loss_pct"]) < 0 or float(config["stop_loss_pct"]) >= 1:
        raise ValueError("stop_loss_pct 必须大于等于 0 且小于 1")

    if config.get("optimize"):
        parse_range(config["opt_fast"], "opt_fast")
        parse_range(config["opt_slow"], "opt_slow")
        if config["plot"]:
            raise ValueError("参数优化模式下不支持 plot=True")
        if config["top"] <= 0:
            raise ValueError("top 必须大于 0")
        return

    if config["fast"] <= 0 or config["slow"] <= 0:
        raise ValueError("fast 和 slow 都必须大于 0")
    if config["fast"] >= config["slow"]:
        raise ValueError("fast 必须小于 slow")


def run_backtest(config: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    cerebro = create_cerebro(config)
    # 设置策略 (Strategy)：添加你定义的交易策略类（注意是类，不是实例）
    cerebro.addstrategy(
        SimpleMovingAverageStrategy,
        fast_period=config["fast"],
        slow_period=config["slow"],
        printlog=config["print_log"],
        p=config,
        df=df,
    )
    # 添加股票日线数据
    cerebro.adddata(build_data_feed(df, config["data_from_date"], config["to_date"]))
    # 添加分析器 (Analyzer)：添加你需要的分析器来评估策略表现
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

    # 绘图
    if config["plot"]:
        report_data = build_backtest_report_data(
            strategy,
            config,
            [config["fast"], config["slow"]],
        )
        generate_html_report(report_data, config, getattr(strategy, "log_messages", []))

    return summary


def run_optimization(config: dict[str, Any], df: pd.DataFrame) -> None:
    """
    参数优化的核心思想是通过穷举法（Grid Search）系统地测试不同的参数组合，
    以找到在历史数据上表现最好的参数设置。
    目前只是找到最适合的两条均线组合
    """

    fast_range = parse_range(config["opt_fast"], "opt_fast")
    slow_range = parse_range(config["opt_slow"], "opt_slow")
    combinations = [
        (fast_period, slow_period)
        for fast_period in fast_range
        for slow_period in slow_range
        if fast_period < slow_period
    ]
    if not combinations:
        raise ValueError("没有可用的均线参数组合，请检查 opt_fast 和 opt_slow")

    cerebro = create_cerebro(config)
    cerebro.optstrategy(
        SimpleMovingAverageStrategy,
        fast_period=sorted({item[0] for item in combinations}),
        slow_period=sorted({item[1] for item in combinations}),
        printlog=False,
    )
    cerebro.adddata(build_data_feed(df, config["from_date"], config["to_date"]))
    add_analyzers(cerebro)

    print(f"开始参数优化: 股票={config['code']}，参数组合数={len(combinations)}")
    optimized_runs = cerebro.run(maxcpus=1)

    results: list[dict[str, Any]] = []
    for run_group in optimized_runs:
        strategy = run_group[0]
        if strategy.params.fast_period >= strategy.params.slow_period:
            continue
        summary = summarize_result(strategy, config["cash"])
        summary.update(
            {
                "fast_period": strategy.params.fast_period,
                "slow_period": strategy.params.slow_period,
            }
        )
        results.append(summary)

    if not results:
        raise ValueError("参数优化没有产出任何有效结果")

    top_results = sorted(
        results,
        key=lambda item: (
            (
                item["annual_return_pct"]
                if item["annual_return_pct"] is not None
                else float("-inf")
            ),
            item["sharpe_ratio"] if item["sharpe_ratio"] is not None else float("-inf"),
        ),
        reverse=True,
    )[: config["top"]]

    print("参数优化结果(按总收益率排序):")
    for index, item in enumerate(top_results, start=1):
        annual_text = (
            f"{item['annual_return_pct']:.2f}%"
            if item["annual_return_pct"] is not None
            else "N/A"
        )
        sharpe_text = (
            f"{item['sharpe_ratio']:.2f}" if item["sharpe_ratio"] is not None else "N/A"
        )
        max_drawdown_text = (
            f"{item['max_drawdown_pct']:.2f}%"
            if item["max_drawdown_pct"] is not None
            else "N/A"
        )
        print(
            f"{index}. 快线={item['fast_period']}, 慢线={item['slow_period']}, "
            # 应该是 Backtrader 的optstrategy参数调优有bug，返回的期末资金所有都是最后一组参数的结果，
            # 以后改成我自己遍历所有参数组合的方式来跑，就不会有这个问题了
            # f"总收益率={item['total_return_pct']:.2f}%, "
            f"年化收益率={annual_text}, "
            f"最大回撤={max_drawdown_text}, 夏普比率={sharpe_text}, "
            f"胜率={item['win_rate_pct']:.2f}%, 交易次数={item['trades_total']}"
        )


def main(config) -> None:
    validate_config(config)
    df = load_daily_data(config["code"], config["adjust_flag"])

    if config.get("optimize"):
        # 执行参数优化
        run_optimization(config, df)
        return

    run_backtest(config, df)


if __name__ == "__main__":
    main(CONFIG)

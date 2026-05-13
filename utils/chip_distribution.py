import sys
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.project_utils import load_daily_data


class ChipDistribution:
    """
    筹码分布计算类

    基于换手率衰减的移动筹码分布算法（平均分布法）。

    使用方法:
        # 1. 准备数据，必须包含以下列：
        #    date: 日期
        #    high: 最高价
        #    low:  最低价
        #    turn: 换手率（小数，例如 0.05 代表 5%）
        chip = ChipDistribution(df, period=120, bins=70)

        # 2. 计算指定日期的筹码分布
        price_grid, chips = chip.compute('2024-01-15')

        # 3. 获取指标或打印图表
        profit_ratio = chip.get_profit_ratio(price_grid, chips, current_price)
        chip.print_ascii(price_grid, chips, current_price)
    """

    def __init__(self, df, period=120, bins=70):
        """
        初始化筹码分布计算器

        参数:
            df: pandas.DataFrame，必须包含列 date, high, low, turn
            period: 计算周期（天数），默认 120
            bins: 价格网格数量，默认 70
        """
        required_cols = {"date", "high", "low", "turn"}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"DataFrame 必须包含列: {required_cols}")

        self.df = df.sort_values("date").reset_index(drop=True).copy()
        self.period = period
        self.bins = bins

        # 缓存已计算的结果 {date: (price_grid, chips)}
        self._cache = {}

    def _get_data_until_date(self, target_date):
        """获取截至 target_date 的历史数据切片（最近 period 个交易日）"""
        # 转换为 datetime 类型以便比较
        self.df["date"] = pd.to_datetime(self.df["date"])
        target_date = pd.to_datetime(target_date)

        # 找到目标日期在数据中的位置
        idx = self.df[self.df["date"] <= target_date].index
        if len(idx) == 0:
            raise ValueError(f"日期 {target_date} 不在数据范围内")

        last_idx = idx[-1]
        start_idx = max(0, last_idx - self.period + 1)
        return self.df.iloc[start_idx : last_idx + 1].copy()

    def compute(self, date=None):
        """
        计算指定日期的筹码分布

        参数:
            date: str 或 datetime，目标日期。若为 None，则使用数据中最后一天

        返回:
            price_grid: np.ndarray 价格网格（从低到高）
            chips: np.ndarray 每个价格对应的筹码占比（归一化，总和为1）
        """
        if date is None:
            date = self.df["date"].iloc[-1]

        date_str = str(pd.to_datetime(date).date())
        if date_str in self._cache:
            return self._cache[date_str]

        # 获取计算窗口数据
        sub_df = self._get_data_until_date(date)
        if len(sub_df) == 0:
            raise ValueError("计算窗口内无有效数据")

        # 价格网格范围（基于窗口内最高/最低价）
        price_min = sub_df["low"].min()
        price_max = sub_df["high"].max()
        price_grid = np.linspace(price_min, price_max, self.bins)

        # 初始化筹码分布
        chips = np.zeros(self.bins)

        # 按时间顺序递推
        for _, row in sub_df.iterrows():
            high, low, turnover = row["high"], row["low"], row["turn"]

            # 构建当日新成交筹码分布（总量 = turnover）
            new_chips = np.zeros(self.bins)

            if high == low:
                # 一字板：全部堆在对应价格
                idx = np.abs(price_grid - high).argmin()
                new_chips[idx] = turnover
            else:
                mask = (price_grid >= low) & (price_grid <= high)
                n_bins = mask.sum()
                if n_bins > 0:
                    new_chips[mask] = turnover / n_bins
                else:
                    # 价格区间过窄，无网格覆盖，分配给最近网格
                    mid_price = (high + low) / 2
                    idx = np.abs(price_grid - mid_price).argmin()
                    new_chips[idx] = turnover

            # 核心递推公式：C = C_old * (1 - turnover) + new_chips
            chips = chips * (1.0 - turnover) + new_chips

        # 归一化
        total = chips.sum()
        if total > 0:
            chips = chips / total
        else:
            chips = np.ones(self.bins) / self.bins

        self._cache[date_str] = (price_grid, chips)
        return price_grid, chips

    def get_profit_ratio(self, price_grid, chips, current_price):
        """
        计算获利盘比例（持仓成本低于当前价的比例）

        参数:
            price_grid: 价格网格数组
            chips: 筹码密度数组
            current_price: 当前价格

        返回:
            float: 获利盘比例 (0~100)
        """
        profit_idx = np.searchsorted(price_grid, current_price)
        if profit_idx == 0:
            return 0.0
        return chips[:profit_idx].sum() * 100.0

    def get_avg_cost(self, price_grid, chips):
        """
        计算加权平均持仓成本
        """
        if chips.sum() == 0:
            return np.nan
        return np.sum(price_grid * chips) / chips.sum()

    def get_concentration(self, price_grid, chips, current_price, threshold=0.9):
        """
        计算筹码集中度

        参数:
            price_grid: 价格网格数组
            chips: 筹码密度数组
            current_price: 当前价格
            threshold: 筹码占比阈值（默认 0.9 即 90%）

        返回:
            concentration: 集中度（百分比，价格区间宽度 / 当前价 * 100）
            low_price: 区间下界
            high_price: 区间上界
            若计算失败则返回 (None, None, None)
        """
        cumsum = np.cumsum(chips)
        lower_idx = np.searchsorted(cumsum, (1 - threshold) / 2)
        upper_idx = np.searchsorted(cumsum, 1 - (1 - threshold) / 2)

        if lower_idx < len(price_grid) and upper_idx < len(price_grid):
            price_range = price_grid[upper_idx] - price_grid[lower_idx]
            concentration = (price_range / current_price) * 100
            return concentration, price_grid[lower_idx], price_grid[upper_idx]
        return None, None, None

    def print_ascii(self, price_grid, chips, current_price, width=50):
        """
        在终端打印 ASCII 筹码分布图

        参数:
            price_grid: 价格网格数组
            chips: 筹码密度数组
            current_price: 当前价格
            width: 柱状图最大宽度（字符数）
        """
        max_chip = chips.max()
        if max_chip == 0:
            print("筹码分布数据异常")
            return

        print("\n" + "=" * 80)
        print(f"筹码分布图 (价格从高到低)")
        print("-" * 80)

        for i in range(len(price_grid) - 1, -1, -1):
            price = price_grid[i]
            chip_val = chips[i]

            bar_len = int((chip_val / max_chip) * width)
            bar = "█" * bar_len

            marker = (
                " <-- 当前价"
                if abs(price - current_price) < (price_grid[1] - price_grid[0]) / 2
                else ""
            )
            percentage = chip_val * 100
            print(f"{price:7.2f} | {bar:<{width}} {percentage:5.2f}%{marker}")

        print("-" * 80)
        print(f"注：柱状图最大宽度对应筹码密度 {max_chip * 100:.2f}%")

    def summary(self, date=None):
        """
        输出指定日期的筹码分布摘要信息（包括 ASCII 图和关键指标）

        参数:
            date: 目标日期，默认为最后一天
        """
        price_grid, chips = self.compute(date)
        sub_df = self._get_data_until_date(date if date else self.df["date"].iloc[-1])
        current_price = (
            sub_df["close"].iloc[-1]
            if "close" in sub_df.columns
            else sub_df["high"].iloc[-1]
        )

        # 打印 ASCII 图
        self.print_ascii(price_grid, chips, current_price)

        # 计算指标
        profit_ratio = self.get_profit_ratio(price_grid, chips, current_price)
        avg_cost = self.get_avg_cost(price_grid, chips)
        conc_90, low_90, high_90 = self.get_concentration(
            price_grid, chips, current_price
        )

        print("\n【关键指标】")
        print(
            f"日期            : {pd.to_datetime(date).date() if date else self.df['date'].iloc[-1].date()}"
        )
        print(f"当前价格        : {current_price:.2f}")
        print(f"获利盘比例      : {profit_ratio:.2f}%")
        print(f"平均持仓成本    : {avg_cost:.2f}")
        if conc_90:
            print(
                f"90%筹码集中度   : {conc_90:.2f}%  (区间 [{low_90:.2f}, {high_90:.2f}])"
            )
            if conc_90 < 10:
                evaluation = "高度集中"
            elif conc_90 < 20:
                evaluation = "相对集中"
            else:
                evaluation = "较为分散"
            print(f"  -> 集中度评价 : {evaluation}")
        print("=" * 80)

    def clear_cache(self):
        """清空计算缓存"""
        self._cache.clear()


def fetch_stock_data(code):
    df = load_daily_data(code, "hfq")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df["turn"] = df["turn"] / 100  # 数据单位本来就是百分比，这里除去。
    return df.reset_index(drop=True)

if __name__ == '__main__':
    df = fetch_stock_data('sz.000100')
    # 2. 初始化筹码分布计算器
    chip = ChipDistribution(df, period=120, bins=70)

    # 3. 查看最新一天的筹码分布摘要
    # chip.summary()

    # 4. 查看历史某一天的筹码分布（例如 2024-01-15）
    # price_grid, chips = chip.compute("2024-01-15")
    # current_price = df[df["date"] == "2024-01-15"]["close"].values[0]
    # profit = chip.get_profit_ratio(price_grid, chips, current_price)
    # print(f"2024-01-15 获利盘比例: {profit:.2f}%")

    # # 5. 也可以直接获取筹码数组用于进一步分析
    chip.summary("2024-02-20")
    price_grid, chips = chip.compute("2024-02-20")
    print(price_grid)
    print(chips)
    # 例如判断筹码是否在某个价格区间密集
    dense_zone = price_grid[chips > chips.mean()]  # 筹码密度高于平均的价格区间
    print(dense_zone)

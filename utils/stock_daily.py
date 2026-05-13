from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime

import pandas as pd

from utils.date import normalize_date


class StockDaily:
    """股票日线数据指标计算器。"""

    REQUIRED_COLUMNS = {
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "pctChg",
        "peTTM",
        "psTTM",
        "pcfNcfTTM",
        "pbMRQ",
    }

    def __init__(self, daily_df: pd.DataFrame):
        if not isinstance(daily_df, pd.DataFrame):
            raise TypeError("daily_df must be a pandas DataFrame")

        missing_columns = self.REQUIRED_COLUMNS.difference(daily_df.columns)
        if missing_columns:
            raise ValueError(
                f"daily_df is missing required columns: {sorted(missing_columns)}"
            )

        self.df = daily_df.copy()
        self.df["date"] = pd.to_datetime(self.df["date"], errors="coerce")
        self.df["close"] = pd.to_numeric(self.df["close"], errors="coerce")
        self.df = self.df.sort_values("date").reset_index(drop=True)

    def calculate_ma(
        self,
        windows: int | Iterable[int],
        price_col: str = "close",
        min_periods: int | None = None,
    ) -> pd.DataFrame:
        """
        计算均线并写入 DataFrame。

        Args:
            windows: 均线周期，例如 5 或 [5, 10, 20]
            price_col: 参与计算的价格列，默认使用 close
            min_periods: rolling 的最小样本数，默认等于窗口长度

        Returns:
            pd.DataFrame: 增加了 ma{window} 列的日线数据
        """
        if price_col not in self.df.columns:
            raise ValueError(f"price_col '{price_col}' not found in daily_df")

        self.df[price_col] = pd.to_numeric(self.df[price_col], errors="coerce")

        normalized_windows = self._normalize_windows(windows)
        for window in normalized_windows:
            rolling_min_periods = min_periods if min_periods is not None else window
            self.df[f"ma{window}"] = (
                self.df[price_col]
                .rolling(window=window, min_periods=rolling_min_periods)
                .mean()
            )

        return self.df

    def get_ma_by_date(
        self,
        target_date: date | datetime | str,
        windows: int | Iterable[int],
        price_col: str = "close",
        min_periods: int | None = None,
    ) -> float | dict[int, float | None] | None:
        """
        返回指定日期对应的均线值。

        Args:
            target_date: 指定日期
            windows: 均线周期，例如 5 或 [5, 10, 20]
            price_col: 参与计算的价格列，默认使用 close
            min_periods: rolling 的最小样本数，默认等于窗口长度

        Returns:
            float | None: 传入单个周期时返回对应均线值
            dict[int, float | None]: 传入多个周期时返回 {周期: 均线值}
        """
        if price_col not in self.df.columns:
            raise ValueError(f"price_col '{price_col}' not found in daily_df")

        normalized_windows = self._normalize_windows(windows)
        normalized_target_date = pd.Timestamp(normalize_date(target_date))

        matched_rows = self.df[self.df["date"] == normalized_target_date]
        if matched_rows.empty:
            raise ValueError(
                f"date '{normalized_target_date.date()}' not found in daily_df"
            )

        row_index = matched_rows.index[-1]
        price_series = pd.to_numeric(self.df[price_col], errors="coerce")
        ma_values = {
            window: self._calculate_ma_value_at_index(
                price_series=price_series,
                row_index=row_index,
                window=window,
                min_periods=min_periods,
            )
            for window in normalized_windows
        }

        if isinstance(windows, int):
            return ma_values[normalized_windows[0]]

        return ma_values

    @staticmethod
    def _calculate_ma_value_at_index(
        price_series: pd.Series,
        row_index: int,
        window: int,
        min_periods: int | None = None,
    ) -> float | None:
        rolling_min_periods = min_periods if min_periods is not None else window
        start_index = max(0, row_index - window + 1)
        window_series = price_series.iloc[start_index : row_index + 1]

        if window_series.count() < rolling_min_periods:
            return None

        return float(window_series.mean())

    @staticmethod
    def _normalize_windows(windows: int | Iterable[int]) -> list[int]:
        if isinstance(windows, int):
            normalized = [windows]
        elif isinstance(windows, Iterable) and not isinstance(windows, (str, bytes)):
            normalized = [int(window) for window in windows]
        else:
            raise TypeError("windows must be an int or an iterable of int")

        if not normalized:
            raise ValueError("windows must not be empty")

        invalid_windows = [window for window in normalized if window <= 0]
        if invalid_windows:
            raise ValueError(f"windows must be positive integers: {invalid_windows}")

        return normalized

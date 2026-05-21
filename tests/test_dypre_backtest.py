import unittest

import backtrader as bt
import pandas as pd

from backtest.backtest_v1 import build_data_feed, create_cerebro


class HoldThroughCorporateActionStrategy(bt.Strategy):
    def __init__(self) -> None:
        self.did_submit = False

    def next(self) -> None:
        if not self.did_submit:
            self.buy(size=100)
            self.did_submit = True


class DypreBacktestTests(unittest.TestCase):
    def test_dypre_adjusts_position_size_on_factor_jump(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 1000,
                    "turn": 1.0,
                    "raw_open": 20.0,
                    "raw_high": 20.0,
                    "raw_low": 20.0,
                    "raw_close": 20.0,
                    "raw_preclose": 20.0,
                    "signal_factor": 0.5,
                    "position_adjust_ratio": 1.0,
                },
                {
                    "date": "2024-01-03",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 1000,
                    "turn": 1.0,
                    "raw_open": 20.0,
                    "raw_high": 20.0,
                    "raw_low": 20.0,
                    "raw_close": 20.0,
                    "raw_preclose": 20.0,
                    "signal_factor": 0.5,
                    "position_adjust_ratio": 1.0,
                },
                {
                    "date": "2024-01-04",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 1000,
                    "turn": 1.0,
                    "raw_open": 10.0,
                    "raw_high": 10.0,
                    "raw_low": 10.0,
                    "raw_close": 10.0,
                    "raw_preclose": 20.0,
                    "signal_factor": 1.0,
                    "position_adjust_ratio": 2.0,
                },
                {
                    "date": "2024-01-05",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 1000,
                    "turn": 1.0,
                    "raw_open": 10.0,
                    "raw_high": 10.0,
                    "raw_low": 10.0,
                    "raw_close": 10.0,
                    "raw_preclose": 10.0,
                    "signal_factor": 1.0,
                    "position_adjust_ratio": 1.0,
                },
            ]
        )
        df["date"] = pd.to_datetime(df["date"])

        config = {
            "cash": 10000.0,
            "commission": 0.0,
            "stamp_duty": 0.0,
            "transfer_fee": 0.0,
            "min_commission": 0.0,
        }
        cerebro = create_cerebro(config)
        cerebro.addstrategy(HoldThroughCorporateActionStrategy)
        cerebro.adddata(build_data_feed(df))

        strategies = cerebro.run()
        strategy = strategies[0]
        position = strategy.getposition(strategy.datas[0])

        self.assertEqual(round(position.size, 4), 200.0)
        self.assertEqual(round(position.price, 4), 10.0)
        self.assertEqual(round(strategy.broker.getvalue(), 4), 10000.0)


if __name__ == "__main__":
    unittest.main()

import sys
import types
import unittest


if "backtrader" not in sys.modules:
    sys.modules["backtrader"] = types.SimpleNamespace(
        indicators=types.SimpleNamespace(
            BollingerBands=object,
            MACD=object,
            StochasticFull=object,
        )
    )

from backtest.patches import boll, kdj, macd
from backtest.patches.bmk import allow_buy as bmk_allow_buy
from backtest.patches.loader import discover_available_patch_names


class DummyLine:
    def __init__(self, values):
        if isinstance(values, dict):
            self.values = {int(key): float(value) for key, value in values.items()}
            return
        history = [float(item) for item in values]
        self.values = {}
        for offset, value in enumerate(reversed(history)):
            self.values[-offset] = value

    def __getitem__(self, index):
        if index in self.values:
            return self.values[index]
        if index < 0:
            return self.values[min(self.values.keys())]
        return self.values[0]


class DummyData:
    def __init__(self, close_values, length=64):
        self.close = DummyLine(close_values)
        self.length = length

    def __len__(self):
        return self.length


class DummyPosition:
    def __init__(self, size=0.0):
        self.size = float(size)

    def __bool__(self):
        return abs(self.size) > 1e-9


class DummyStrategy:
    def __init__(self):
        self.param = {}
        self.data = DummyData(
            [8.0, 8.3, 8.6, 8.8, 9.1, 9.3, 9.7, 10.2, 10.6],
            length=80,
        )
        self.position = DummyPosition(1000)
        self.order = None
        self.logged = []
        self._signal_prices = {
            "close": {0: 10.6, -1: 10.1},
        }

    def _get_signal_price(self, field, ago=0):
        return self._signal_prices[field][ago]

    def log(self, text):
        self.logged.append(text)

    def close(self):
        return "SELL"


class IndicatorPatchTests(unittest.TestCase):
    def test_loader_ignores_private_helper_and_keeps_public_patches(self):
        available = discover_available_patch_names()

        self.assertIn("boll", available)
        self.assertIn("macd", available)
        self.assertIn("kdj", available)
        self.assertIn("bmk", available)
        self.assertNotIn("_indicator_patch_utils", available)

    def test_boll_buy_and_sell_signal(self):
        strategy = DummyStrategy()
        strategy.boll_top = DummyLine({0: 10.5, -1: 10.0})
        strategy.boll_mid = DummyLine({0: 9.7, -1: 9.6})
        strategy.boll_bot = DummyLine({0: 8.9, -1: 9.0})

        buy_decision = boll.evaluate_buy_signal(strategy)
        self.assertTrue(buy_decision["allow"])

        sell_strategy = DummyStrategy()
        sell_strategy.boll_top = DummyLine({0: 10.4, -1: 10.3})
        sell_strategy.boll_mid = DummyLine({0: 9.8, -1: 9.8})
        sell_strategy.boll_bot = DummyLine({0: 9.1, -1: 9.2})
        sell_strategy._signal_prices["close"][0] = 9.0
        sell_decision = boll.evaluate_sell_signal(sell_strategy)
        self.assertTrue(sell_decision["should_sell"])

    def test_macd_buy_and_sell_signal(self):
        strategy = DummyStrategy()
        strategy.macd_line = DummyLine([ -0.5, -0.3, -0.2, -0.1, 0.05, 0.12, 0.18 ])
        strategy.macd_signal = DummyLine([ -0.4, -0.32, -0.25, -0.15, -0.02, 0.03, 0.08 ])
        strategy.macd_hist = DummyLine([ -0.1, -0.02, 0.01, 0.05, 0.07, 0.09, 0.10 ])

        buy_decision = macd.evaluate_buy_signal(strategy)
        self.assertTrue(buy_decision["allow"])

        sell_strategy = DummyStrategy()
        sell_strategy.macd_line = DummyLine([0.30, 0.26, 0.18, 0.08, -0.01, -0.05, -0.09])
        sell_strategy.macd_signal = DummyLine([0.24, 0.22, 0.20, 0.12, 0.02, 0.00, -0.01])
        sell_strategy.macd_hist = DummyLine([0.06, 0.04, -0.02, -0.04, -0.06, -0.08, -0.10])
        sell_decision = macd.evaluate_sell_signal(sell_strategy)
        self.assertTrue(sell_decision["should_sell"])

    def test_kdj_buy_and_sell_signal(self):
        strategy = DummyStrategy()
        strategy.kdj_k = DummyLine([10, 12, 13, 15, 17, 19])
        strategy.kdj_d = DummyLine([12, 13, 14, 16, 18, 18.5])
        strategy.kdj_j = DummyLine([-10, -5, -2, 5, 12, 18])

        buy_decision = kdj.evaluate_buy_signal(strategy)
        self.assertTrue(buy_decision["allow"])

        sell_strategy = DummyStrategy()
        sell_strategy.kdj_k = DummyLine([88, 86, 84, 83, 81, 79])
        sell_strategy.kdj_d = DummyLine([82, 83, 84, 84, 83, 82])
        sell_strategy.kdj_j = DummyLine([100, 102, 104, 103, 101, 99])
        sell_strategy._signal_prices["close"] = {0: 10.3, -1: 10.6}
        sell_decision = kdj.evaluate_sell_signal(sell_strategy)
        self.assertTrue(sell_decision["should_sell"])

    def test_bmk_requires_all_three_bullish_signals(self):
        strategy = DummyStrategy()
        strategy.boll_top = DummyLine({0: 10.5, -1: 10.0})
        strategy.boll_mid = DummyLine({0: 9.7, -1: 9.6})
        strategy.boll_bot = DummyLine({0: 8.9, -1: 9.0})
        strategy.macd_line = DummyLine([-0.5, -0.2, 0.02, 0.11])
        strategy.macd_signal = DummyLine([-0.4, -0.22, -0.01, 0.05])
        strategy.macd_hist = DummyLine([-0.1, 0.02, 0.05, 0.06])
        strategy.kdj_k = DummyLine([10, 12, 15, 19])
        strategy.kdj_d = DummyLine([12, 13, 16, 18.5])
        strategy.kdj_j = DummyLine([-8, -4, 4, 18])

        decision = bmk_allow_buy(strategy, {})
        self.assertTrue(decision["allow"])

        strategy.kdj_k = DummyLine([45, 50, 52, 54])
        strategy.kdj_d = DummyLine([42, 45, 48, 50])
        strategy.kdj_j = DummyLine([51, 60, 60, 62])
        decision = bmk_allow_buy(strategy, {})
        self.assertFalse(decision["allow"])


if __name__ == "__main__":
    unittest.main()

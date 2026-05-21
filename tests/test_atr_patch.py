import sys
import types
import unittest


if "backtrader" not in sys.modules:
    sys.modules["backtrader"] = types.SimpleNamespace(
        indicators=types.SimpleNamespace(
            AverageTrueRange=object,
            Highest=object,
            Lowest=object,
        )
    )

from backtest.patches import atr
from backtest.patches.loader import discover_available_patch_names


class DummyLine:
    def __init__(self, values):
        if isinstance(values, (int, float)):
            values = {0: float(values)}
        self.values = {int(key): float(value) for key, value in values.items()}

    def __getitem__(self, index):
        return self.values.get(index, self.values.get(0, 0.0))


class DummyBroker:
    def __init__(self, value: float):
        self.value = float(value)

    def getvalue(self) -> float:
        return self.value


class DummyPosition:
    def __init__(self, size: float = 0.0, price: float = 0.0):
        self.size = float(size)
        self.price = float(price)

    def __bool__(self) -> bool:
        return abs(self.size) > 1e-9


class DummyData:
    def __init__(self, length: int):
        self.length = length

    def __len__(self) -> int:
        return self.length


class DummyStrategy:
    def __init__(self):
        self.param = {
            "lot_size": 100,
            "stop_loss_pct": 0.1,
            "atr_risk_pct": 0.02,
            "atr_breakout_period": 20,
            "atr_exit_period": 10,
            "atr_max_units": 3,
            "atr_add_unit_atr": 1.0,
            "atr_stop_atr_multiplier": 2.0,
        }
        self.broker = DummyBroker(100000.0)
        self.position = DummyPosition()
        self.data = DummyData(length=80)
        self.order = None
        self.last_buy_price = None
        self.log_messages = []
        setattr(
            self,
            atr.STATE_KEY,
            {
                "original_calculate_buy_size": lambda: 5000,
                "observed_position_size": 0.0,
                "unit_size": 1000,
                "units": 1,
                "entry_base_price": 10.0,
                "last_add_price": 10.0,
                "next_add_price": 10.5,
                "stop_price": 9.7,
            },
        )
        self._signal_prices = {
            "open": {0: 10.2, -1: 9.9},
            "high": {0: 10.8, -1: 10.0},
            "low": {0: 10.0, -1: 9.8},
            "close": {0: 10.6, -1: 10.1},
        }
        self._trade_prices = {
            "open": {0: 10.2},
            "high": {0: 10.8},
            "close": {0: 10.6},
        }
        self.atr_patch_atr = DummyLine(1.0)
        self.atr_patch_breakout_high = DummyLine({0: 10.5, -1: 10.0})
        self.atr_patch_exit_low = DummyLine({0: 9.7, -1: 9.8})

    def _get_signal_price(self, field: str, ago: int = 0) -> float:
        return self._signal_prices[field][ago]

    def _get_trade_price(self, field: str, ago: int = 0) -> float:
        return self._trade_prices[field][ago]

    def log(self, text: str) -> None:
        self.log_messages.append(text)

    def buy(self, size: int):
        self.last_buy_size = size
        return "BUY"

    def close(self):
        return "SELL"


class AtrPatchTests(unittest.TestCase):
    def test_loader_discovers_atr_patch(self):
        available = discover_available_patch_names()

        self.assertIn("atr", available)

    def test_calculate_buy_size_uses_atr_risk_budget(self):
        strategy = DummyStrategy()

        size = atr._calculate_buy_size_with_atr(strategy)

        self.assertEqual(size, 1800)

    def test_allow_buy_requires_breakout(self):
        strategy = DummyStrategy()

        decision = atr.allow_buy(strategy, {})
        self.assertTrue(decision["allow"])

        strategy._signal_prices["close"][0] = 9.9
        decision = atr.allow_buy(strategy, {})
        self.assertFalse(decision["allow"])
        self.assertIn("未突破20日高点", decision["reason"])

    def test_exit_reason_uses_stop_and_exit_floor(self):
        strategy = DummyStrategy()
        strategy.position = DummyPosition(size=1000, price=10.0)
        strategy._signal_prices["close"][0] = 9.6

        reason = atr._build_exit_reason(strategy)

        self.assertIn("ATR止损触发", reason)

    def test_before_next_triggers_add_on_order(self):
        strategy = DummyStrategy()
        strategy.position = DummyPosition(size=1000, price=10.0)
        state = getattr(strategy, atr.STATE_KEY)
        state["observed_position_size"] = 1000.0
        state["unit_size"] = 1000
        state["units"] = 1
        state["next_add_price"] = 10.5
        state["stop_price"] = 9.0

        atr.before_next(strategy, {})

        self.assertEqual(strategy.order, "BUY")
        self.assertEqual(strategy.last_buy_size, 1000)
        self.assertTrue(any("ATR补丁触发加仓" in item for item in strategy.log_messages))


if __name__ == "__main__":
    unittest.main()

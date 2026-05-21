import sys
import types
import unittest


if "backtrader" not in sys.modules:
    sys.modules["backtrader"] = types.SimpleNamespace(
        indicators=types.SimpleNamespace(
            AverageTrueRange=object,
            Highest=object,
            Lowest=object,
            SimpleMovingAverage=object,
        )
    )

from backtest.patches import sea_turtle


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
    def __init__(self, length: int, volume: float = 1000.0):
        self.length = length
        self.volume = DummyLine(volume)

    def __len__(self) -> int:
        return self.length


class DummyStrategy:
    def __init__(self):
        self.param = {
            "lot_size": 100,
            "stop_loss_pct": 0.1,
            "sea_turtle_risk_pct": 0.02,
            "sea_turtle_qr_threshold": 0.65,
            "sea_turtle_profit_target_pct": 0.2,
        }
        self.broker = DummyBroker(100000.0)
        self.position = DummyPosition()
        self.data = DummyData(length=80, volume=1200.0)
        self.last_buy_price = None
        self.log_messages = []
        setattr(
            self,
            sea_turtle.STATE_KEY,
            {
                "original_calculate_buy_size": lambda: 5000,
                "observed_position_size": 0.0,
                "unit_size": 1000,
                "units": 1,
                "entry_window": 20,
                "entry_base_price": 10.0,
                "stop_price": 9.0,
                "next_add_price": 10.5,
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
        self.sea_turtle_atr = DummyLine(1.0)
        self.sea_turtle_entry_high_20 = DummyLine({0: 10.5, -1: 10.0})
        self.sea_turtle_entry_high_55 = DummyLine({0: 10.9, -1: 10.4})
        self.sea_turtle_range_low_55 = DummyLine({0: 8.5, -1: 8.5})
        self.sea_turtle_slow_ma = DummyLine(9.8)
        self.sea_turtle_volume_ma = DummyLine(1000.0)
        self.sea_turtle_exit_low_10 = DummyLine({0: 9.7, -1: 9.8})
        self.sea_turtle_exit_low_20 = DummyLine({0: 9.5, -1: 9.6})
        self.sea_turtle_fast_ma = DummyLine(10.2)
        self.sea_turtle_mid_ma = DummyLine(10.0)

    def _get_signal_price(self, field: str, ago: int = 0) -> float:
        return self._signal_prices[field][ago]

    def _get_trade_price(self, field: str, ago: int = 0) -> float:
        return self._trade_prices[field][ago]

    def log(self, text: str) -> None:
        self.log_messages.append(text)


class SeaTurtlePatchTests(unittest.TestCase):
    def test_calculate_buy_size_uses_atr_risk_budget(self) -> None:
        strategy = DummyStrategy()

        size = sea_turtle._calculate_buy_size_with_turtle(strategy)

        self.assertEqual(size, 1800)

    def test_allow_buy_requires_breakout_and_filters(self) -> None:
        strategy = DummyStrategy()

        decision = sea_turtle.allow_buy(strategy, {})

        self.assertTrue(decision["allow"])

        strategy._signal_prices["close"][0] = 9.9
        decision = sea_turtle.allow_buy(strategy, {})
        self.assertFalse(decision["allow"])
        self.assertIn("未突破20日或55日高点", decision["reason"])

    def test_exit_reason_uses_10_day_floor_for_20_day_entry(self) -> None:
        strategy = DummyStrategy()
        strategy.position = DummyPosition(size=1000, price=10.0)
        strategy._signal_prices["close"][0] = 9.7

        reason = sea_turtle._build_exit_reason(strategy)

        self.assertIn("跌破10日低点", reason)

    def test_exit_reason_returns_none_when_trend_still_valid(self) -> None:
        strategy = DummyStrategy()
        strategy.position = DummyPosition(size=1000, price=10.0)
        strategy._signal_prices["close"][0] = 10.6

        reason = sea_turtle._build_exit_reason(strategy)

        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()

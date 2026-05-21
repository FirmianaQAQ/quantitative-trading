import unittest

from backtest.patches import dypre
from backtest.patches.loader import discover_available_patch_names


class DummyLine:
    def __init__(self, value):
        self.value = value

    def __getitem__(self, index):
        return self.value


class DummyLines:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, DummyLine(value))


class DummyPosition:
    def __init__(self, size=0.0):
        self.size = float(size)

    def __bool__(self):
        return abs(self.size) > 1e-9


class DummyData:
    def __init__(self, **kwargs):
        self.lines = DummyLines(**kwargs)
        for key, value in kwargs.items():
            setattr(self, key, DummyLine(value))
        if "close" not in kwargs:
            self.close = DummyLine(10.0)


class DummyStrategy:
    def __init__(self, adjust_flag="dypre", position_size=0.0, **data_kwargs):
        self.param = {"adjust_flag": adjust_flag}
        self.data = DummyData(**data_kwargs)
        self.position = DummyPosition(position_size)
        self.logged: list[str] = []

    def log(self, text):
        self.logged.append(text)


class DyprePatchTests(unittest.TestCase):
    def test_loader_discovers_dypre_patch(self):
        available = discover_available_patch_names()

        self.assertIn("dypre", available)

    def test_setup_patch_marks_enabled_for_dypre(self):
        strategy = DummyStrategy(
            raw_open=10.0,
            raw_high=10.0,
            raw_low=10.0,
            raw_close=10.0,
            raw_preclose=9.5,
            signal_factor=1.0,
            position_adjust_ratio=1.0,
        )

        dypre.setup_patch(strategy, {"patch_name": "dypre"})

        state = getattr(strategy, dypre.STATE_KEY)
        self.assertTrue(state["enabled"])
        self.assertEqual(state["missing_lines"], [])
        self.assertTrue(any("Dypre补丁已启用" in item for item in strategy.logged))

    def test_allow_buy_blocks_invalid_dypre_snapshot(self):
        strategy = DummyStrategy(
            raw_open=10.0,
            raw_high=10.0,
            raw_low=10.0,
            raw_close=0.0,
            raw_preclose=9.5,
            signal_factor=0.0,
            position_adjust_ratio=1.0,
        )
        dypre.setup_patch(strategy, {"patch_name": "dypre"})

        decision = dypre.allow_buy(strategy, {})

        self.assertFalse(decision["allow"])
        self.assertIn("raw_close 无效", decision["reason"])
        self.assertIn("signal_factor 无效", decision["reason"])

    def test_before_next_records_corporate_action_events(self):
        strategy = DummyStrategy(
            position_size=100.0,
            raw_open=10.0,
            raw_high=10.0,
            raw_low=10.0,
            raw_close=10.0,
            raw_preclose=20.0,
            signal_factor=1.0,
            position_adjust_ratio=2.0,
        )
        dypre.setup_patch(strategy, {"patch_name": "dypre"})

        dypre.before_next(strategy, {"bar_index": 12})

        state = getattr(strategy, dypre.STATE_KEY)
        self.assertEqual(state["corporate_action_events"], 1)
        self.assertEqual(state["holding_adjustment_events"], 1)
        self.assertEqual(state["last_bar_index"], 12)

    def test_non_dypre_mode_is_bypassed(self):
        strategy = DummyStrategy(
            adjust_flag="qfq",
            raw_open=10.0,
            raw_high=10.0,
            raw_low=10.0,
            raw_close=10.0,
            raw_preclose=9.5,
            signal_factor=1.0,
            position_adjust_ratio=1.0,
        )
        dypre.setup_patch(strategy, {"patch_name": "dypre"})

        decision = dypre.allow_buy(strategy, {})

        self.assertTrue(decision["allow"])
        self.assertTrue(any("Dypre补丁已旁路" in item for item in strategy.logged))


if __name__ == "__main__":
    unittest.main()

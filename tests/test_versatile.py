import copy
import unittest

from backtest import versatile


class VersatileConfigTests(unittest.TestCase):
    def test_default_config_is_valid(self):
        config = copy.deepcopy(versatile.CONFIG)

        versatile.validate_config(config)

    def test_atr_exit_period_cannot_exceed_breakout_period(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["atr_breakout_period"] = 5
        config["atr_exit_period"] = 6

        with self.assertRaisesRegex(
            ValueError, "atr_exit_period 不能大于 atr_breakout_period"
        ):
            versatile.validate_config(config)

    def test_atr_risk_pct_must_be_between_zero_and_one(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["atr_risk_pct"] = 1

        with self.assertRaisesRegex(
            ValueError, "atr_risk_pct 必须大于 0 且小于 1"
        ):
            versatile.validate_config(config)


if __name__ == "__main__":
    unittest.main()

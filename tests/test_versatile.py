import copy
import unittest

from backtest.backtest_v1 import compute_optimization_score, parse_decimal_range
from backtest import versatile


class VersatileConfigTests(unittest.TestCase):
    def test_default_config_is_valid(self):
        config = copy.deepcopy(versatile.CONFIG)

        versatile.validate_config(config)

    def test_atr_exit_period_cannot_exceed_breakout_period(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["patches"] = ["atr"]
        config["atr_breakout_period"] = 5
        config["atr_exit_period"] = 6

        with self.assertRaisesRegex(
            ValueError, "atr_exit_period 不能大于 atr_breakout_period"
        ):
            versatile.validate_config(config)

    def test_atr_risk_pct_must_be_between_zero_and_one(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["patches"] = ["atr"]
        config["atr_risk_pct"] = 1

        with self.assertRaisesRegex(
            ValueError, "atr_risk_pct 必须大于 0 且小于 1"
        ):
            versatile.validate_config(config)

    def test_buy_limit_position_pct_must_be_between_zero_and_one(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["buy_limit_position_pct"] = 1

        with self.assertRaisesRegex(
            ValueError, "buy_limit_position_pct 必须大于 0 且小于 1"
        ):
            versatile.validate_config(config)

    def test_optimize_config_accepts_decimal_range_for_buy_limit_position_pct(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["optimize"] = True
        config["plot"] = False
        config["opt_buy_limit_position_pct"] = "0.75:0.95:0.05"
        config["opt_protect_profit_floor_pct"] = "0.02:0.05:0.01"
        config["opt_sell_trigger_multiplier"] = "0.80:0.95:0.05"

        versatile.validate_config(config)

    def test_parse_decimal_range_supports_inclusive_float_grid(self):
        values = parse_decimal_range("0.75:0.95:0.05", "opt_buy_limit_position_pct")

        self.assertEqual(values, [0.75, 0.8, 0.85, 0.9, 0.95])

    def test_compute_optimization_score_penalizes_drawdown(self):
        config = copy.deepcopy(versatile.CONFIG)
        better = {
            "annual_return_pct": 12.0,
            "max_drawdown_pct": 4.0,
            "sharpe_ratio": 0.8,
        }
        worse = {
            "annual_return_pct": 12.0,
            "max_drawdown_pct": 9.0,
            "sharpe_ratio": 0.8,
        }

        self.assertGreater(
            compute_optimization_score(better, config),
            compute_optimization_score(worse, config),
        )

    def test_compute_optimization_score_penalizes_overtrading(self):
        config = copy.deepcopy(versatile.CONFIG)
        config["opt_score_trade_penalty_weight"] = 0.1
        calmer = {
            "annual_return_pct": 10.0,
            "max_drawdown_pct": 4.0,
            "sharpe_ratio": 0.8,
            "trades_total": 8,
        }
        noisier = {
            "annual_return_pct": 10.0,
            "max_drawdown_pct": 4.0,
            "sharpe_ratio": 0.8,
            "trades_total": 18,
        }

        self.assertGreater(
            compute_optimization_score(calmer, config),
            compute_optimization_score(noisier, config),
        )


if __name__ == "__main__":
    unittest.main()

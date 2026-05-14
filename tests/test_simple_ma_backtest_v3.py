import unittest

from backtest.simple_ma_backtest_v3 import (
    CONFIG,
    evaluate_v3_breakout_entry,
    evaluate_v3_protective_exit,
    validate_config,
)


class SimpleMABacktestV3Tests(unittest.TestCase):
    def test_breakout_entry_requires_trend_and_momentum(self) -> None:
        breakout_ok, info = evaluate_v3_breakout_entry(
            current_close=109.9,
            previous_close=107.5,
            fast_ma=107.0,
            slow_ma=100.0,
            slow_ma_prev=99.7,
            recent_breakout_high=110.0,
            momentum_return_pct=0.03,
            crossover_signal=1.0,
            params=CONFIG,
        )

        self.assertTrue(breakout_ok)
        self.assertTrue(info["trend_ok"])
        self.assertTrue(info["momentum_ok"])

    def test_breakout_entry_rejects_overextended_chase(self) -> None:
        breakout_ok, info = evaluate_v3_breakout_entry(
            current_close=114.0,
            previous_close=110.0,
            fast_ma=108.0,
            slow_ma=100.0,
            slow_ma_prev=99.7,
            recent_breakout_high=110.0,
            momentum_return_pct=0.05,
            crossover_signal=1.0,
            params=CONFIG,
        )

        self.assertFalse(breakout_ok)
        self.assertGreater(info["breakout_extension_pct"], CONFIG["breakout_max_extension_pct"])

    def test_protective_exit_prefers_trailing_stop_when_profit_retraces(self) -> None:
        exit_name, exit_info = evaluate_v3_protective_exit(
            current_close=113.0,
            fast_ma=115.0,
            slow_ma=106.0,
            last_buy_price=100.0,
            highest_close_since_entry=121.0,
            recent_down_days=1,
            momentum_return_pct=-0.01,
            params=CONFIG,
        )

        self.assertEqual(exit_name, "trailing_stop")
        self.assertTrue(exit_info["trailing_stop_hit"])

    def test_validate_config_rejects_invalid_momentum_days(self) -> None:
        bad_config = dict(CONFIG)
        bad_config["momentum_entry_up_days_required"] = (
            bad_config["momentum_entry_window"] + 1
        )

        with self.assertRaisesRegex(
            ValueError, "momentum_entry_up_days_required"
        ):
            validate_config(bad_config)


if __name__ == "__main__":
    unittest.main()

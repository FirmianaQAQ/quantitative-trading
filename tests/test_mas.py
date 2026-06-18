import unittest

import pandas as pd

from backtest.mas import CONFIG, _evaluate_entry_filters, validate_config


class MasConfigTests(unittest.TestCase):
    def test_validate_config_accepts_uptrend_mode(self) -> None:
        config = dict(CONFIG)
        config["market_trend_mode"] = "uptrend"

        validate_config(config)

    def test_validate_config_rejects_invalid_market_trend_mode(self) -> None:
        config = dict(CONFIG)
        config["market_trend_mode"] = "bull"

        with self.assertRaisesRegex(ValueError, "market_trend_mode"):
            validate_config(config)


class MasFilterTests(unittest.TestCase):
    def test_uptrend_mode_relaxes_entry_filters(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "fundamental_pass": True,
                    "chip_concentration_pct": 16.5,
                    "chip_single_peak": False,
                    "recent_gain_pct": 0.45,
                    "relative_position_pct": 0.82,
                    "volume_ratio": 1.20,
                    "volume_expand_ratio": 1.05,
                    "ma_short_avg_slope_pct": 0.0036,
                    "ma_mid_slope_pct": 0.01,
                    "ma_long_slope_pct": 0.01,
                }
            ]
        )

        normal_config = dict(CONFIG)
        normal_config.update(
            {
                "market_trend_mode": "normal",
                "stage1_require_volume_confirm": True,
            }
        )
        uptrend_config = dict(normal_config)
        uptrend_config["market_trend_mode"] = "uptrend"

        normal_allowed, normal_reasons = _evaluate_entry_filters(
            frame,
            0,
            normal_config,
            stage_signal=1,
        )
        uptrend_allowed, uptrend_reasons = _evaluate_entry_filters(
            frame,
            0,
            uptrend_config,
            stage_signal=1,
        )

        self.assertFalse(normal_allowed)
        self.assertTrue(normal_reasons)
        self.assertTrue(uptrend_allowed)
        self.assertEqual(uptrend_reasons, [])
        self.assertIn("筹码分布不是单峰密集", normal_reasons)


if __name__ == "__main__":
    unittest.main()

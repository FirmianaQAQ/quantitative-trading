import unittest

from backtest.datang_simple_ma_backtest import CONFIG, DATANG_CODE, validate_config
from backtest.strategy_registry import list_strategy_specs


class DatangSimpleMABacktestTests(unittest.TestCase):
    def test_validate_config_rejects_non_datang_code(self) -> None:
        bad_config = dict(CONFIG)
        bad_config["code"] = "sh.600236"

        with self.assertRaisesRegex(ValueError, DATANG_CODE):
            validate_config(bad_config)

    def test_strategy_registry_contains_datang_family(self) -> None:
        specs = {spec.strategy_id: spec for spec in list_strategy_specs()}
        self.assertIn("datang_simple_ma_backtest", specs)
        self.assertEqual(
            specs["datang_simple_ma_backtest"].family_id,
            "specialized_ma_backtest",
        )


if __name__ == "__main__":
    unittest.main()

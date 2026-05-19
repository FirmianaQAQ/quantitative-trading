import unittest

from backtest.boe_simple_ma_backtest import BOE_CODE, CONFIG, validate_config
from backtest.strategy_registry import list_strategy_specs


class BoeSimpleMABacktestTests(unittest.TestCase):
    def test_validate_config_rejects_non_boe_code(self) -> None:
        bad_config = dict(CONFIG)
        bad_config["code"] = "sz.000100"

        with self.assertRaisesRegex(ValueError, BOE_CODE):
            validate_config(bad_config)

    def test_strategy_registry_contains_boe_family(self) -> None:
        strategy_ids = {spec.strategy_id for spec in list_strategy_specs()}
        self.assertIn("boe_simple_ma_backtest", strategy_ids)


if __name__ == "__main__":
    unittest.main()

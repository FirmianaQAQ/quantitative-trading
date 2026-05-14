import unittest

from backtest.strategy_registry import list_strategy_specs
from backtest.tcl_simple_ma_backtest import CONFIG, TCL_CODE, validate_config


class TclSimpleMABacktestTests(unittest.TestCase):
    def test_validate_config_rejects_non_tcl_code(self) -> None:
        bad_config = dict(CONFIG)
        bad_config["code"] = "sz.000725"

        with self.assertRaisesRegex(ValueError, TCL_CODE):
            validate_config(bad_config)

    def test_strategy_registry_contains_tcl_family(self) -> None:
        strategy_ids = {spec.strategy_id for spec in list_strategy_specs()}
        self.assertIn("tcl_simple_ma_backtest", strategy_ids)


if __name__ == "__main__":
    unittest.main()

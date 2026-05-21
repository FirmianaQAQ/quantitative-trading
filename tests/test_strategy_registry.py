import unittest

from backtest.strategy_registry import get_default_strategy_spec, list_strategy_specs
from utils.default_stocks import (
    DEFAULT_BASE_STRATEGY_ID,
    DEFAULT_BASE_STRATEGY_MODULE_NAME,
    DEFAULT_BASE_STRATEGY_NAME,
    normalize_strategy_source_module,
)


class DefaultStrategySourceTests(unittest.TestCase):
    def tearDown(self) -> None:
        list_strategy_specs.cache_clear()

    def test_normalize_strategy_source_module_supports_python_file_path(self) -> None:
        self.assertEqual(
            normalize_strategy_source_module("backtest/versatile.py"),
            "backtest.versatile",
        )
        self.assertEqual(
            normalize_strategy_source_module(r"backtest\\versatile.py"),
            "backtest.versatile",
        )

    def test_default_strategy_spec_points_to_configured_source_module(self) -> None:
        list_strategy_specs.cache_clear()

        spec = get_default_strategy_spec()

        self.assertEqual(spec.strategy_id, DEFAULT_BASE_STRATEGY_ID)
        self.assertEqual(spec.module_name, DEFAULT_BASE_STRATEGY_MODULE_NAME)
        self.assertEqual(spec.display_name, DEFAULT_BASE_STRATEGY_NAME)


if __name__ == "__main__":
    unittest.main()

import unittest

from backtest.strategy_registry import (
    get_default_strategy_spec,
    group_strategy_specs,
    list_strategy_specs,
)
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
            normalize_strategy_source_module("backtest/mas.py"),
            "backtest.mas",
        )
        self.assertEqual(
            normalize_strategy_source_module(r"backtest\\mas.py"),
            "backtest.mas",
        )

    def test_default_strategy_spec_points_to_configured_source_module(self) -> None:
        list_strategy_specs.cache_clear()

        spec = get_default_strategy_spec()

        self.assertEqual(spec.strategy_id, DEFAULT_BASE_STRATEGY_ID)
        self.assertEqual(spec.module_name, DEFAULT_BASE_STRATEGY_MODULE_NAME)
        self.assertEqual(spec.display_name, DEFAULT_BASE_STRATEGY_NAME)

    def test_registry_exposes_only_mas_as_top_level_family(self) -> None:
        list_strategy_specs.cache_clear()

        grouped_specs = group_strategy_specs()
        family_names = [family_name for family_name, _ in grouped_specs]

        self.assertEqual(family_names, [DEFAULT_BASE_STRATEGY_NAME])
        self.assertNotIn("Versatile", family_names)
        mas_specs = grouped_specs[0][1]
        self.assertEqual(len(mas_specs), 1)
        self.assertEqual(mas_specs[0].strategy_id, "MAS")
        self.assertEqual(mas_specs[0].family_id, "MAS")


if __name__ == "__main__":
    unittest.main()

import unittest
from importlib import import_module

from backtest.strategy_registry import list_strategy_specs
from backtest.extended_strategies.specialized_ma_support import (
    SPECIALIZED_BASE_TEMPLATE,
    resolve_specialized_profile,
)
from utils.default_stocks import DEFAULT_STOCK_CODES


SPECIALIZED_DEFAULT_CASES = [
    ("backtest.extended_strategies.guiguan_simple_ma_backtest", "guiguan_simple_ma_backtest", "sh.600236", "sh.600036"),
    ("backtest.extended_strategies.cmb_simple_ma_backtest", "cmb_simple_ma_backtest", "sh.600036", "sh.600236"),
    ("backtest.extended_strategies.shandong_fiberglass_simple_ma_backtest", "shandong_fiberglass_simple_ma_backtest", "sh.605006", "sh.600236"),
    ("backtest.extended_strategies.southern_air_simple_ma_backtest", "southern_air_simple_ma_backtest", "sh.600029", "sh.600236"),
    ("backtest.extended_strategies.perfect_world_simple_ma_backtest", "perfect_world_simple_ma_backtest", "sh.002624", "sh.600236"),
    ("backtest.extended_strategies.haier_simple_ma_backtest", "haier_simple_ma_backtest", "sh.600690", "sh.600236"),
    ("backtest.extended_strategies.nari_simple_ma_backtest", "nari_simple_ma_backtest", "sh.600406", "sh.600236"),
    ("backtest.extended_strategies.wolong_simple_ma_backtest", "wolong_simple_ma_backtest", "sh.600580", "sh.600236"),
    ("backtest.extended_strategies.kangguan_simple_ma_backtest", "kangguan_simple_ma_backtest", "sz.001308", "sh.600236"),
    ("backtest.extended_strategies.byd_simple_ma_backtest", "byd_simple_ma_backtest", "sz.002594", "sh.600236"),
]


class SpecializedDefaultBacktestsTests(unittest.TestCase):
    def test_validate_config_rejects_non_target_code(self) -> None:
        for module_name, _strategy_id, expected_code, wrong_code in SPECIALIZED_DEFAULT_CASES:
            with self.subTest(module=module_name):
                module = import_module(module_name)
                bad_config = dict(module.CONFIG)
                bad_config["code"] = wrong_code
                with self.assertRaisesRegex(ValueError, expected_code):
                    module.validate_config(bad_config)

    def test_strategy_registry_contains_all_specialized_defaults(self) -> None:
        specs = {spec.strategy_id: spec for spec in list_strategy_specs()}
        for _module_name, strategy_id, expected_code, _wrong_code in SPECIALIZED_DEFAULT_CASES:
            with self.subTest(strategy_id=strategy_id):
                self.assertIn(strategy_id, specs)
                self.assertEqual(specs[strategy_id].family_id, "specialized_ma_backtest")
                self.assertEqual(specs[strategy_id].config.get("code"), expected_code)

    def test_all_default_stocks_have_specialized_strategy(self) -> None:
        specialized_codes = {
            str(spec.config.get("code"))
            for spec in list_strategy_specs()
            if spec.family_id == "specialized_ma_backtest"
        }
        for code in DEFAULT_STOCK_CODES:
            with self.subTest(code=code):
                self.assertIn(code, specialized_codes)

    def test_specialized_profiles_are_no_longer_tcl_one_size_fits_all(self) -> None:
        tcl_profile = resolve_specialized_profile("sz.000100")
        boe_profile = resolve_specialized_profile("sz.000725")
        cmb_profile = resolve_specialized_profile("sh.600036")
        byd_profile = resolve_specialized_profile("sz.002594")

        self.assertNotEqual(boe_profile, tcl_profile)
        self.assertNotEqual(cmb_profile, tcl_profile)
        self.assertNotEqual(byd_profile, tcl_profile)
        self.assertNotEqual(cmb_profile, byd_profile)

    def test_specialized_configs_apply_stock_specific_overrides(self) -> None:
        specs = {spec.strategy_id: spec for spec in list_strategy_specs()}

        self.assertEqual(specs["tcl_simple_ma_backtest"].config["fast"], 10)
        self.assertEqual(specs["boe_simple_ma_backtest"].config["fast"], 9)
        self.assertEqual(specs["cmb_simple_ma_backtest"].config["stop_loss_pct"], 0.06)
        self.assertEqual(specs["byd_simple_ma_backtest"].config["breakout_power_threshold"], 0.68)

        for strategy_id in (
            "tcl_simple_ma_backtest",
            "boe_simple_ma_backtest",
            "cmb_simple_ma_backtest",
            "byd_simple_ma_backtest",
        ):
            with self.subTest(strategy_id=strategy_id):
                self.assertEqual(
                    specs[strategy_id].config["benchmark_code"],
                    SPECIALIZED_BASE_TEMPLATE["benchmark_code"],
                )


if __name__ == "__main__":
    unittest.main()

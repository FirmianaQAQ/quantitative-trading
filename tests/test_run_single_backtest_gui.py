import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backtest.strategy_registry import StrategySpec
from utils.default_stocks import DEFAULT_BASE_STRATEGY_ID, DEFAULT_BASE_STRATEGY_NAME
from run_single_backtest_gui import (
    AI_ANALYSIS_OFF,
    AI_ANALYSIS_ON,
    MANUAL_MENU_VALUE,
    aggregate_recommendation_results_by_code,
    choose_stock_interactively,
    parse_cli_args,
    prompt_strategy_menu,
    resolve_ai_analysis_enabled,
    sync_manual_stock_selection,
    write_family_dashboard_report,
)


def _dummy_strategy_spec(strategy_id: str, display_name: str) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        module_name="test.module",
        display_name=display_name,
        brief_description="测试版本",
        family_id="test_family",
        family_display_name="测试家族",
        version_number=1,
        config={},
        test_cases=[],
        run_backtest=lambda config, df: {},
        validate_config=lambda config: None,
    )


class FamilyDashboardReportTests(unittest.TestCase):
    def test_prompt_strategy_menu_skips_second_level_for_single_version_family(self) -> None:
        single_spec = _dummy_strategy_spec(
            DEFAULT_BASE_STRATEGY_ID,
            DEFAULT_BASE_STRATEGY_NAME,
        )
        with patch(
            "run_single_backtest_gui.group_strategy_specs",
            return_value=[(DEFAULT_BASE_STRATEGY_NAME, [single_spec])],
        ):
            with patch("builtins.input", side_effect=["1"]):
                self.assertEqual(prompt_strategy_menu(), DEFAULT_BASE_STRATEGY_ID)

    def test_parse_cli_args_supports_ai_flag_with_strategy_and_stock(self) -> None:
        with patch(
            "sys.argv",
            [
                "run_single_backtest_gui.py",
                "--ai=off",
                DEFAULT_BASE_STRATEGY_ID,
                "sz.000725",
            ],
        ):
            strategy_id, stock_code, ai_mode = parse_cli_args()

        self.assertEqual(strategy_id, DEFAULT_BASE_STRATEGY_ID)
        self.assertEqual(stock_code, "sz.000725")
        self.assertEqual(ai_mode, AI_ANALYSIS_OFF)

    def test_sync_manual_stock_selection_syncs_required_codes(self) -> None:
        pair_spec = StrategySpec(
            strategy_id="pair_trade_backtest",
            module_name="test.module",
            display_name="配对策略",
            brief_description="测试版本",
            family_id="pair_trade_backtest",
            family_display_name="配对策略",
            version_number=1,
            config={},
            test_cases=[],
            run_backtest=lambda config, df: {},
            validate_config=lambda config: None,
        )
        with patch(
            "run_single_backtest_gui.sync_single_stock_data",
            return_value=True,
        ) as sync_mock:
            result = sync_manual_stock_selection(
                pair_spec,
                "pair_auto|sz.000725|sz.002594",
            )

        self.assertTrue(result)
        self.assertEqual(
            [call.args[0] for call in sync_mock.call_args_list],
            ["sz.000725", "sz.002594"],
        )

    def test_choose_stock_interactively_manual_input_syncs_before_return(self) -> None:
        single_spec = _dummy_strategy_spec(
            DEFAULT_BASE_STRATEGY_ID,
            DEFAULT_BASE_STRATEGY_NAME,
        )
        with patch(
            "run_single_backtest_gui.prompt_stock_menu",
            side_effect=[MANUAL_MENU_VALUE],
        ):
            with patch("builtins.input", side_effect=["000725"]):
                with patch(
                    "run_single_backtest_gui.sync_manual_stock_selection",
                    return_value=True,
                ) as sync_mock:
                    selected = choose_stock_interactively(single_spec)

        self.assertEqual(selected, "sz.000725")
        sync_mock.assert_called_once_with(single_spec, "sz.000725")

    def test_resolve_ai_analysis_enabled_honors_explicit_switch(self) -> None:
        with patch("run_single_backtest_gui.is_llm_analysis_available", return_value=False):
            self.assertTrue(resolve_ai_analysis_enabled(AI_ANALYSIS_ON))
            self.assertFalse(resolve_ai_analysis_enabled(AI_ANALYSIS_OFF))

    def test_dashboard_embeds_child_reports_into_single_html(self) -> None:
        spec_a = _dummy_strategy_spec("strategy_a", "版本A")
        spec_b = _dummy_strategy_spec("strategy_b", "版本B")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            report_a = temp_path / "report_a.html"
            report_b = temp_path / "report_b.html"
            report_a.write_text(
                "<!DOCTYPE html><html><body><h1>报告A</h1><script>console.log('a')</script></body></html>",
                encoding="utf-8",
            )
            report_b.write_text(
                "<!DOCTYPE html><html><body><h1>报告B</h1></body></html>",
                encoding="utf-8",
            )

            dashboard_path = write_family_dashboard_report(
                family_name="测试家族",
                code="sz.000725",
                stock_label="京东方A",
                cash=100000.0,
                active_strategy_id="strategy_a",
                version_reports=[
                    (spec_a, report_a),
                    (spec_b, report_b),
                ],
            )
            dashboard_html = dashboard_path.read_text(encoding="utf-8")

        self.assertIn("const embeddedReports =", dashboard_html)
        self.assertIn("iframe.srcdoc = embeddedReports[panel.dataset.panel]", dashboard_html)
        self.assertIn("报告A", dashboard_html)
        self.assertIn("报告B", dashboard_html)
        self.assertIn("<\\/script>", dashboard_html)
        self.assertNotIn('src="report_a.html"', dashboard_html)
        self.assertNotIn('src="report_b.html"', dashboard_html)

        if dashboard_path.exists():
            dashboard_path.unlink()

    def test_aggregate_recommendation_results_by_code_keeps_best_adjust_flag_only(self) -> None:
        ranked_results = [
            (
                "sz.000725",
                "hfq",
                {
                    "annual_return_pct": 18.0,
                    "sharpe_ratio": 1.1,
                    "max_drawdown_pct": 12.0,
                },
            ),
            (
                "sz.000725",
                "qfq",
                {
                    "annual_return_pct": 22.0,
                    "sharpe_ratio": 1.3,
                    "max_drawdown_pct": 10.0,
                },
            ),
            (
                "sh.600580",
                "cq",
                {
                    "annual_return_pct": 20.0,
                    "sharpe_ratio": 1.2,
                    "max_drawdown_pct": 9.0,
                },
            ),
        ]

        aggregated = aggregate_recommendation_results_by_code(ranked_results)

        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0][0], "sz.000725")
        self.assertEqual(aggregated[0][1], "qfq")
        self.assertEqual(aggregated[1][0], "sh.600580")


if __name__ == "__main__":
    unittest.main()

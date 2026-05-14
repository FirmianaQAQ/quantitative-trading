import tempfile
import unittest
from pathlib import Path

from backtest.strategy_registry import StrategySpec
from run_single_backtest_gui import write_family_dashboard_report


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


if __name__ == "__main__":
    unittest.main()

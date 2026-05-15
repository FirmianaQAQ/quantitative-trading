import unittest
import tempfile
from pathlib import Path

from utils.backtest_report import (
    _build_advice_panel,
    _extract_daily_advice_entries,
    html as generate_backtest_html,
)


def build_buy_sell_report(
    *,
    dates: list[str],
    buy_points: list[list[str | float]] | None = None,
    sell_points: list[list[str | float]] | None = None,
) -> list[dict]:
    candles = [[10.0 + index, 10.0 + index] for index, _ in enumerate(dates)]
    return [
        {
            "chart_name": "买卖点",
            "chart_data": {
                "x_axis": dates,
                "candles": candles,
                "buy_points": buy_points or [],
                "sell_points": sell_points or [],
                "indicator_lines": [],
            },
        }
    ]


class BacktestReportAdviceTests(unittest.TestCase):
    def test_latest_advice_respects_empty_position_when_backtest_is_holding(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-13", "2026-05-14"],
            buy_points=[["2026-05-13", 10.0]],
        )

        entries = _extract_daily_advice_entries(
            report_data,
            log_lines=[],
            current_position="empty",
        )

        self.assertEqual(entries[0]["date"], "2026-05-14")
        self.assertEqual(entries[0]["action"], "observe")
        self.assertIn("当前实际空仓", str(entries[0]["reason"]))

    def test_latest_advice_skips_sell_when_user_is_empty(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-13", "2026-05-14"],
            buy_points=[["2026-05-13", 10.0]],
            sell_points=[["2026-05-14", 11.0]],
        )

        entries = _extract_daily_advice_entries(
            report_data,
            log_lines=[],
            current_position="empty",
        )

        self.assertEqual(entries[0]["date"], "2026-05-14")
        self.assertEqual(entries[0]["action"], "observe")
        self.assertIn("卖出信号无需执行", str(entries[0]["reason"]))

    def test_advice_panel_contains_all_position_tabs(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-13", "2026-05-14"],
            buy_points=[["2026-05-13", 10.0]],
            sell_points=[["2026-05-14", 11.0]],
        )

        html = _build_advice_panel(
            report_data,
            log_lines=[],
            current_position="hold",
        )

        self.assertIn('data-advice-position-mode="auto"', html)
        self.assertIn('data-advice-position-mode="empty"', html)
        self.assertIn('data-advice-position-mode="hold"', html)
        self.assertIn('data-position-mode-stats="auto"', html)
        self.assertIn('data-position-mode-stats="empty"', html)
        self.assertIn('data-position-mode-stats="hold"', html)
        self.assertIn(
            'class="advice-position-chip is-active"',
            html,
        )
        self.assertIn("当前实际持仓", html)

    def test_advice_panel_contains_optimized_strategy_source(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-13", "2026-05-14"],
            buy_points=[["2026-05-13", 10.0]],
        )
        report_data.append(
            {
                "chart_name": "优化买卖点",
                "chart_data": {
                    "x_axis": ["2026-05-13", "2026-05-14"],
                    "candles": [[10.0, 10.0], [11.0, 11.0]],
                    "buy_points": [["2026-05-13", 10.0]],
                    "sell_points": [],
                    "indicator_lines": [],
                    "advice_entries": [
                        {
                            "date": "2026-05-14",
                            "action": "watch_buy",
                            "title": "优化观察",
                            "price": "11.00",
                            "summary": "等待更好的入场点。",
                            "reason": "趋势转暖，但不追高。",
                            "is_signal": True,
                        }
                    ],
                },
            }
        )

        html = _build_advice_panel(
            report_data,
            log_lines=[],
            current_position="auto",
        )

        self.assertIn('data-advice-source="optimized"', html)
        self.assertIn('data-default-advice-source="optimized"', html)
        self.assertIn("优化策略", html)
        self.assertIn("优化观察", html)

    def test_html_report_title_can_include_ai_link(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-13", "2026-05-14"],
            buy_points=[["2026-05-13", 10.0]],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "report.html"
            generate_backtest_html(
                report_data=report_data,
                output_path=str(output_path),
                benchmarks=[],
                title="测试回测报告",
                ai_report_link="../llm_analysis/test-ai-report.html",
            )
            html = output_path.read_text(encoding="utf-8")

        self.assertIn("page-header-ai-link", html)
        self.assertIn('href="../llm_analysis/test-ai-report.html"', html)
        self.assertIn(">AI<", html)


if __name__ == "__main__":
    unittest.main()

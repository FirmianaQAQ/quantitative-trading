import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from utils.default_stocks import DEFAULT_BASE_STRATEGY_NAME
from utils.backtest_report import (
    _build_next_trade_plan_card,
    _build_advice_panel,
    _build_metric_cards,
    _extract_daily_advice_entries,
    html as generate_backtest_html,
    merge_backtest_html_with_ai_report,
)
from utils.backtest_report_builder import (
    build_enhanced_trade_chart_data,
    build_empty_entry_timing_plan,
    build_next_trade_plan,
    describe_adjust_flag,
    extract_trade_metrics,
    extract_next_trade_plan_from_chart_data,
)


def build_buy_sell_report(
    *,
    dates: list[str],
    buy_points: list[list[str | float]] | None = None,
    sell_points: list[list[str | float]] | None = None,
    ex_right_closes: list[float | None] | None = None,
) -> list[dict]:
    candles = [[10.0 + index, 10.0 + index] for index, _ in enumerate(dates)]
    return [
        {
            "chart_name": "买卖点",
            "chart_data": {
                "x_axis": dates,
                "candles": candles,
                "ex_right_closes": ex_right_closes or [None] * len(dates),
                "buy_points": buy_points or [],
                "sell_points": sell_points or [],
                "indicator_lines": [],
            },
        }
    ]


class BacktestReportAdviceTests(unittest.TestCase):
    def test_extract_trade_metrics_falls_back_to_strategy_counters(self) -> None:
        strategy = SimpleNamespace(
            completed_trades_total=3,
            completed_trades_won=2,
            completed_trades_lost=1,
            completed_trade_net_profit=1500.0,
        )

        metrics = extract_trade_metrics({}, strategy=strategy)

        self.assertEqual(metrics["trades_total"], 3)
        self.assertEqual(metrics["trades_won"], 2)
        self.assertEqual(metrics["trades_lost"], 1)
        self.assertEqual(metrics["win_rate_pct"], 66.67)
        self.assertEqual(metrics["net_profit"], 1500.0)
        self.assertEqual(metrics["avg_trade_profit"], 500.0)

    def test_extract_trade_metrics_uses_sell_estimate_when_trade_counter_missing(self) -> None:
        strategy = SimpleNamespace(
            completed_trades_total=0,
            completed_sell_orders=4,
            sell_markers=[1, 2, 3, 4],
            completed_sell_estimated_won=3,
            completed_sell_estimated_lost=1,
            completed_sell_estimated_net_profit=800.0,
        )

        metrics = extract_trade_metrics({}, strategy=strategy)

        self.assertEqual(metrics["trades_total"], 4)
        self.assertEqual(metrics["trades_won"], 3)
        self.assertEqual(metrics["trades_lost"], 1)
        self.assertEqual(metrics["win_rate_pct"], 75.0)
        self.assertEqual(metrics["net_profit"], 800.0)
        self.assertEqual(metrics["avg_trade_profit"], 200.0)

    def test_describe_adjust_flag_explains_dypre_semantics(self) -> None:
        description = describe_adjust_flag("dypre")

        self.assertIn("Dypre 动态前复权", description)
        self.assertIn("前复权", description)
        self.assertIn("不复权", description)
        self.assertIn("调整持仓股数", description)

    def test_metric_cards_hide_redundant_strategy_and_forecast_cards(self) -> None:
        html = _build_metric_cards(
            [
                {
                    "chart_name": "指标概览",
                    "chart_data": {
                        "股票代码": "sh.600236",
                        "策略名称": DEFAULT_BASE_STRATEGY_NAME,
                        "复权口径": "Dypre 动态前复权",
                        "均线说明": "快线看短期节奏，慢线看中期趋势。",
                        "总收益率": "41.20%",
                        "空仓-下一交易日策略": "观察买点",
                        "空仓-预判摘要": "趋势转暖，但仍需等更好的入场点。",
                        "空仓-建仓时机": "等待趋势翻多",
                        "空仓-建仓提示": "当前均线结构还没完全转强。",
                        "持仓-下一交易日策略": "偏持有",
                        "持仓-预判摘要": "当前更适合继续持有。",
                        "新闻情绪": "偏积极",
                        "新闻主题": "订单合同",
                        "资金面判断": "偏流入",
                        "财报面判断": "中性",
                    },
                }
            ]
        )

        self.assertIn("总收益率", html)
        self.assertNotIn('data-metric-label="股票代码"', html)
        self.assertNotIn("策略名称", html)
        self.assertNotIn("复权口径", html)
        self.assertNotIn("均线说明", html)
        self.assertNotIn("空仓-下一交易日策略", html)
        self.assertNotIn("空仓-建仓提示", html)
        self.assertNotIn("持仓-下一交易日策略", html)
        self.assertNotIn("新闻情绪", html)
        self.assertNotIn("新闻主题", html)
        self.assertNotIn("资金面判断", html)
        self.assertNotIn("财报面判断", html)

    def test_next_trade_plan_card_contains_action_summary_and_reason(self) -> None:
        report_data = [
            {
                "chart_name": "优化买卖点",
                "chart_data": {
                    "x_axis": ["2026-05-19"],
                    "candles": [[10.0, 10.1, 9.9, 10.2]],
                    "buy_points": [],
                    "sell_points": [],
                    "indicator_lines": [],
                    "advice_entries": [
                        {
                            "date": "2026-05-19",
                            "action": "hold",
                            "title": "优化持有",
                            "summary": "趋势尚未破坏，继续持有观察。",
                            "reason": "当前优化规则下仍未触发止损、回撤保护或趋势转弱卖点。",
                            "is_signal": False,
                        }
                    ],
                },
            },
        ]

        html = _build_next_trade_plan_card(report_data)

        self.assertIn("当前生成时的购买策略", html)
        self.assertIn("按当前生成结果直接给出空仓与持仓两种执行视角", html)
        self.assertIn('class="forecast-scenario-reason"', html)
        self.assertIn("如果你当前空仓", html)
        self.assertIn("如果你当前持仓", html)
        self.assertIn("继续观察", html)
        self.assertIn("继续持有", html)
        self.assertIn("预判依据", html)
        self.assertIn("2026-05-19", html)
        self.assertNotIn("今日策略", html)
        self.assertNotIn("明日策略", html)

    def test_build_enhanced_trade_chart_data_keeps_optimized_plan_when_no_advice_entries(self) -> None:
        filtered_df = pd.DataFrame(
            [
                {"date": "2026-05-19", "close": 10.2},
            ]
        )
        optimized_chart_data = {
            "x_axis": ["2026-05-19"],
            "candles": [[10.0, 10.1, 9.9, 10.2]],
            "buy_points": [],
            "sell_points": [],
            "indicator_lines": [],
            "advice_entries": [],
        }

        result = build_enhanced_trade_chart_data(
            filtered_df=filtered_df,
            optimized_chart_data=optimized_chart_data,
            external_context=None,
        )

        self.assertEqual(result, optimized_chart_data)

    def test_build_enhanced_trade_chart_data_preserves_main_strategy_signal(self) -> None:
        filtered_df = pd.DataFrame(
            [
                {"date": "2026-05-19", "close": 10.2},
            ]
        )
        optimized_chart_data = {
            "x_axis": ["2026-05-19"],
            "candles": [[10.0, 10.1, 9.9, 10.2]],
            "buy_points": [],
            "sell_points": [],
            "indicator_lines": [],
            "advice_entries": [
                {
                    "date": "2026-05-19",
                    "action": "watch_buy",
                    "title": "优化观察",
                    "summary": "等待更好的入场点。",
                    "reason": "趋势转暖，但不追高。",
                    "is_signal": True,
                }
            ],
        }

        with patch(
            "utils.backtest_report_builder.build_strategy_enhancement_patch",
            return_value={
                "action": "observe",
                "title": "优化观望",
                "display_action": "优化观望",
                "summary": "外部因子明显转弱，暂缓偏多信号。",
                "reason": "新闻面偏谨慎，资金面偏流出。",
                "enhancement_score": -3,
                "enhancement_label": "偏谨慎",
                "news_sentiment_label": "偏负面",
                "fund_flow_label": "偏流出",
                "financial_label": "中性",
            },
        ):
            result = build_enhanced_trade_chart_data(
                filtered_df=filtered_df,
                optimized_chart_data=optimized_chart_data,
                external_context={"news": {"status": "ok"}},
            )

        latest_entry = result["advice_entries"][-1]
        self.assertEqual(latest_entry["action"], "watch_buy")
        self.assertEqual(latest_entry["title"], "优化观察")
        self.assertEqual(latest_entry["summary"], "等待更好的入场点。")
        self.assertEqual(latest_entry["reason"], "趋势转暖，但不追高。")
        self.assertTrue(latest_entry["is_signal"])
        self.assertEqual(latest_entry["enhancement_action"], "observe")
        self.assertEqual(latest_entry["enhancement_title"], "优化观望")
        self.assertEqual(latest_entry["enhancement_score"], -3)

    def test_extract_next_trade_plan_from_chart_data_maps_latest_advice(self) -> None:
        plan = extract_next_trade_plan_from_chart_data(
            {
                "advice_entries": [
                    {
                        "date": "2026-05-19",
                        "action": "watch_buy",
                        "summary": "趋势转暖，但仍需等更好的入场点。",
                        "reason": "长线趋势不差，但当前还没同时满足低吸位置与动量确认，先观察。",
                    }
                ]
            }
        )

        self.assertEqual(plan["action"], "watch_buy")
        self.assertEqual(plan["display_action"], "观察买点")
        self.assertIn("2026-05-19", plan["summary"])
        self.assertIn("低吸位置", plan["reason"])

    def test_build_next_trade_plan_can_generate_hold_bias(self) -> None:
        dates = pd.date_range("2026-04-01", periods=40, freq="D")
        rows = []
        for index, day in enumerate(dates, start=1):
            close = 10 + index * 0.06
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "open": round(close - 0.05, 4),
                    "high": round(close + 0.08, 4),
                    "low": round(close - 0.08, 4),
                    "close": round(close, 4),
                    "volume": 100000 + index * 1000,
                    "turn": 1.0 + index * 0.01,
                }
            )
        df = pd.DataFrame(rows)

        plan = build_next_trade_plan(
            source_df=df,
            config={
                "from_date": "2026-04-01",
                "to_date": "2026-05-10",
                "fast": 8,
                "slow": 20,
                "stop_loss_pct": 0.1,
            },
            ma_periods=[8, 20],
        )

        self.assertEqual(plan["action"], "hold")
        self.assertEqual(plan["display_action"], "偏持有")
        self.assertIn("下一交易日", plan["title"])

    def test_build_empty_entry_timing_plan_returns_entry_window_hint(self) -> None:
        dates = pd.date_range("2026-04-01", periods=40, freq="D")
        rows = []
        for index, day in enumerate(dates, start=1):
            close = 10 + index * 0.04
            rows.append(
                {
                    "date": day.strftime("%Y-%m-%d"),
                    "open": round(close - 0.05, 4),
                    "high": round(close + 0.08, 4),
                    "low": round(close - 0.08, 4),
                    "close": round(close, 4),
                    "volume": 100000 + index * 1000,
                    "turn": 1.0 + index * 0.01,
                }
            )
        df = pd.DataFrame(rows)

        timing = build_empty_entry_timing_plan(
            source_df=df,
            config={
                "from_date": "2026-04-01",
                "to_date": "2026-05-10",
                "fast": 8,
                "slow": 20,
                "stop_loss_pct": 0.1,
            },
            ma_periods=[8, 20],
        )

        self.assertIn("label", timing)
        self.assertIn("summary", timing)
        self.assertIn("reference", timing)

    def test_build_next_trade_plan_by_position_distinguishes_empty_and_hold(self) -> None:
        chart_data = {
            "advice_entries": [
                {
                    "date": "2026-05-19",
                    "action": "watch_buy",
                    "summary": "趋势转暖，但仍需等更好的入场点。",
                    "reason": "长线趋势不差，但当前还没同时满足低吸位置与动量确认，先观察。",
                }
            ]
        }

        empty_plan = extract_next_trade_plan_from_chart_data(
            chart_data,
            current_position="empty",
        )
        hold_plan = extract_next_trade_plan_from_chart_data(
            chart_data,
            current_position="hold",
        )

        self.assertEqual(empty_plan["action"], "watch_buy")
        self.assertEqual(hold_plan["action"], "hold")
        self.assertIn("继续持有观察", hold_plan["reason"])

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
            ex_right_closes=[9.8, 10.6],
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

    def test_advice_panel_keeps_enhanced_source_available_but_defaults_to_strategy(self) -> None:
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
                            "ex_right_price": "10.60",
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

        self.assertIn('data-advice-source="strategy"', html)
        self.assertIn('data-advice-source="optimized"', html)
        self.assertIn('data-default-advice-source="strategy"', html)
        self.assertIn("优化策略", html)
        self.assertIn("优化观察", html)

    def test_advice_panel_defaults_to_main_strategy_even_after_external_patch(self) -> None:
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
                            "action": "observe",
                            "title": "优化观望",
                            "price": "11.00",
                            "summary": "外部因子明显转弱，暂缓偏多信号。",
                            "reason": "新闻面偏谨慎，资金面偏流出。",
                            "is_signal": False,
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

        self.assertNotIn('data-advice-source="enhanced"', html)
        self.assertIn('data-advice-source="strategy"', html)
        self.assertIn('data-advice-source="optimized"', html)
        self.assertIn('data-default-advice-source="strategy"', html)
        self.assertIn("优化策略", html)
        self.assertIn("优化观望", html)

    def test_next_trade_plan_card_uses_externally_patched_optimized_source(self) -> None:
        report_data = [
            {
                "chart_name": "优化买卖点",
                "chart_data": {
                    "x_axis": ["2026-05-19"],
                    "candles": [[10.0, 10.1, 9.9, 10.2]],
                    "buy_points": [],
                    "sell_points": [],
                    "indicator_lines": [],
                    "advice_entries": [
                        {
                            "date": "2026-05-19",
                            "action": "observe",
                            "title": "优化观望",
                            "summary": "外部因子明显转弱，暂缓偏多信号。",
                            "reason": "新闻面偏谨慎，资金面偏流出。",
                            "is_signal": False,
                        }
                    ],
                },
            },
        ]

        html = _build_next_trade_plan_card(report_data)

        self.assertIn("优化观望", html)
        self.assertIn("暂缓偏多信号", html)

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

    def test_html_report_renders_next_trade_plan_card(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-19"],
            buy_points=[],
            sell_points=[],
        )
        report_data.append(
            {
                "chart_name": "优化买卖点",
                "chart_data": {
                    "x_axis": ["2026-05-19"],
                    "candles": [[10.0, 10.2, 9.9, 10.3]],
                    "buy_points": [],
                    "sell_points": [],
                    "indicator_lines": [],
                    "advice_entries": [
                        {
                            "date": "2026-05-19",
                            "action": "watch_buy",
                            "title": "优化观察",
                            "summary": "趋势转暖，但仍需等更好的入场点。",
                            "reason": "长线趋势不差，但当前还没同时满足低吸位置与动量确认，先观察。",
                            "is_signal": True,
                        }
                    ],
                },
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "report.html"
            generate_backtest_html(
                report_data=report_data,
                output_path=str(output_path),
                benchmarks=[],
                title="测试回测报告",
            )
            html = output_path.read_text(encoding="utf-8")

        self.assertIn("当前生成时的购买策略", html)
        self.assertIn("当日股价：10.20", html)
        self.assertIn("如果你当前空仓", html)
        self.assertIn("如果你当前持仓", html)
        self.assertIn("含策略预判", html)

    def test_html_report_can_embed_ai_report_into_single_file(self) -> None:
        report_data = build_buy_sell_report(
            dates=["2026-05-13", "2026-05-14"],
            buy_points=[["2026-05-13", 10.0]],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "report.html"
            ai_report_path = Path(temp_dir) / "ai.html"
            ai_report_path.write_text(
                """<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>AI 页</title></head>
<body><div>AI 结论：趋势策略有效，但震荡市需谨慎。</div></body>
</html>
""",
                encoding="utf-8",
            )
            generate_backtest_html(
                report_data=report_data,
                output_path=str(output_path),
                benchmarks=[],
                title="测试回测报告",
                ai_report_link="../llm_analysis/test-ai-report.html",
                ai_report_path=str(ai_report_path),
            )
            html = output_path.read_text(encoding="utf-8")

        self.assertIn('href="#ai-analysis-section"', html)
        self.assertIn('id="ai-analysis-section"', html)
        self.assertIn('class="embedded-ai-iframe"', html)
        self.assertIn("查看独立 AI 页", html)
        self.assertIn("AI 结论：趋势策略有效，但震荡市需谨慎。", html)

    def test_merge_existing_backtest_and_ai_html_into_share_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            backtest_report_path = temp_path / "backtest.html"
            ai_report_path = temp_path / "ai.html"
            output_path = temp_path / "backtest-share.html"

            backtest_report_path.write_text(
                """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <style>
    .page-header-ai-link { color: #fff; }
  </style>
</head>
<body>
  <div class="container">
    <header class="page-header">
      <div class="page-header-title">
        <h1>测试回测报告</h1>
        <a class="page-header-ai-link" href="../llm_analysis/ai.html">AI</a>
      </div>
    </header>
  </div>
  <script>
    window.__BTReport = {};
  </script>
</body>
</html>
""",
                encoding="utf-8",
            )
            ai_report_path.write_text(
                """<!DOCTYPE html>
<html lang="zh-CN">
<body><div>AI 分析内容：可直接分享。</div></body>
</html>
""",
                encoding="utf-8",
            )

            merged_path = merge_backtest_html_with_ai_report(
                str(backtest_report_path),
                str(ai_report_path),
                str(output_path),
            )
            html = merged_path.read_text(encoding="utf-8")

        self.assertEqual(merged_path, output_path)
        self.assertIn('href="#ai-analysis-section"', html)
        self.assertIn('id="ai-analysis-section"', html)
        self.assertIn("AI 分析内容：可直接分享。", html)
        self.assertIn("resizeEmbeddedAIFrames", html)
        self.assertIn(".embedded-ai-section", html)


if __name__ == "__main__":
    unittest.main()

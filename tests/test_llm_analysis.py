import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import io
from contextlib import redirect_stdout

import pandas as pd

from utils.config import DEFAULT_A_SHARE_ADJUST
from analysis.config import LLMAnalysisSettings, load_llm_analysis_settings
from analysis.payload_builder import (
    build_batch_analysis_payload,
    build_single_stock_analysis_payload,
)
from analysis.context_enricher import enrich_single_stock_context
from analysis.service import (
    maybe_generate_batch_analysis,
    maybe_generate_single_stock_analysis,
)


def _build_sample_price_df() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=90, freq="D")
    rows: list[dict] = []
    for index, day in enumerate(dates, start=1):
        close = 10 + index * 0.1
        rows.append(
            {
                "date": day.strftime("%Y-%m-%d"),
                "open": round(close - 0.1, 4),
                "high": round(close + 0.2, 4),
                "low": round(close - 0.2, 4),
                "close": round(close, 4),
                "volume": 100000 + index * 1000,
                "turn": 1.0 + index * 0.01,
            }
        )
    return pd.DataFrame(rows)


class LLMAnalysisTests(unittest.TestCase):
    def test_load_settings_defaults_to_deepseek_provider(self) -> None:
        env = {
            "QT_ENABLE_LLM_ANALYSIS": "1",
            "QT_LLM_PROVIDER": "deepseek",
            "QT_LLM_DEEPSEEK_API_KEY": "test-deepseek-key",
        }
        with patch.dict("os.environ", env, clear=True):
            settings = load_llm_analysis_settings()

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.provider, "deepseek")
        self.assertEqual(settings.base_url, "https://api.deepseek.com")
        self.assertEqual(settings.model, "deepseek-chat")
        self.assertEqual(settings.api_key, "test-deepseek-key")

    def test_load_settings_allows_switching_provider_and_model(self) -> None:
        env = {
            "QT_ENABLE_LLM_ANALYSIS": "1",
            "QT_LLM_PROVIDER": "openai",
            "QT_LLM_OPENAI_API_KEY": "test-openai-key",
            "QT_LLM_MODEL": "gpt-5",
        }
        with patch.dict("os.environ", env, clear=True):
            settings = load_llm_analysis_settings()

        self.assertEqual(settings.provider, "openai")
        self.assertEqual(settings.base_url, "https://api.openai.com/v1")
        self.assertEqual(settings.model, "gpt-5")
        self.assertEqual(settings.api_key, "test-openai-key")

    def test_build_single_stock_analysis_payload_contains_snapshot(self) -> None:
        payload = build_single_stock_analysis_payload(
            config={
                "code": "sz.000725",
                "report_name": "base_backtest",
                "strategy_name": "普通双均线",
                "strategy_brief": "基础版",
                "adjust_flag": DEFAULT_A_SHARE_ADJUST,
                "from_date": "2024-01-01",
                "to_date": "2024-03-30",
                "fast": 8,
                "slow": 250,
                "cash": 100000.0,
            },
            summary={
                "annual_return_pct": 18.3,
                "max_drawdown_pct": 9.5,
                "sharpe_ratio": 1.2,
                "next_trade_plan": {
                    "action": "watch_buy",
                    "display_action": "观察买点",
                    "summary": "基于 2024-03-30 收盘后的趋势结构，趋势转暖，但仍需等更好的入场点。",
                },
            },
            df=_build_sample_price_df(),
        )

        self.assertEqual(payload["task_type"], "single_stock_backtest_analysis")
        self.assertEqual(payload["asset"]["code"], "sz.000725")
        self.assertEqual(payload["strategy"]["parameters"]["fast"], 8)
        self.assertIn("market_snapshot", payload)
        self.assertIsNotNone(payload["market_snapshot"]["return_20d_pct"])
        self.assertIsNotNone(payload["market_snapshot"]["ma20"])
        self.assertEqual(
            payload["performance_summary"]["next_trade_plan"]["action"],
            "watch_buy",
        )

    def test_build_single_stock_analysis_payload_can_include_external_context(self) -> None:
        payload = build_single_stock_analysis_payload(
            config={
                "code": "sz.000725",
                "report_name": "base_backtest",
                "strategy_name": "普通双均线",
                "strategy_brief": "基础版",
                "adjust_flag": DEFAULT_A_SHARE_ADJUST,
                "from_date": "2024-01-01",
                "to_date": "2024-03-30",
                "fast": 8,
                "slow": 250,
                "cash": 100000.0,
            },
            summary={
                "annual_return_pct": 18.3,
                "max_drawdown_pct": 9.5,
                "sharpe_ratio": 1.2,
            },
            df=_build_sample_price_df(),
            external_context={
                "news": {"status": "ok", "items": [{"title": "面板价格回升"}]},
                "fund_flow": {"status": "ok", "main_net_inflow_5d": 123.4},
            },
        )

        self.assertIn("external_context", payload)
        self.assertEqual(payload["external_context"]["news"]["status"], "ok")
        self.assertEqual(
            payload["external_context"]["fund_flow"]["main_net_inflow_5d"],
            123.4,
        )

    def test_enrich_single_stock_context_filters_future_news_and_summarizes_sources(self) -> None:
        sample_df = _build_sample_price_df()
        sample_df = sample_df[sample_df["date"] <= "2024-03-30"].reset_index(drop=True)

        news_df = pd.DataFrame(
            [
                {
                    "发布时间": "2024-03-29 10:00:00",
                    "文章来源": "东方财富",
                    "新闻标题": "公司发布新产品",
                    "新闻链接": "https://example.com/news-1",
                },
                {
                    "发布时间": "2024-04-02 09:00:00",
                    "文章来源": "东方财富",
                    "新闻标题": "回测结束后的新闻",
                    "新闻链接": "https://example.com/news-2",
                },
            ]
        )
        fund_flow_df = pd.DataFrame(
            [
                {
                    "日期": "2024-03-28",
                    "主力净流入-净额": "1000",
                    "主力净流入-净占比": "1.2",
                    "超大单净流入-净占比": "0.8",
                    "大单净流入-净占比": "0.4",
                },
                {
                    "日期": "2024-03-29",
                    "主力净流入-净额": "2000",
                    "主力净流入-净占比": "2.3",
                    "超大单净流入-净占比": "1.1",
                    "大单净流入-净占比": "0.6",
                },
            ]
        )
        abstract_df = pd.DataFrame(
            [
                {
                    "报告期": "2023-12-31",
                    "营业总收入同比": "12.5",
                    "净利润同比": "-5.2",
                    "净利润": "321000000",
                }
            ]
        )
        indicator_df = pd.DataFrame(
            [
                {
                    "日期": "2023-12-31",
                    "净资产收益率(%)": "9.8",
                    "销售毛利率(%)": "18.6",
                    "资产负债率(%)": "42.1",
                    "每股经营性现金流(元)": "0.56",
                }
            ]
        )

        with patch("analysis.context_enricher.ak.stock_news_em", return_value=news_df):
            with patch(
                "analysis.context_enricher.ak.stock_individual_fund_flow",
                return_value=fund_flow_df,
            ):
                with patch(
                    "analysis.context_enricher.ak.stock_financial_abstract",
                    return_value=abstract_df,
                ):
                    with patch(
                        "analysis.context_enricher.ak.stock_financial_analysis_indicator",
                        return_value=indicator_df,
                    ):
                        context = enrich_single_stock_context(
                            {"code": "sz.000725", "to_date": "2024-03-30"},
                            sample_df,
                        )

        self.assertEqual(context["as_of_date"], "2024-03-30")
        self.assertEqual(context["news"]["status"], "ok")
        self.assertEqual(len(context["news"]["items"]), 1)
        self.assertEqual(context["news"]["items"][0]["title"], "公司发布新产品")
        self.assertEqual(context["fund_flow"]["main_net_inflow_3d"], 3000.0)
        self.assertEqual(context["fund_flow"]["main_net_inflow_5d"], 3000.0)
        self.assertEqual(context["financials"]["report_date"], "2023-12-31")
        self.assertEqual(context["financials"]["revenue_yoy_pct"], 12.5)
        self.assertEqual(context["financials"]["roe_pct"], 9.8)

    def test_build_batch_analysis_payload_ranks_best_candidate_first(self) -> None:
        payload = build_batch_analysis_payload(
            strategy_id="base_backtest",
            strategy_name="普通双均线",
            batch_results=[
                {
                    "code": "sz.000725",
                    "annual_return_pct": 12.0,
                    "total_return_pct": 20.0,
                    "max_drawdown_pct": 8.0,
                    "sharpe_ratio": 1.1,
                    "win_rate_pct": 55.0,
                    "net_profit": 8000.0,
                    "trades_total": 8,
                },
                {
                    "code": "sz.000100",
                    "annual_return_pct": 18.0,
                    "total_return_pct": 28.0,
                    "max_drawdown_pct": 7.0,
                    "sharpe_ratio": 1.3,
                    "win_rate_pct": 60.0,
                    "net_profit": 12000.0,
                    "trades_total": 10,
                },
            ],
        )

        self.assertEqual(payload["task_type"], "batch_backtest_analysis")
        self.assertEqual(payload["candidates"][0]["code"], "sz.000100")
        self.assertEqual(payload["batch_summary"]["sample_size"], 2)

    def test_single_analysis_writes_markdown_report(self) -> None:
        config = {
            "code": "sz.000725",
                "report_name": "base_backtest",
            "strategy_name": "普通双均线",
            "strategy_brief": "基础版",
            "adjust_flag": DEFAULT_A_SHARE_ADJUST,
            "from_date": "2024-01-01",
            "to_date": "2024-03-30",
            "fast": 8,
            "slow": 250,
            "cash": 100000.0,
            "enable_llm_analysis": True,
        }
        summary = {
            "annual_return_pct": 18.3,
            "max_drawdown_pct": 9.5,
            "sharpe_ratio": 1.2,
        }
        fake_result = {
            "score": 82,
            "conclusion": "策略表现较稳健，但趋势依赖较强。",
            "strengths": ["收益风险比尚可", "回撤可控"],
            "risks": ["样本数量有限", "震荡市可能失效"],
            "regime_fit": "更适合中期趋势缓慢抬升的行情。",
            "next_action": "继续扩大样本并做滚动窗口验证。",
            "confidence": 74,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "single.html"
            fake_settings = LLMAnalysisSettings(
                enabled=True,
                provider="deepseek",
                api_key="test-key",
                base_url="https://api.deepseek.com",
                model="deepseek-chat",
                timeout_seconds=60,
                temperature=0.2,
            )
            with patch(
                "analysis.service._request_analysis_result",
                return_value=(fake_settings, fake_result),
            ):
                with patch(
                    "analysis.service.enrich_single_stock_context",
                    return_value={
                        "news": {"status": "ok", "items": [{"title": "面板价格回升"}]},
                        "fund_flow": {"status": "ok", "main_net_inflow_5d": 12345.6},
                    },
                ):
                    with patch("analysis.service._build_single_report_path", return_value=output_path):
                        report_path = maybe_generate_single_stock_analysis(
                            config=config,
                            summary=summary,
                            df=_build_sample_price_df(),
                        )

            self.assertEqual(report_path, output_path)
            self.assertTrue(output_path.exists())
            content = output_path.read_text(encoding="utf-8")
            self.assertIn("普通双均线 sz.000725 大模型分析报告", content)
            self.assertIn("<!DOCTYPE html>", content)
            self.assertIn("模型提供方：deepseek", content)
            self.assertIn("模型名称：deepseek-chat", content)
            self.assertIn("策略表现较稳健", content)
            self.assertIn("收益风险比尚可", content)

    def test_single_analysis_passes_external_context_into_request_payload(self) -> None:
        config = {
            "code": "sz.000725",
            "report_name": "base_backtest",
            "strategy_name": "普通双均线",
            "strategy_brief": "基础版",
            "adjust_flag": DEFAULT_A_SHARE_ADJUST,
            "from_date": "2024-01-01",
            "to_date": "2024-03-30",
            "fast": 8,
            "slow": 250,
            "cash": 100000.0,
            "enable_llm_analysis": True,
        }
        summary = {
            "annual_return_pct": 18.3,
            "max_drawdown_pct": 9.5,
            "sharpe_ratio": 1.2,
        }
        fake_result = {
            "score": 82,
            "conclusion": "策略表现较稳健，但趋势依赖较强。",
            "strengths": ["收益风险比尚可", "回撤可控"],
            "risks": ["样本数量有限", "震荡市可能失效"],
            "regime_fit": "更适合中期趋势缓慢抬升的行情。",
            "next_action": "继续扩大样本并做滚动窗口验证。",
            "confidence": 74,
        }
        fake_settings = LLMAnalysisSettings(
            enabled=True,
            provider="deepseek",
            api_key="test-key",
            base_url="https://api.deepseek.com",
            model="deepseek-chat",
            timeout_seconds=60,
            temperature=0.2,
        )
        captured_payloads: list[dict] = []

        def _fake_request_analysis_result(*, payload: dict, task_title: str):
            captured_payloads.append(payload)
            return fake_settings, fake_result

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "single.html"
            with patch(
                "analysis.service.enrich_single_stock_context",
                return_value={
                    "news": {"status": "ok", "items": [{"title": "面板价格回升"}]},
                    "fund_flow": {"status": "ok", "main_net_inflow_5d": 12345.6},
                    "financials": {"status": "ok", "report_date": "2023-12-31"},
                },
            ):
                with patch(
                    "analysis.service._request_analysis_result",
                    side_effect=_fake_request_analysis_result,
                ):
                    with patch("analysis.service._build_single_report_path", return_value=output_path):
                        maybe_generate_single_stock_analysis(
                            config=config,
                            summary=summary,
                            df=_build_sample_price_df(),
                        )

        self.assertEqual(len(captured_payloads), 1)
        self.assertIn("external_context", captured_payloads[0])
        self.assertEqual(
            captured_payloads[0]["external_context"]["financials"]["report_date"],
            "2023-12-31",
        )

    def test_batch_analysis_skips_when_disabled(self) -> None:
        report_path = maybe_generate_batch_analysis(
            strategy_id="base_backtest",
            strategy_name="普通双均线",
            batch_results=[
                {
                    "code": "sz.000725",
                    "annual_return_pct": 12.0,
                    "enable_llm_analysis": False,
                }
            ],
        )
        self.assertIsNone(report_path)

    def test_single_analysis_failure_does_not_raise_and_writes_failure_report(self) -> None:
        config = {
            "code": "sz.000100",
            "report_name": "tcl_simple_ma_backtest",
            "strategy_name": "TCL双均线专版",
            "strategy_brief": "稳健增强版",
            "adjust_flag": DEFAULT_A_SHARE_ADJUST,
            "from_date": "2024-01-01",
            "to_date": "2024-03-30",
            "fast": 10,
            "slow": 250,
            "cash": 100000.0,
            "enable_llm_analysis": True,
        }
        summary = {
            "annual_return_pct": 15.2,
            "max_drawdown_pct": 11.1,
            "sharpe_ratio": 0.9,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "single.html"
            stdout_buffer = io.StringIO()
            with patch(
                "analysis.service._request_analysis_result",
                side_effect=RuntimeError("大模型分析请求失败: HTTP 402 余额不足，详情: Insufficient Balance"),
            ):
                with patch("analysis.service._build_single_report_path", return_value=output_path):
                    with redirect_stdout(stdout_buffer):
                        report_path = maybe_generate_single_stock_analysis(
                            config=config,
                            summary=summary,
                            df=_build_sample_price_df(),
                        )

            failure_path = output_path.with_suffix(".failed.html")
            self.assertEqual(report_path, failure_path)
            self.assertTrue(failure_path.exists())
            content = failure_path.read_text(encoding="utf-8")
            self.assertIn("<!DOCTYPE html>", content)
            self.assertIn("失败报告", content)
            self.assertIn("余额不足", content)
            self.assertIn("回测主流程已经完成", content)
            stdout_text = stdout_buffer.getvalue()
            self.assertIn("AI 分析失败", stdout_text)
            self.assertIn(str(failure_path), stdout_text)


if __name__ == "__main__":
    unittest.main()

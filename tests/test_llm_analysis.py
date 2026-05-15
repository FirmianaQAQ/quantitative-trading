import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import io
from contextlib import redirect_stdout

import pandas as pd

from analysis.config import LLMAnalysisSettings, load_llm_analysis_settings
from analysis.payload_builder import (
    build_batch_analysis_payload,
    build_single_stock_analysis_payload,
)
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
                "report_name": "simple_ma_backtest",
                "strategy_name": "普通双均线",
                "strategy_brief": "基础版",
                "adjust_flag": "hfq",
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
        )

        self.assertEqual(payload["task_type"], "single_stock_backtest_analysis")
        self.assertEqual(payload["asset"]["code"], "sz.000725")
        self.assertEqual(payload["strategy"]["parameters"]["fast"], 8)
        self.assertIn("market_snapshot", payload)
        self.assertIsNotNone(payload["market_snapshot"]["return_20d_pct"])
        self.assertIsNotNone(payload["market_snapshot"]["ma20"])

    def test_build_batch_analysis_payload_ranks_best_candidate_first(self) -> None:
        payload = build_batch_analysis_payload(
            strategy_id="simple_ma_backtest_v2",
            strategy_name="普通双均线V2",
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
            "report_name": "simple_ma_backtest",
            "strategy_name": "普通双均线",
            "strategy_brief": "基础版",
            "adjust_flag": "hfq",
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

    def test_batch_analysis_skips_when_disabled(self) -> None:
        report_path = maybe_generate_batch_analysis(
            strategy_id="simple_ma_backtest_v2",
            strategy_name="普通双均线V2",
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
            "adjust_flag": "hfq",
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

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from sync.sync_akshare import (
    STORAGE_COLUMNS,
    build_unified_history_df,
    attach_ex_right_close_for_sync,
    build_eastmoney_cookie_chrome_applescript,
    fetch_stock_history_from_ths,
    compute_incremental_start_date,
    fetch_index_history,
    fetch_index_history_from_eastmoney,
    fetch_stock_history,
    fetch_stock_history_from_eastmoney,
    merge_and_save_history,
    normalize_sina_stock_history_df,
    parse_cli_options,
    refresh_eastmoney_cookie,
    refresh_eastmoney_cookie_from_chrome,
    refresh_eastmoney_cookie_from_chrome_profile,
    resolve_effective_adjust_flag,
    resolve_eastmoney_cookie_bootstrap_urls,
    should_full_refresh_history,
    to_akshare_adjust_flag,
    to_baostock_adjust_flag,
    to_ths_adjust_flag,
)
from utils.project_utils import load_daily_data


def build_row(trade_date: str, close_price: float) -> dict:
    return {
        "date": trade_date,
        "code": "sz.000725",
        "open": close_price,
        "high": close_price,
        "low": close_price,
        "close": close_price,
        "ex_right_close": pd.NA,
        "preclose": pd.NA,
        "volume": 100,
        "amount": 1000,
        "adjustflag": "hfq",
        "turn": 1.0,
        "pctChg": 0.0,
        "peTTM": pd.NA,
        "psTTM": pd.NA,
        "pcfNcfTTM": pd.NA,
        "pbMRQ": pd.NA,
    }


class SyncAkshareHistoryTests(unittest.TestCase):
    def test_dynamic_pre_alias_maps_to_qfq_across_sync_helpers(self) -> None:
        self.assertEqual(resolve_effective_adjust_flag("dypre"), "qfq")
        self.assertEqual(to_baostock_adjust_flag("dypre"), "2")
        self.assertEqual(to_akshare_adjust_flag("dypre"), "qfq")
        self.assertEqual(to_ths_adjust_flag("dypre"), "01")

    def test_parse_cli_options_accepts_ths_source(self) -> None:
        with mock.patch(
            "sys.argv",
            ["sync_akshare.py", "--source=ths", "000100"],
        ):
            code_args, sync_all_sh_main, source_name = parse_cli_options()

        self.assertEqual(code_args, ["000100"])
        self.assertFalse(sync_all_sh_main)
        self.assertEqual(source_name, "ths")

    def test_resolve_eastmoney_cookie_bootstrap_urls_uses_multiple_candidates(self) -> None:
        urls = resolve_eastmoney_cookie_bootstrap_urls("sh.600580")

        self.assertEqual(urls[0], "https://quote.eastmoney.com/sh600580.html")
        self.assertIn("https://quote.eastmoney.com/", urls)
        self.assertIn("https://quote.eastmoney.com/center/gridlist.html", urls)
        self.assertIn("https://quote.eastmoney.com/concept/sz000014.html", urls)

    def test_refresh_eastmoney_cookie_returns_empty_cookie_when_bootstrap_pages_have_no_cookie(self) -> None:
        response_mock = mock.Mock()
        response_mock.raise_for_status.return_value = None

        with (
            mock.patch(
                "sync.sync_akshare.refresh_eastmoney_cookie_from_chrome",
                side_effect=RuntimeError("chrome down"),
            ),
            mock.patch(
                "sync.sync_akshare.retry_request_call",
                return_value=response_mock,
            ) as retry_mock,
            mock.patch(
                "sync.sync_akshare.format_cookie_header",
                return_value="",
            ),
            mock.patch("sync.sync_akshare.write_eastmoney_cookie") as write_cookie_mock,
        ):
            session, cookie = refresh_eastmoney_cookie(
                ["https://quote.eastmoney.com/sh600580.html"]
            )

        self.assertEqual(cookie, "")
        self.assertIsNotNone(session)
        self.assertEqual(retry_mock.call_count, 1)
        write_cookie_mock.assert_not_called()

    def test_refresh_eastmoney_cookie_prefers_chrome_cookie(self) -> None:
        with (
            mock.patch(
                "sync.sync_akshare.refresh_eastmoney_cookie_from_chrome",
                return_value="foo=bar",
            ) as chrome_mock,
            mock.patch("sync.sync_akshare.retry_request_call") as retry_mock,
        ):
            session, cookie = refresh_eastmoney_cookie(
                ["https://quote.eastmoney.com/sh600580.html"]
            )

        self.assertEqual(cookie, "foo=bar")
        self.assertIsNotNone(session)
        chrome_mock.assert_called_once()
        retry_mock.assert_not_called()

    def test_refresh_eastmoney_cookie_from_chrome_profile_writes_cookie(self) -> None:
        cookie_jar = [
            type("Cookie", (), {"name": "foo", "value": "bar"})(),
            type("Cookie", (), {"name": "baz", "value": "qux"})(),
        ]

        with (
            mock.patch("sync.sync_akshare.browser_cookie3") as browser_cookie3_mock,
            mock.patch("sync.sync_akshare.write_eastmoney_cookie") as write_cookie_mock,
        ):
            browser_cookie3_mock.chrome.return_value = cookie_jar
            cookie = refresh_eastmoney_cookie_from_chrome_profile()

        self.assertEqual(cookie, "foo=bar; baz=qux")
        browser_cookie3_mock.chrome.assert_called_once_with(domain_name=".eastmoney.com")
        write_cookie_mock.assert_called_once_with("foo=bar; baz=qux")

    def test_refresh_eastmoney_cookie_from_chrome_writes_cookie(self) -> None:
        process_result = mock.Mock(returncode=0, stdout="foo=bar; baz=qux\n", stderr="")

        with (
            mock.patch(
                "sync.sync_akshare.refresh_eastmoney_cookie_from_chrome_profile",
                side_effect=RuntimeError("profile down"),
            ),
            mock.patch("sync.sync_akshare.sys.platform", "darwin"),
            mock.patch("sync.sync_akshare.shutil.which", return_value="/usr/bin/osascript"),
            mock.patch(
                "sync.sync_akshare.subprocess.run",
                return_value=process_result,
            ) as run_mock,
            mock.patch("sync.sync_akshare.write_eastmoney_cookie") as write_cookie_mock,
        ):
            cookie = refresh_eastmoney_cookie_from_chrome()

        self.assertEqual(cookie, "foo=bar; baz=qux")
        self.assertIn("Google Chrome", run_mock.call_args.kwargs["input"])
        self.assertIn("https://quote.eastmoney.com/", run_mock.call_args.kwargs["input"])
        write_cookie_mock.assert_called_once_with("foo=bar; baz=qux")

    def test_build_eastmoney_cookie_chrome_applescript_contains_target(self) -> None:
        script = build_eastmoney_cookie_chrome_applescript(
            chrome_app_name='Google "Chrome"',
            target_url="https://quote.eastmoney.com/",
        )

        self.assertIn('set chromeAppName to "Google \\"Chrome\\""', script)
        self.assertIn('set targetURL to "https://quote.eastmoney.com/"', script)
        self.assertIn('using terms from application "Google Chrome"', script)
        self.assertIn("open location targetURL", script)
        self.assertIn(
            'set cookieText to execute javascript "document.cookie" in active tab of targetWindow',
            script,
        )

    def test_compute_incremental_start_date_reloads_latest_day(self) -> None:
        existing_df = pd.DataFrame(
            [
                {"date": "2026-05-12"},
                {"date": "2026-05-13"},
            ]
        )

        result = compute_incremental_start_date(existing_df, "2020-01-01")

        self.assertEqual(result, "2026-05-13")

    def test_should_full_refresh_history(self) -> None:
        self.assertTrue(should_full_refresh_history("hfq"))
        self.assertTrue(should_full_refresh_history("qfq"))
        self.assertFalse(should_full_refresh_history("cq"))
        self.assertFalse(should_full_refresh_history("bfq"))

    def test_build_unified_history_df_merges_cq_qfq_hfq_into_single_file_shape(self) -> None:
        cq_df = pd.DataFrame(
            [build_row("2026-05-13", 10.0)],
            columns=STORAGE_COLUMNS,
        )
        qfq_df = pd.DataFrame(
            [build_row("2026-05-13", 11.0)],
            columns=STORAGE_COLUMNS,
        )
        hfq_df = pd.DataFrame(
            [build_row("2026-05-13", 12.0)],
            columns=STORAGE_COLUMNS,
        )
        cq_df["adjustflag"] = "cq"
        qfq_df["adjustflag"] = "qfq"
        hfq_df["adjustflag"] = "hfq"
        cq_df["ex_right_close"] = 10.0
        qfq_df["ex_right_close"] = 10.0
        hfq_df["ex_right_close"] = 10.0

        result = build_unified_history_df(
            "sz.000725",
            {"cq": cq_df, "qfq": qfq_df, "hfq": hfq_df},
        )

        self.assertEqual(result["code"].tolist(), ["sz.000725"])
        self.assertEqual(result["cq_close"].tolist(), [10.0])
        self.assertEqual(result["qfq_close"].tolist(), [11.0])
        self.assertEqual(result["hfq_close"].tolist(), [12.0])

    def test_load_daily_data_reads_selected_adjustment_columns_from_unified_csv(self) -> None:
        unified_df = pd.DataFrame(
            [
                {
                    "date": "2026-05-13",
                    "code": "sz.000725",
                    "cq_open": 10.0,
                    "cq_high": 10.5,
                    "cq_low": 9.8,
                    "cq_close": 10.2,
                    "cq_ex_right_close": 10.2,
                    "cq_preclose": 10.0,
                    "cq_volume": 1000,
                    "cq_amount": 10000,
                    "cq_adjustflag": "cq",
                    "cq_turn": 1.2,
                    "cq_pctChg": 2.0,
                    "qfq_open": 8.0,
                    "qfq_high": 8.5,
                    "qfq_low": 7.8,
                    "qfq_close": 8.2,
                    "qfq_ex_right_close": 10.2,
                    "qfq_preclose": 8.0,
                    "qfq_volume": 1000,
                    "qfq_amount": 10000,
                    "qfq_adjustflag": "qfq",
                    "qfq_turn": 1.2,
                    "qfq_pctChg": 2.0,
                    "hfq_open": 18.0,
                    "hfq_high": 18.5,
                    "hfq_low": 17.8,
                    "hfq_close": 18.2,
                    "hfq_ex_right_close": 10.2,
                    "hfq_preclose": 18.0,
                    "hfq_volume": 1000,
                    "hfq_amount": 10000,
                    "hfq_adjustflag": "hfq",
                    "hfq_turn": 1.2,
                    "hfq_pctChg": 2.0,
                }
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sz.000725.csv"
            unified_df.to_csv(csv_path, index=False)
            with mock.patch(
                "utils.project_utils.get_daily_csv_path",
                return_value=csv_path,
            ):
                qfq_df = load_daily_data("sz.000725", "qfq")

        self.assertEqual(qfq_df["close"].tolist(), [8.2])
        self.assertEqual(qfq_df["ex_right_close"].tolist(), [10.2])

    def test_merge_and_save_history_full_refresh_replaces_existing_history(self) -> None:
        existing_df = pd.DataFrame(
            [
                build_row("2026-05-12", 10.0),
                build_row("2026-05-13", 11.0),
            ],
            columns=STORAGE_COLUMNS,
        )
        new_df = pd.DataFrame(
            [
                build_row("2026-05-13", 21.0),
                build_row("2026-05-14", 22.0),
            ],
            columns=STORAGE_COLUMNS,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "daily.csv"
            written = merge_and_save_history(
                csv_path,
                existing_df,
                new_df,
                full_refresh=True,
            )
            saved_df = pd.read_csv(csv_path)

        self.assertEqual(written, 2)
        self.assertEqual(saved_df["date"].tolist(), ["2026-05-13", "2026-05-14"])
        self.assertEqual(saved_df["close"].tolist(), [21.0, 22.0])

    def test_merge_and_save_history_incremental_overwrites_latest_day(self) -> None:
        existing_df = pd.DataFrame(
            [
                build_row("2026-05-13", 10.0),
                build_row("2026-05-14", 11.0),
            ],
            columns=STORAGE_COLUMNS,
        )
        new_df = pd.DataFrame(
            [
                build_row("2026-05-14", 12.0),
                build_row("2026-05-15", 13.0),
            ],
            columns=STORAGE_COLUMNS,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "daily.csv"
            written = merge_and_save_history(
                csv_path,
                existing_df,
                new_df,
                full_refresh=False,
            )
            saved_df = pd.read_csv(csv_path)

        self.assertEqual(written, 2)
        self.assertEqual(
            saved_df["date"].tolist(),
            ["2026-05-13", "2026-05-14", "2026-05-15"],
        )
        self.assertEqual(saved_df["close"].tolist(), [10.0, 12.0, 13.0])
        self.assertTrue(pd.isna(saved_df.loc[0, "preclose"]))
        self.assertEqual(saved_df.loc[1, "preclose"], 10.0)
        self.assertEqual(saved_df.loc[2, "preclose"], 12.0)

    def test_merge_and_save_history_backfills_ex_right_close_from_local_cq_csv(self) -> None:
        existing_df = pd.DataFrame(
            [
                build_row("2026-05-13", 10.0),
                build_row("2026-05-14", 11.0),
            ],
            columns=STORAGE_COLUMNS,
        )
        new_df = pd.DataFrame(
            [
                build_row("2026-05-15", 12.0),
            ],
            columns=STORAGE_COLUMNS,
        )

        cq_df = pd.DataFrame(
            [
                {"date": "2026-05-13", "close": 9.8},
                {"date": "2026-05-14", "close": 9.9},
                {"date": "2026-05-15", "close": 10.1},
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "daily.csv"
            cq_path = Path(temp_dir) / "cq.csv"
            cq_df.to_csv(cq_path, index=False)

            with mock.patch(
                "sync.sync_akshare.get_daily_csv_path",
                return_value=cq_path,
            ):
                merge_and_save_history(
                    csv_path,
                    existing_df,
                    new_df,
                    full_refresh=False,
                )
            saved_df = pd.read_csv(csv_path)

        self.assertEqual(saved_df["ex_right_close"].tolist(), [9.8, 9.9, 10.1])

    def test_attach_ex_right_close_for_sync_fetches_cq_close_for_adjusted_stock(self) -> None:
        history_df = pd.DataFrame(
            [
                build_row("2026-05-13", 10.0),
                build_row("2026-05-14", 11.0),
            ],
            columns=STORAGE_COLUMNS,
        )
        cq_df = pd.DataFrame(
            [
                build_row("2026-05-13", 9.8),
                build_row("2026-05-14", 10.1),
            ],
            columns=STORAGE_COLUMNS,
        )

        with (
            mock.patch(
                "sync.sync_akshare.read_local_ex_right_history",
                return_value=pd.DataFrame(columns=["date", "close"]),
            ),
            mock.patch(
                "sync.sync_akshare.fetch_stock_history",
                return_value=cq_df,
            ) as fetch_mock,
        ):
            result = attach_ex_right_close_for_sync(
                history_df,
                full_code="sz.000100",
                start_date="2026-05-13",
                end_date="2026-05-14",
                adjust_flag="hfq",
                source_name="eastmoney",
            )

        fetch_mock.assert_called_once_with(
            symbol="000100",
            start_date="2026-05-13",
            end_date="2026-05-14",
            adjust_flag="cq",
            source_name="eastmoney",
        )
        self.assertEqual(result["ex_right_close"].tolist(), [9.8, 10.1])

    def test_fetch_stock_history_falls_back_after_eastmoney_failure(self) -> None:
        fallback_df = pd.DataFrame([{"date": "2026-05-19", "close": 10.0}])

        with (
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_eastmoney",
                side_effect=RuntimeError("eastmoney down"),
            ) as eastmoney_mock,
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_ths",
                side_effect=RuntimeError("ths down"),
            ) as ths_mock,
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_baostock",
                return_value=fallback_df,
            ) as baostock_mock,
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_sina",
                return_value=fallback_df,
            ) as sina_mock,
            mock.patch("sync.sync_akshare.fetch_stock_history_from_tushare") as tushare_mock,
            mock.patch(
                "sync.sync_akshare.normalize_stock_history_df",
                side_effect=lambda raw_df, **_: raw_df,
            ),
            mock.patch(
                "sync.sync_akshare.normalize_sina_stock_history_df",
                side_effect=lambda raw_df, **_: raw_df,
            ),
        ):
            result = fetch_stock_history(
                symbol="600580",
                start_date="2026-05-01",
                end_date="2026-05-19",
                adjust_flag="hfq",
                source_name="eastmoney",
            )

        self.assertTrue(result.equals(fallback_df))
        eastmoney_mock.assert_called_once()
        ths_mock.assert_called_once()
        sina_mock.assert_called_once()
        baostock_mock.assert_not_called()
        tushare_mock.assert_not_called()

    def test_fetch_stock_history_uses_ths_when_requested(self) -> None:
        fallback_df = pd.DataFrame([{"date": "2026-05-19", "close": 10.0}])

        with (
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_ths",
                return_value=fallback_df,
            ) as ths_mock,
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_eastmoney",
            ) as eastmoney_mock,
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_sina",
            ) as sina_mock,
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_baostock",
            ) as baostock_mock,
            mock.patch(
                "sync.sync_akshare.normalize_ths_stock_history_df",
                side_effect=lambda raw_df, **_: raw_df,
            ),
        ):
            result = fetch_stock_history(
                symbol="000100",
                start_date="2026-05-01",
                end_date="2026-05-19",
                adjust_flag="hfq",
                source_name="ths",
            )

        self.assertTrue(result.equals(fallback_df))
        ths_mock.assert_called_once()
        eastmoney_mock.assert_not_called()
        sina_mock.assert_not_called()
        baostock_mock.assert_not_called()

    def test_fetch_stock_history_from_eastmoney_retries_with_empty_cookie_session(self) -> None:
        success_response = mock.Mock()
        success_response.raise_for_status.return_value = None
        success_response.json.return_value = {
            "data": {
                "klines": [
                    "2026-05-19,10,11,12,9,1000,2000,0,0,0,0",
                ]
            }
        }

        with (
            mock.patch(
                "sync.sync_akshare.load_eastmoney_cookie",
                return_value="stale=1",
            ),
            mock.patch(
                "sync.sync_akshare.retry_request_call",
                side_effect=[RuntimeError("first failed"), success_response],
            ) as retry_mock,
            mock.patch(
                "sync.sync_akshare.refresh_eastmoney_cookie",
                return_value=(mock.Mock(), ""),
            ) as refresh_mock,
        ):
            result = fetch_stock_history_from_eastmoney(
                symbol="600580",
                start_date="2026-05-01",
                end_date="2026-05-19",
                adjust_flag="hfq",
            )

        self.assertEqual(retry_mock.call_count, 2)
        refresh_mock.assert_called_once()
        self.assertEqual(result["日期"].tolist(), ["2026-05-19"])
        self.assertEqual(result["收盘"].tolist(), ["11"])

    def test_fetch_stock_history_from_ths_parses_last36000_payload(self) -> None:
        response_mock = mock.Mock()
        response_mock.raise_for_status.return_value = None
        response_mock.text = (
            'quotebridge_v6_line_hs_000100_00_last36000('
            '{"name":"TCL科技","data":"20260518,4.51,4.60,4.48,4.55,1000,455000;'
            '20260519,4.55,4.66,4.53,4.60,1200,552000"}'
            ")"
        )

        with mock.patch(
            "sync.sync_akshare.retry_request_call",
            return_value=response_mock,
        ) as retry_mock:
            result = fetch_stock_history_from_ths(
                symbol="000100",
                start_date="2026-05-19",
                end_date="2026-05-19",
                adjust_flag="cq",
            )

        self.assertEqual(retry_mock.call_count, 1)
        self.assertEqual(result["date"].tolist(), ["2026-05-19"])
        self.assertEqual(result["close"].tolist(), ["4.60"])
        self.assertEqual(result["amount"].tolist(), ["552000"])

    def test_fetch_index_history_falls_back_after_eastmoney_failure(self) -> None:
        fallback_df = pd.DataFrame([{"date": "2026-05-19", "close": 3200.0}])

        with (
            mock.patch(
                "sync.sync_akshare.fetch_index_history_from_eastmoney",
                side_effect=RuntimeError("eastmoney down"),
            ) as eastmoney_mock,
            mock.patch(
                "sync.sync_akshare.fetch_index_history_from_baostock",
                return_value=fallback_df,
            ) as baostock_mock,
            mock.patch(
                "sync.sync_akshare.normalize_index_history_df",
                side_effect=lambda raw_df, **_: raw_df,
            ),
        ):
            result = fetch_index_history(
                full_code="sh.000001",
                start_date="2026-05-01",
                end_date="2026-05-19",
                adjust_flag="cq",
                source_name="eastmoney",
            )

        self.assertTrue(result.equals(fallback_df))
        eastmoney_mock.assert_called_once()
        baostock_mock.assert_called_once()

    def test_fetch_index_history_from_eastmoney_retries_with_empty_cookie_session(self) -> None:
        success_response = mock.Mock()
        success_response.raise_for_status.return_value = None
        success_response.json.return_value = {
            "data": {
                "klines": [
                    "2026-05-19,3200,3210,3220,3190,1000,2000,0,0,0,0",
                ]
            }
        }

        with (
            mock.patch(
                "sync.sync_akshare.load_eastmoney_cookie",
                return_value="stale=1",
            ),
            mock.patch(
                "sync.sync_akshare.retry_request_call",
                side_effect=[RuntimeError("first failed"), success_response],
            ) as retry_mock,
            mock.patch(
                "sync.sync_akshare.refresh_eastmoney_cookie",
                return_value=(mock.Mock(), ""),
            ) as refresh_mock,
        ):
            result = fetch_index_history_from_eastmoney(
                symbol="000001",
                start_date="2026-05-01",
                end_date="2026-05-19",
            )

        self.assertEqual(retry_mock.call_count, 2)
        refresh_mock.assert_called_once()
        self.assertEqual(result["日期"].tolist(), ["2026-05-19"])
        self.assertEqual(result["收盘"].tolist(), ["3210"])

    def test_normalize_sina_stock_history_df_accepts_english_columns(self) -> None:
        raw_df = pd.DataFrame(
            [
                {
                    "Date": "2026-05-18",
                    "Open": 10,
                    "High": 11,
                    "Low": 9,
                    "Close": 10.5,
                    "Volume": 1000,
                    "Amount": 2000,
                },
                {
                    "Date": "2026-05-19",
                    "Open": 10.5,
                    "High": 12,
                    "Low": 10,
                    "Close": 11.0,
                    "Volume": 1200,
                    "Amount": 2200,
                },
            ]
        )

        result = normalize_sina_stock_history_df(
            raw_df,
            full_code="sh.600580",
            adjust_flag="hfq",
        )

        self.assertEqual(result["date"].tolist(), ["2026-05-18", "2026-05-19"])
        self.assertEqual(result["code"].tolist(), ["sh.600580", "sh.600580"])
        self.assertEqual(result["turn"].tolist(), [0.0, 0.0])
        self.assertTrue(pd.isna(result.loc[0, "preclose"]))
        self.assertEqual(result.loc[1, "preclose"], 10.5)


if __name__ == "__main__":
    unittest.main()

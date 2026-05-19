import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from sync.sync_akshare import (
    STORAGE_COLUMNS,
    build_eastmoney_cookie_chrome_applescript,
    compute_incremental_start_date,
    fetch_index_history,
    fetch_index_history_from_eastmoney,
    fetch_stock_history,
    fetch_stock_history_from_eastmoney,
    merge_and_save_history,
    normalize_sina_stock_history_df,
    refresh_eastmoney_cookie,
    refresh_eastmoney_cookie_from_chrome,
    refresh_eastmoney_cookie_from_chrome_profile,
    resolve_eastmoney_cookie_bootstrap_urls,
    should_full_refresh_history,
)


def build_row(trade_date: str, close_price: float) -> dict:
    return {
        "date": trade_date,
        "code": "sz.000725",
        "open": close_price,
        "high": close_price,
        "low": close_price,
        "close": close_price,
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

    def test_fetch_stock_history_falls_back_after_eastmoney_failure(self) -> None:
        fallback_df = pd.DataFrame([{"date": "2026-05-19", "close": 10.0}])

        with (
            mock.patch(
                "sync.sync_akshare.fetch_stock_history_from_eastmoney",
                side_effect=RuntimeError("eastmoney down"),
            ) as eastmoney_mock,
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
        sina_mock.assert_called_once()
        baostock_mock.assert_not_called()
        tushare_mock.assert_not_called()

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

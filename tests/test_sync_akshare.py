import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from sync.sync_akshare import (
    STORAGE_COLUMNS,
    compute_incremental_start_date,
    fetch_index_history,
    fetch_stock_history,
    merge_and_save_history,
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
            mock.patch("sync.sync_akshare.fetch_stock_history_from_sina") as sina_mock,
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
        baostock_mock.assert_called_once()
        sina_mock.assert_not_called()
        tushare_mock.assert_not_called()

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


if __name__ == "__main__":
    unittest.main()

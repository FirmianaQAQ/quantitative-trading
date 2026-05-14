import tempfile
import unittest
from pathlib import Path

import pandas as pd

from sync.sync_akshare import (
    STORAGE_COLUMNS,
    compute_incremental_start_date,
    merge_and_save_history,
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


if __name__ == "__main__":
    unittest.main()

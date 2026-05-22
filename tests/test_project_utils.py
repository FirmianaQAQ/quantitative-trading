import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from utils.project_utils import build_adjusted_daily_column_map, load_daily_data


class ProjectUtilsAdjustFlagTests(unittest.TestCase):
    def test_build_adjusted_daily_column_map_accepts_dypre_alias(self) -> None:
        self.assertEqual(
            build_adjusted_daily_column_map("dypre")["close"],
            "qfq_close",
        )

    def test_load_daily_data_reads_qfq_columns_for_dypre_alias(self) -> None:
        sample_df = pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
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
                    "qfq_high": 8.4,
                    "qfq_low": 7.9,
                    "qfq_close": 8.2,
                    "qfq_ex_right_close": 10.2,
                    "qfq_preclose": 8.0,
                    "qfq_volume": 1000,
                    "qfq_amount": 10000,
                    "qfq_adjustflag": "qfq",
                    "qfq_turn": 1.2,
                    "qfq_pctChg": 2.5,
                    "hfq_open": 12.0,
                    "hfq_high": 12.4,
                    "hfq_low": 11.8,
                    "hfq_close": 12.2,
                    "hfq_ex_right_close": 10.2,
                    "hfq_preclose": 12.0,
                    "hfq_volume": 1000,
                    "hfq_amount": 10000,
                    "hfq_adjustflag": "hfq",
                    "hfq_turn": 1.2,
                    "hfq_pctChg": 1.8,
                },
                {
                    "date": "2024-01-03",
                    "code": "sz.000725",
                    "cq_open": 5.0,
                    "cq_high": 5.4,
                    "cq_low": 4.9,
                    "cq_close": 5.1,
                    "cq_ex_right_close": 5.1,
                    "cq_preclose": 10.2,
                    "cq_volume": 1200,
                    "cq_amount": 10000,
                    "cq_adjustflag": "cq",
                    "cq_turn": 1.5,
                    "cq_pctChg": 0.0,
                    "qfq_open": 8.0,
                    "qfq_high": 8.6,
                    "qfq_low": 7.9,
                    "qfq_close": 8.4,
                    "qfq_ex_right_close": 5.1,
                    "qfq_preclose": 8.2,
                    "qfq_volume": 1200,
                    "qfq_amount": 10000,
                    "qfq_adjustflag": "qfq",
                    "qfq_turn": 1.5,
                    "qfq_pctChg": 2.4,
                    "hfq_open": 12.0,
                    "hfq_high": 12.6,
                    "hfq_low": 11.9,
                    "hfq_close": 12.4,
                    "hfq_ex_right_close": 5.1,
                    "hfq_preclose": 12.2,
                    "hfq_volume": 1200,
                    "hfq_amount": 10000,
                    "hfq_adjustflag": "hfq",
                    "hfq_turn": 1.5,
                    "hfq_pctChg": 1.6,
                },
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sz.000725.csv"
            sample_df.to_csv(csv_path, index=False)

            with mock.patch(
                "utils.project_utils.get_daily_csv_path",
                return_value=csv_path,
            ):
                result = load_daily_data("sz.000725", "dypre")

        self.assertEqual(result["close"].tolist(), [8.2, 8.4])
        self.assertEqual(result["raw_close"].tolist(), [10.2, 5.1])
        self.assertEqual(result["ex_right_close"].tolist(), [10.2, 5.1])
        self.assertEqual(result["signal_factor"].round(6).tolist(), [0.803922, 1.647059])
        self.assertEqual(result["position_adjust_ratio"].round(6).tolist(), [1.0, 2.04878])

    def test_load_daily_data_keeps_latest_row_when_dypre_qfq_values_are_missing(self) -> None:
        sample_df = pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
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
                    "qfq_high": 8.4,
                    "qfq_low": 7.9,
                    "qfq_close": 8.2,
                    "qfq_ex_right_close": 10.2,
                    "qfq_preclose": 8.0,
                    "qfq_volume": 1000,
                    "qfq_amount": 10000,
                    "qfq_adjustflag": "qfq",
                    "qfq_turn": 1.2,
                    "qfq_pctChg": 2.5,
                },
                {
                    "date": "2024-01-03",
                    "code": "sz.000725",
                    "cq_open": 10.3,
                    "cq_high": 10.6,
                    "cq_low": 10.1,
                    "cq_close": 10.4,
                    "cq_ex_right_close": 10.4,
                    "cq_preclose": 10.2,
                    "cq_volume": 1200,
                    "cq_amount": 12000,
                    "cq_adjustflag": "cq",
                    "cq_turn": 1.5,
                    "cq_pctChg": 1.9,
                    "qfq_open": pd.NA,
                    "qfq_high": pd.NA,
                    "qfq_low": pd.NA,
                    "qfq_close": pd.NA,
                    "qfq_ex_right_close": pd.NA,
                    "qfq_preclose": pd.NA,
                    "qfq_volume": pd.NA,
                    "qfq_amount": pd.NA,
                    "qfq_adjustflag": pd.NA,
                    "qfq_turn": pd.NA,
                    "qfq_pctChg": pd.NA,
                },
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sz.000725.csv"
            sample_df.to_csv(csv_path, index=False)

            with mock.patch(
                "utils.project_utils.get_daily_csv_path",
                return_value=csv_path,
            ):
                result = load_daily_data("sz.000725", "dypre")

        self.assertEqual(result["date"].dt.strftime("%Y-%m-%d").tolist(), ["2024-01-02", "2024-01-03"])
        self.assertEqual(result["close"].tolist(), [8.2, 10.4])
        self.assertEqual(result["raw_close"].tolist(), [10.2, 10.4])
        self.assertEqual(result["turn"].tolist(), [1.2, 1.5])


if __name__ == "__main__":
    unittest.main()

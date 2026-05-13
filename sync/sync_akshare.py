from __future__ import annotations

import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest import CONFIG, TEST_CASES
from utils.project_utils import get_daily_csv_path

logger = logging.getLogger(__name__)

STORAGE_COLUMNS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "pctChg",
    "peTTM",
    "psTTM",
    "pcfNcfTTM",
    "pbMRQ",
]


def configure_logging() -> None:
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "sync_akshare.log", encoding="utf-8"),
        ],
    )


def resolve_target_codes() -> list[str]:
    if len(sys.argv) > 1:
        return sorted({normalize_full_code(arg) for arg in sys.argv[1:] if arg.strip()})
    code_set = {
        CONFIG["code"],
        *(item["code"] for item in TEST_CASES),
    }
    if should_include_benchmark():
        code_set.add(CONFIG["benchmark_code"])
    return sorted(code_set)


def should_include_benchmark() -> bool:
    return os.getenv("SYNC_INCLUDE_BENCHMARK", "0") == "1"


def normalize_full_code(raw_code: str) -> str:
    code = raw_code.strip().lower()
    if not code:
        raise ValueError("股票代码不能为空")
    if "." in code:
        return code
    if len(code) != 6 or not code.isdigit():
        raise ValueError(f"股票代码格式错误: {raw_code}")
    prefix = "sh" if code.startswith("6") else "sz"
    return f"{prefix}.{code}"


def resolve_sync_start_date() -> str:
    candidate_dates = [
        CONFIG.get("data_from_date"),
        CONFIG.get("from_date"),
    ]
    valid_dates = [item for item in candidate_dates if item]
    if not valid_dates:
        return "2018-01-01"
    return min(valid_dates)


def resolve_sync_end_date() -> str:
    return CONFIG.get("to_date") or date.today().isoformat()


def fetch_stock_history(symbol: str, start_date: str, end_date: str, adjust_flag: str) -> pd.DataFrame:
    raw_df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date.replace("-", ""),
        end_date=end_date.replace("-", ""),
        adjust=adjust_flag,
        timeout=30,
    )
    return normalize_stock_history_df(raw_df, full_code=to_full_code(symbol), adjust_flag=adjust_flag)


def fetch_index_history(full_code: str, start_date: str, end_date: str, adjust_flag: str) -> pd.DataFrame:
    symbol = full_code.split(".", 1)[1]
    raw_df = fetch_index_history_from_eastmoney(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )
    return normalize_index_history_df(raw_df, full_code=full_code, adjust_flag=adjust_flag)


def fetch_index_history_from_eastmoney(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    headers = {
        "user-agent": (
            "Chrome/136.0.0.0 Safari/537.36"
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
        )
    }
    secid_candidates = ["1", "0", "2", "47"]
    last_error: Exception | None = None

    for market_prefix in secid_candidates:
        params = {
            "secid": f"{market_prefix}.{symbol}",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "0",
            "beg": start_date.replace("-", ""),
            "end": end_date.replace("-", ""),
        }
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data_json = response.json()
            if not data_json.get("data") or not data_json["data"].get("klines"):
                continue
            temp_df = pd.DataFrame(
                [item.split(",") for item in data_json["data"]["klines"]]
            )
            temp_df.columns = [
                "日期",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "振幅",
                "涨跌幅",
                "涨跌额",
                "换手率",
            ]
            return temp_df
        except Exception as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def normalize_stock_history_df(raw_df: pd.DataFrame, full_code: str, adjust_flag: str) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    normalized_df = raw_df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turn",
            "涨跌幅": "pctChg",
        }
    ).copy()
    normalized_df["date"] = pd.to_datetime(normalized_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized_df["code"] = full_code
    normalized_df["adjustflag"] = adjust_flag
    normalized_df["preclose"] = pd.to_numeric(normalized_df["close"], errors="coerce").shift(1)
    return finalize_history_df(normalized_df)


def normalize_index_history_df(raw_df: pd.DataFrame, full_code: str, adjust_flag: str) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    normalized_df = raw_df.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
            "换手率": "turn",
            "涨跌幅": "pctChg",
        }
    ).copy()
    normalized_df["date"] = pd.to_datetime(normalized_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    normalized_df["code"] = full_code
    normalized_df["adjustflag"] = adjust_flag
    normalized_df["preclose"] = pd.to_numeric(normalized_df["close"], errors="coerce").shift(1)
    return finalize_history_df(normalized_df)


def finalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    normalized_df["open"] = pd.to_numeric(normalized_df["open"], errors="coerce")
    normalized_df["high"] = pd.to_numeric(normalized_df["high"], errors="coerce")
    normalized_df["low"] = pd.to_numeric(normalized_df["low"], errors="coerce")
    normalized_df["close"] = pd.to_numeric(normalized_df["close"], errors="coerce")
    normalized_df["preclose"] = pd.to_numeric(normalized_df["preclose"], errors="coerce")
    normalized_df["volume"] = pd.to_numeric(normalized_df["volume"], errors="coerce").fillna(0)
    normalized_df["amount"] = pd.to_numeric(normalized_df["amount"], errors="coerce").fillna(0)
    if "turn" not in normalized_df.columns:
        normalized_df["turn"] = 0
    else:
        normalized_df["turn"] = pd.to_numeric(normalized_df["turn"], errors="coerce").fillna(0)
    if "pctChg" not in normalized_df.columns:
        normalized_df["pctChg"] = 0
    else:
        normalized_df["pctChg"] = pd.to_numeric(normalized_df["pctChg"], errors="coerce").fillna(0)
    normalized_df["peTTM"] = pd.NA
    normalized_df["psTTM"] = pd.NA
    normalized_df["pcfNcfTTM"] = pd.NA
    normalized_df["pbMRQ"] = pd.NA
    normalized_df = normalized_df[STORAGE_COLUMNS]
    normalized_df = normalized_df.dropna(subset=["date", "open", "high", "low", "close", "volume"])
    normalized_df = normalized_df.sort_values("date").reset_index(drop=True)
    return normalized_df


def to_full_code(symbol: str) -> str:
    if symbol.startswith("6"):
        return f"sh.{symbol}"
    return f"sz.{symbol}"


def read_existing_history(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=STORAGE_COLUMNS)
    existing_df = pd.read_csv(csv_path)
    if existing_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)
    return existing_df


def compute_incremental_start_date(existing_df: pd.DataFrame, default_start_date: str) -> str:
    if existing_df.empty or "date" not in existing_df.columns:
        return default_start_date

    latest_date = pd.to_datetime(existing_df["date"], errors="coerce").dropna()
    if latest_date.empty:
        return default_start_date

    return (latest_date.max().date() + timedelta(days=1)).isoformat()


def merge_and_save_history(csv_path: Path, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> int:
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["date", "code"], keep="last")
    combined_df = combined_df.sort_values("date").reset_index(drop=True)
    combined_df["preclose"] = pd.to_numeric(combined_df["close"], errors="coerce").shift(1)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(csv_path, index=False, encoding="utf-8")
    return len(new_df)


def sync_one_code(full_code: str, start_date: str, end_date: str, adjust_flag: str) -> str:
    csv_path = get_daily_csv_path(full_code, adjust_flag)
    existing_df = read_existing_history(csv_path)
    request_start_date = compute_incremental_start_date(existing_df, start_date)
    if request_start_date > end_date:
        logger.info("%s 已是最新，跳过", full_code)
        return "skipped"

    logger.info("开始同步 %s, 区间 %s ~ %s", full_code, request_start_date, end_date)
    if full_code == CONFIG["benchmark_code"]:
        new_df = fetch_index_history(
            full_code=full_code,
            start_date=request_start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
        )
    else:
        symbol = full_code.split(".", 1)[1]
        new_df = fetch_stock_history(
            symbol=symbol,
            start_date=request_start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
        )

    if new_df.empty:
        logger.warning("%s 没有拉到数据", full_code)
        return "empty"

    save_count = merge_and_save_history(csv_path, existing_df, new_df)
    logger.info("%s 同步完成，新增 %s 条", full_code, save_count)
    return "success"


def is_network_unavailable_error(exc: Exception) -> bool:
    message = str(exc).lower()
    keywords = [
        "name resolution",
        "failed to resolve",
        "nodename nor servname provided",
        "max retries exceeded",
        "connection aborted",
        "remote end closed connection",
    ]
    return any(keyword in message for keyword in keywords)


def main() -> None:
    configure_logging()
    adjust_flag = CONFIG["adjust_flag"]
    start_date = resolve_sync_start_date()
    end_date = resolve_sync_end_date()
    target_codes = resolve_target_codes()

    logger.info("使用 Akshare 同步启动所需数据")
    logger.info("目标代码: %s", ", ".join(target_codes))
    logger.info("同步区间: %s ~ %s", start_date, end_date)

    success = 0
    skipped = 0
    failed = 0
    for full_code in target_codes:
        try:
            result = sync_one_code(
                full_code=full_code,
                start_date=start_date,
                end_date=end_date,
                adjust_flag=adjust_flag,
            )
            if result == "success":
                success += 1
            elif result == "skipped":
                skipped += 1
            else:
                failed += 1
        except Exception:
            logger.exception("%s 同步失败", full_code)
            if is_network_unavailable_error(sys.exc_info()[1] or Exception()):
                logger.error("检测到行情源网络不可用，停止后续同步")
                raise SystemExit(2)
            failed += 1

    logger.info("同步结束: success=%s skipped=%s failed=%s", success, skipped, failed)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

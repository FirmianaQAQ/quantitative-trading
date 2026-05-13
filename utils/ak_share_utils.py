from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd

from utils.date import normalize_date
from utils.stock_utils import stock_is_ignored

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DEFAULT_CACHE_PATH = DATA_DIR / "a_share_code_name_cache.json"
DEFAULT_TRADE_DATE_CACHE_PATH = DATA_DIR / "trade_date_hist_sina_cache.json"


def _normalize_stock_list_df(stock_list_df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = stock_list_df.copy()
    normalized_df["code"] = normalized_df["code"].astype(str).str.zfill(6)
    normalized_df["name"] = normalized_df["name"].fillna("").astype(str)
    return normalized_df[["code", "name"]].sort_values("code").reset_index(drop=True)


def _normalize_trade_calendar_df(trade_calendar_df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = trade_calendar_df.copy()
    normalized_df["trade_date"] = pd.to_datetime(
        normalized_df["trade_date"],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
    normalized_df = normalized_df.dropna(subset=["trade_date"])
    normalized_df = normalized_df.drop_duplicates(subset=["trade_date"])
    return normalized_df[["trade_date"]].sort_values("trade_date").reset_index(drop=True)


def _load_cache_payload(cache_path: Path) -> dict[str, Any] | None:
    if not cache_path.exists():
        return None

    with cache_path.open("r", encoding="utf-8") as cache_file:
        payload = json.load(cache_file)

    if not isinstance(payload, dict):
        return None
    if not isinstance(payload.get("updated_at"), str):
        return None
    if not isinstance(payload.get("data"), list):
        return None
    return payload

def _cache_payload_to_trade_calendar_df(payload: dict[str, Any]) -> pd.DataFrame:
    return _normalize_trade_calendar_df(pd.DataFrame(payload["data"]))


def _save_cache_payload(
    cache_path: Path,
    normalized_df: pd.DataFrame,
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "trade_date": date.today().isoformat(),
        "data": normalized_df.to_dict(orient="records"),
    }
    with cache_path.open("w", encoding="utf-8") as cache_file:
        json.dump(payload, cache_file, ensure_ascii=False, indent=2)


def get_a_share_code_name_df(
    cache_path: str | Path = DEFAULT_CACHE_PATH,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """获取 A 股股票代码和名称的 DataFrame，包含 'code' 和 'name' 两列"""
    resolved_cache_path = Path(cache_path)
    cache_payload = _load_cache_payload(resolved_cache_path)
    today = date.today().isoformat()

    if not force_refresh and cache_payload is not None and cache_payload.get("trade_date") == today:
        return _normalize_stock_list_df(pd.DataFrame(cache_payload["data"]))
        
    try:
        temp = ak.stock_info_a_code_name()
        stock_list_df = _normalize_stock_list_df(temp)
    except Exception:
        if cache_payload is not None:
            return _normalize_stock_list_df(pd.DataFrame(cache_payload["data"]))
        raise

    _save_cache_payload(resolved_cache_path, stock_list_df)
    return stock_list_df


def get_a_share_code_name_df_and_filter() -> pd.DataFrame:
    """把上面函数获得的股票过滤一下，只返回需要的"""
    stock_list_df = get_a_share_code_name_df()
    filtered_df = stock_list_df.copy()

    keep_mask = ~filtered_df.apply(
        lambda row: stock_is_ignored(str(row["code"]).zfill(6), str(row["name"])),
        axis=1,
    )
    return filtered_df.loc[keep_mask, ["code", "name"]].sort_values("code").reset_index(drop=True)


def get_trade_date_hist_df(
    cache_path: str | Path = DEFAULT_TRADE_DATE_CACHE_PATH,
    force_refresh: bool = False,
) -> pd.DataFrame:
    resolved_cache_path = Path(cache_path)
    cache_payload = _load_cache_payload(resolved_cache_path)
    today = date.today().isoformat()

    if not force_refresh and cache_payload is not None and cache_payload.get("trade_date") == today:
        return _cache_payload_to_trade_calendar_df(cache_payload)

    try:
        trade_calendar_df = _normalize_trade_calendar_df(ak.tool_trade_date_hist_sina())
    except Exception:
        if cache_payload is not None:
            return _cache_payload_to_trade_calendar_df(cache_payload)
        raise

    _save_cache_payload(resolved_cache_path, trade_calendar_df)
    return trade_calendar_df


def get_previous_trade_date(base_date: date | datetime | str | None = None) -> date:
    normalized_date = normalize_date(base_date)
    trade_calendar_df = get_trade_date_hist_df()
    trade_dates = pd.to_datetime(trade_calendar_df["trade_date"]).dt.date
    previous_trade_dates = trade_dates[trade_dates < normalized_date]
    if previous_trade_dates.empty:
        raise ValueError(f"base_date {normalized_date} has no previous trade date")
    return previous_trade_dates.iloc[-1]


def get_trade_dates_in_range(
    start_date: date,
    end_date: date,
) -> list[date]:
    trade_calendar_df = get_trade_date_hist_df()
    trade_dates = pd.to_datetime(trade_calendar_df["trade_date"], errors="coerce").dt.date
    filtered_trade_dates = trade_dates[(trade_dates >= start_date) & (trade_dates <= end_date)]
    return sorted(filtered_trade_dates.tolist())

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
from requests import exceptions as requests_exceptions

try:
    import tushare as ts
except ImportError:
    ts = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.simple_ma_backtest import CONFIG, TEST_CASES
from utils.ak_share_utils import get_a_share_code_name_df_and_filter
from utils.project_utils import get_daily_csv_path
from utils.stock_utils import stock_is_ignored

logger = logging.getLogger(__name__)

SH_MAIN_BOARD_PREFIXES = ("600", "601", "603", "605")
EXCLUDED_NAME_KEYWORDS = ("融创",)
REQUEST_RETRY_TIMES = 3
REQUEST_RETRY_SLEEP_SECONDS = 2
CODE_SYNC_INTERVAL_SECONDS = 0.5
FAILED_QUEUE_RETRY_ROUNDS = 1
FAILED_QUEUE_RETRY_COOLDOWN_SECONDS = 8
TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"

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

_TUSHARE_PRO_CLIENT = None


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


def get_tushare_token() -> str:
    token = os.getenv(TUSHARE_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(
            f"缺少 {TUSHARE_TOKEN_ENV} 环境变量，无法使用 Tushare 同步数据"
        )
    return token


def get_tushare_pro_client():
    global _TUSHARE_PRO_CLIENT
    if _TUSHARE_PRO_CLIENT is not None:
        return _TUSHARE_PRO_CLIENT
    if ts is None:
        raise RuntimeError("未安装 tushare，请先执行 pip install -r requirements.txt")
    token = get_tushare_token()
    ts.set_token(token)
    _TUSHARE_PRO_CLIENT = ts.pro_api(token)
    return _TUSHARE_PRO_CLIENT


def to_tushare_code(full_code: str) -> str:
    market, symbol = full_code.split(".", 1)
    return f"{symbol}.{market.upper()}"


def resolve_target_codes() -> list[str]:
    if len(sys.argv) > 1:
        cli_args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
        if len(cli_args) == 1 and cli_args[0] == "--all-sh-main":
            return resolve_all_sh_main_codes()
        return sorted({normalize_full_code(arg) for arg in cli_args})
    code_set = {
        CONFIG["code"],
        *(item["code"] for item in TEST_CASES),
    }
    if should_include_benchmark():
        code_set.add(CONFIG["benchmark_code"])
    return sorted(code_set)


def resolve_all_sh_main_codes() -> list[str]:
    """
    获取上证主板普通账户可买的股票代码。

    当前口径限定为上证 A 股主板常见前缀：
    - 600
    - 601
    - 603
    - 605

    已复用全局过滤规则，因此会自动排除 ST、科创板、创业板、
    北交所和 ignore_stock_code 中的特殊个股。
    此外会额外排除名称中包含“融创”的股票。
    """
    try:
        stock_df = fetch_sh_main_stock_pool_from_tushare()
    except Exception as exc:
        logger.warning("Tushare 股票池获取失败，回退到 Akshare: %s", exc)
        stock_df = get_a_share_code_name_df_and_filter()

    sh_main_df = stock_df[
        stock_df["code"].astype(str).str.startswith(SH_MAIN_BOARD_PREFIXES)
    ].copy()
    if EXCLUDED_NAME_KEYWORDS:
        keep_mask = ~sh_main_df["name"].astype(str).apply(
            lambda stock_name: any(
                keyword in stock_name for keyword in EXCLUDED_NAME_KEYWORDS
            )
        )
        sh_main_df = sh_main_df.loc[keep_mask].copy()
    return [f"sh.{code}" for code in sh_main_df["code"].tolist()]


def fetch_sh_main_stock_pool_from_tushare() -> pd.DataFrame:
    pro = get_tushare_pro_client()
    raw_df = retry_request_call(
        lambda: pro.stock_basic(
            exchange="SSE",
            list_status="L",
            fields="ts_code,symbol,name",
        ),
        action_name="拉取上证主板股票池",
    )
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=["code", "name"])

    normalized_df = raw_df.rename(columns={"symbol": "code"}).copy()
    normalized_df["code"] = normalized_df["code"].astype(str).str.zfill(6)
    normalized_df["name"] = normalized_df["name"].fillna("").astype(str)
    keep_mask = ~normalized_df.apply(
        lambda row: stock_is_ignored(str(row["code"]), str(row["name"]), market="sh"),
        axis=1,
    )
    return (
        normalized_df.loc[keep_mask, ["code", "name"]]
        .sort_values("code")
        .reset_index(drop=True)
    )


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


def is_retryable_request_error(exc: Exception) -> bool:
    message = str(exc).lower()
    keywords = [
        "remote end closed connection without response",
        "connection aborted",
        "temporarily unavailable",
        "read timed out",
        "timed out",
        "connection reset by peer",
        "bad gateway",
        "service unavailable",
        "too many requests",
    ]
    if isinstance(
        exc,
        (
            requests_exceptions.ConnectionError,
            requests_exceptions.Timeout,
            requests_exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    return any(keyword in message for keyword in keywords)


def retry_request_call(func, *, action_name: str):
    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRY_TIMES + 1):
        try:
            return func()
        except Exception as exc:
            last_error = exc
            if not is_retryable_request_error(exc) or attempt >= REQUEST_RETRY_TIMES:
                raise
            wait_seconds = REQUEST_RETRY_SLEEP_SECONDS * attempt
            logger.warning(
                "%s 第 %s/%s 次失败: %s，%s 秒后重试",
                action_name,
                attempt,
                REQUEST_RETRY_TIMES,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{action_name} 未执行")


def fetch_stock_history(symbol: str, start_date: str, end_date: str, adjust_flag: str) -> pd.DataFrame:
    full_code = to_full_code(symbol)
    ts_code = to_tushare_code(full_code)
    source_errors: list[str] = []

    stock_sources = [
        (
            "tushare",
            lambda: fetch_stock_history_from_tushare(
                ts_code=ts_code,
                full_code=full_code,
                start_date=start_date,
                end_date=end_date,
                adjust_flag=adjust_flag,
            ),
        ),
        (
            "eastmoney",
            lambda: normalize_stock_history_df(
                fetch_stock_history_from_eastmoney(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust_flag=adjust_flag,
                ),
                full_code=full_code,
                adjust_flag=adjust_flag,
            ),
        ),
        (
            "sina",
            lambda: normalize_sina_stock_history_df(
                fetch_stock_history_from_sina(
                    full_code=full_code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust_flag=adjust_flag,
                ),
                full_code=full_code,
                adjust_flag=adjust_flag,
            ),
        ),
    ]

    for source_name, fetcher in stock_sources:
        try:
            result_df = fetcher()
            if result_df.empty:
                logger.warning("%s 使用 %s 未拉到数据，尝试下一个源", full_code, source_name)
                source_errors.append(f"{source_name}: empty")
                continue
            logger.info("%s 使用 %s 拉取成功", full_code, source_name)
            return result_df
        except Exception as exc:
            logger.warning("%s 使用 %s 失败: %s", full_code, source_name, exc)
            source_errors.append(f"{source_name}: {exc}")

    raise RuntimeError(f"{full_code} 所有股票数据源均失败: {' | '.join(source_errors)}")


def fetch_stock_history_from_tushare(
    ts_code: str,
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    if ts is None:
        raise RuntimeError("未安装 tushare")

    tushare_adjust = adjust_flag if adjust_flag in ("qfq", "hfq") else None
    start_date_compact = start_date.replace("-", "")
    end_date_compact = end_date.replace("-", "")

    bar_df = retry_request_call(
        lambda: ts.pro_bar(
            ts_code=ts_code,
            asset="E",
            adj=tushare_adjust,
            start_date=start_date_compact,
            end_date=end_date_compact,
        ),
        action_name=f"拉取股票 {ts_code} Tushare 日线",
    )
    if bar_df is None or bar_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    pro = get_tushare_pro_client()
    basic_df = retry_request_call(
        lambda: pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date_compact,
            end_date=end_date_compact,
            fields="ts_code,trade_date,turnover_rate,pe_ttm,ps_ttm,pb",
        ),
        action_name=f"拉取股票 {ts_code} Tushare 基本面",
    )
    return normalize_tushare_stock_history_df(
        bar_df=bar_df,
        basic_df=basic_df,
        full_code=full_code,
        adjust_flag=adjust_flag,
    )


def fetch_stock_history_from_eastmoney(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    market_prefix = "sh" if symbol.startswith("6") else "sz"
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        "referer": f"https://quote.eastmoney.com/concept/{market_prefix}{symbol}.html",
        "accept": "application/json, text/javascript, */*; q=0.01",
    }
    market_code = "1" if symbol.startswith("6") else "0"
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": adjust_dict.get(adjust_flag, "0"),
        "secid": f"{market_code}.{symbol}",
        "beg": start_date.replace("-", ""),
        "end": end_date.replace("-", ""),
    }
    response = retry_request_call(
        lambda: requests.get(url, params=params, headers=headers, timeout=30),
        action_name=f"拉取股票 {symbol} 日线",
    )
    response.raise_for_status()
    data_json = response.json()
    if not data_json.get("data") or not data_json["data"].get("klines"):
        return pd.DataFrame()
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
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
        "股票代码",
    ]
    return temp_df


def fetch_stock_history_from_sina(
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    return retry_request_call(
        lambda: ak.stock_zh_a_daily(
            symbol=full_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            adjust=adjust_flag,
        ),
        action_name=f"拉取股票 {full_code} 新浪日线",
    )


def fetch_index_history(full_code: str, start_date: str, end_date: str, adjust_flag: str) -> pd.DataFrame:
    symbol = full_code.split(".", 1)[1]
    ts_code = to_tushare_code(full_code)
    source_errors: list[str] = []

    index_sources = [
        (
            "tushare",
            lambda: fetch_index_history_from_tushare(
                ts_code=ts_code,
                full_code=full_code,
                start_date=start_date,
                end_date=end_date,
                adjust_flag=adjust_flag,
            ),
        ),
        (
            "eastmoney",
            lambda: normalize_index_history_df(
                fetch_index_history_from_eastmoney(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                ),
                full_code=full_code,
                adjust_flag=adjust_flag,
            ),
        ),
    ]

    for source_name, fetcher in index_sources:
        try:
            result_df = fetcher()
            if result_df.empty:
                logger.warning("%s 使用 %s 未拉到数据，尝试下一个源", full_code, source_name)
                source_errors.append(f"{source_name}: empty")
                continue
            logger.info("%s 使用 %s 拉取成功", full_code, source_name)
            return result_df
        except Exception as exc:
            logger.warning("%s 使用 %s 失败: %s", full_code, source_name, exc)
            source_errors.append(f"{source_name}: {exc}")

    raise RuntimeError(f"{full_code} 所有指数数据源均失败: {' | '.join(source_errors)}")


def fetch_index_history_from_tushare(
    ts_code: str,
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    pro = get_tushare_pro_client()
    raw_df = retry_request_call(
        lambda: pro.index_daily(
            ts_code=ts_code,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
        ),
        action_name=f"拉取指数 {ts_code} Tushare 日线",
    )
    return normalize_tushare_index_history_df(raw_df, full_code=full_code, adjust_flag=adjust_flag)


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
            response = retry_request_call(
                lambda: requests.get(url, params=params, headers=headers, timeout=30),
                action_name=f"拉取指数 {symbol} 日线",
            )
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


def normalize_tushare_stock_history_df(
    bar_df: pd.DataFrame,
    basic_df: pd.DataFrame,
    full_code: str,
    adjust_flag: str,
) -> pd.DataFrame:
    if bar_df is None or bar_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    normalized_df = bar_df.copy()
    normalized_df["trade_date"] = pd.to_datetime(
        normalized_df["trade_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    normalized_df = normalized_df.rename(
        columns={
            "trade_date": "date",
            "pre_close": "preclose",
            "vol": "volume",
            "pct_chg": "pctChg",
        }
    )

    if basic_df is not None and not basic_df.empty:
        basic_normalized_df = basic_df.copy()
        basic_normalized_df["trade_date"] = pd.to_datetime(
            basic_normalized_df["trade_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        basic_normalized_df = basic_normalized_df.rename(
            columns={
                "trade_date": "date",
                "turnover_rate": "turn",
                "pe_ttm": "peTTM",
                "ps_ttm": "psTTM",
                "pb": "pbMRQ",
            }
        )
        normalized_df = normalized_df.merge(
            basic_normalized_df[["date", "turn", "peTTM", "psTTM", "pbMRQ"]],
            on="date",
            how="left",
        )
    else:
        normalized_df["turn"] = 0
        normalized_df["peTTM"] = pd.NA
        normalized_df["psTTM"] = pd.NA
        normalized_df["pbMRQ"] = pd.NA

    normalized_df["code"] = full_code
    normalized_df["adjustflag"] = adjust_flag
    normalized_df["pcfNcfTTM"] = pd.NA
    return finalize_history_df(normalized_df)


def normalize_sina_stock_history_df(
    raw_df: pd.DataFrame,
    full_code: str,
    adjust_flag: str,
) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    normalized_df = raw_df.copy()
    normalized_df["date"] = pd.to_datetime(
        normalized_df["date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    normalized_df["code"] = full_code
    normalized_df["adjustflag"] = adjust_flag
    normalized_df["turn"] = pd.to_numeric(
        normalized_df.get("turnover", 0), errors="coerce"
    ).fillna(0) * 100
    normalized_df["pctChg"] = (
        (
            pd.to_numeric(normalized_df["close"], errors="coerce")
            / pd.to_numeric(normalized_df["close"], errors="coerce").shift(1)
            - 1
        )
        * 100
    )
    normalized_df["preclose"] = pd.to_numeric(
        normalized_df["close"], errors="coerce"
    ).shift(1)
    return finalize_history_df(normalized_df)


def normalize_tushare_index_history_df(
    raw_df: pd.DataFrame,
    full_code: str,
    adjust_flag: str,
) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    normalized_df = raw_df.copy()
    normalized_df["trade_date"] = pd.to_datetime(
        normalized_df["trade_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    normalized_df = normalized_df.rename(
        columns={
            "trade_date": "date",
            "pre_close": "preclose",
            "vol": "volume",
            "pct_chg": "pctChg",
        }
    )
    normalized_df["code"] = full_code
    normalized_df["adjustflag"] = adjust_flag
    normalized_df["turn"] = 0
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
        "temporary failure in name resolution",
        "network is unreachable",
        "no route to host",
        "proxyerror",
    ]
    return any(keyword in message for keyword in keywords)


def sleep_between_code_sync() -> None:
    if CODE_SYNC_INTERVAL_SECONDS <= 0:
        return
    time.sleep(CODE_SYNC_INTERVAL_SECONDS)


def run_sync_batch(
    target_codes: list[str],
    *,
    start_date: str,
    end_date: str,
    adjust_flag: str,
    batch_name: str,
) -> tuple[int, int, int, list[str]]:
    success = 0
    skipped = 0
    failed = 0
    failed_codes: list[str] = []

    if not target_codes:
        logger.info("%s 没有待同步代码", batch_name)
        return success, skipped, failed, failed_codes

    logger.info(
        "%s 开始: count=%s interval=%ss",
        batch_name,
        len(target_codes),
        CODE_SYNC_INTERVAL_SECONDS,
    )

    for index, full_code in enumerate(target_codes, start=1):
        try:
            logger.info("%s 进度 %s/%s: %s", batch_name, index, len(target_codes), full_code)
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
                failed_codes.append(full_code)
        except Exception:
            logger.exception("%s 同步失败", full_code)
            current_error = sys.exc_info()[1] or Exception()
            if is_network_unavailable_error(current_error):
                logger.error("检测到网络不可用，停止当前批次")
                raise SystemExit(2)
            failed += 1
            failed_codes.append(full_code)

        if index < len(target_codes):
            sleep_between_code_sync()

    logger.info(
        "%s 结束: success=%s skipped=%s failed=%s",
        batch_name,
        success,
        skipped,
        failed,
    )
    if failed_codes:
        logger.warning("%s 失败代码: %s", batch_name, ", ".join(failed_codes))
    return success, skipped, failed, failed_codes


def main() -> None:
    configure_logging()
    adjust_flag = CONFIG["adjust_flag"]
    start_date = resolve_sync_start_date()
    end_date = resolve_sync_end_date()
    target_codes = resolve_target_codes()

    logger.info("使用 Akshare 同步启动所需数据")
    logger.info("目标代码: %s", ", ".join(target_codes))
    logger.info("同步区间: %s ~ %s", start_date, end_date)

    total_success, total_skipped, _, failed_codes = run_sync_batch(
        target_codes,
        start_date=start_date,
        end_date=end_date,
        adjust_flag=adjust_flag,
        batch_name="首轮同步",
    )

    for retry_round in range(1, FAILED_QUEUE_RETRY_ROUNDS + 1):
        if not failed_codes:
            break
        logger.info(
            "失败队列重跑第 %s/%s 轮，冷却 %s 秒后开始",
            retry_round,
            FAILED_QUEUE_RETRY_ROUNDS,
            FAILED_QUEUE_RETRY_COOLDOWN_SECONDS,
        )
        time.sleep(FAILED_QUEUE_RETRY_COOLDOWN_SECONDS)
        retry_success, retry_skipped, _, failed_codes = run_sync_batch(
            failed_codes,
            start_date=start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
            batch_name=f"失败队列重跑第{retry_round}轮",
        )
        total_success += retry_success
        total_skipped += retry_skipped

    logger.info(
        "同步结束: success=%s skipped=%s remaining_failed=%s",
        total_success,
        total_skipped,
        len(failed_codes),
    )
    if failed_codes:
        logger.error("最终失败代码: %s", ", ".join(failed_codes))
        raise SystemExit(1)


if __name__ == "__main__":
    main()

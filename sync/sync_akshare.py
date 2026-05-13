from __future__ import annotations

import atexit
import logging
import os
import sys
import time
from collections import deque
from datetime import date, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
from requests import exceptions as requests_exceptions

try:
    import baostock as bs
except ImportError:
    bs = None

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
EASTMONEY_COOKIE_BOOTSTRAP_URL = "https://quote.eastmoney.com/concept/sz000014.html"
TUSHARE_TOKEN = "2755050aba62303e45f6842ee9e67defdf6e3c1b32bb033ca4ba037e"
TUSHARE_DAILY_MAX_CALLS_PER_MINUTE = 45
TUSHARE_DAILY_WINDOW_SECONDS = 60

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
_TUSHARE_DAILY_CALL_TIMESTAMPS: deque[float] = deque()
_BAOSTOCK_LOGGED_IN = False
_BAOSTOCK_DISABLED = False
SYNC_SOURCE_AUTO = "auto"
SYNC_SOURCE_BAOSTOCK = "baostock"
SYNC_SOURCE_AKSHARE = "akshare"
SYNC_SOURCE_TUSHARE = "tushare"
SYNC_SOURCE_CHOICES = {
    SYNC_SOURCE_AUTO,
    SYNC_SOURCE_BAOSTOCK,
    SYNC_SOURCE_AKSHARE,
    SYNC_SOURCE_TUSHARE,
}


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
    token = TUSHARE_TOKEN.strip()
    if not token:
        raise RuntimeError("缺少 Tushare Token，无法使用 Tushare 同步数据")
    return token


def get_eastmoney_cookie_path() -> Path:
    return PROJECT_ROOT / "data" / "cookie.txt"


def format_cookie_header(cookie_jar) -> str:
    cookie_items = []
    for cookie in cookie_jar:
        if not getattr(cookie, "name", ""):
            continue
        cookie_items.append(f"{cookie.name}={cookie.value}")
    return "; ".join(cookie_items)


def write_eastmoney_cookie(cookie_str: str) -> None:
    if not cookie_str.strip():
        return
    cookie_path = get_eastmoney_cookie_path()
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookie_path.write_text(cookie_str.strip(), encoding="utf-8")
    logger.info("已刷新东方财富 cookie: %s", cookie_path)


def load_eastmoney_cookie() -> str:
    cookie_path = get_eastmoney_cookie_path()
    if not cookie_path.exists():
        return ""
    return cookie_path.read_text(encoding="utf-8").strip()


def build_eastmoney_headers(referer_url: str, cookie_str: str = "") -> dict[str, str]:
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        "referer": referer_url,
        "accept": "application/json, text/javascript, */*; q=0.01",
    }
    if cookie_str.strip():
        headers["cookie"] = cookie_str.strip()
    return headers


def refresh_eastmoney_cookie() -> tuple[requests.Session, str]:
    session = requests.Session()
    response = retry_request_call(
        lambda: session.get(
            EASTMONEY_COOKIE_BOOTSTRAP_URL,
            headers=build_eastmoney_headers(EASTMONEY_COOKIE_BOOTSTRAP_URL),
            timeout=30,
        ),
        action_name="访问东方财富页面获取 cookie",
    )
    response.raise_for_status()
    cookie_str = format_cookie_header(session.cookies)
    if not cookie_str:
        raise RuntimeError("访问东方财富页面后未拿到有效 cookie")
    write_eastmoney_cookie(cookie_str)
    return session, cookie_str


def ensure_baostock_login() -> None:
    global _BAOSTOCK_LOGGED_IN, _BAOSTOCK_DISABLED
    if _BAOSTOCK_LOGGED_IN:
        return
    if _BAOSTOCK_DISABLED:
        raise RuntimeError("Baostock 已被禁用，自动切换到 Akshare")
    if bs is None:
        _BAOSTOCK_DISABLED = True
        raise RuntimeError("未安装 baostock，自动切换到 Akshare")
    login_result = bs.login()
    if getattr(login_result, "error_code", "") != "0":
        _BAOSTOCK_DISABLED = True
        raise RuntimeError(
            f"Baostock 登录失败，已切换到 Akshare: "
            f"{login_result.error_code} {login_result.error_msg}"
        )
    _BAOSTOCK_LOGGED_IN = True


def logout_baostock() -> None:
    global _BAOSTOCK_LOGGED_IN
    if not _BAOSTOCK_LOGGED_IN or bs is None:
        return
    try:
        bs.logout()
    finally:
        _BAOSTOCK_LOGGED_IN = False


atexit.register(logout_baostock)


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


def parse_cli_options() -> tuple[list[str], bool, str]:
    raw_args = [arg.strip() for arg in sys.argv[1:] if arg.strip()]
    source_name = SYNC_SOURCE_AUTO
    code_args: list[str] = []
    sync_all_sh_main = False

    index = 0
    while index < len(raw_args):
        current = raw_args[index]
        if current == "--all-sh-main":
            sync_all_sh_main = True
            index += 1
            continue
        if current.startswith("--source="):
            source_name = current.split("=", 1)[1].strip().lower()
            index += 1
            continue
        if current == "--source":
            if index + 1 >= len(raw_args):
                raise ValueError("--source 缺少取值")
            source_name = raw_args[index + 1].strip().lower()
            index += 2
            continue
        code_args.append(current)
        index += 1

    if source_name not in SYNC_SOURCE_CHOICES:
        choices_text = ", ".join(sorted(SYNC_SOURCE_CHOICES))
        raise ValueError(f"不支持的数据源: {source_name}，可选值: {choices_text}")
    if sync_all_sh_main and code_args:
        raise ValueError("--all-sh-main 不能与股票代码同时传入")

    return code_args, sync_all_sh_main, source_name


def resolve_target_codes(code_args: list[str], sync_all_sh_main: bool, source_name: str) -> list[str]:
    if sync_all_sh_main:
        return resolve_all_sh_main_codes(source_name)
    if code_args:
        return sorted({normalize_full_code(arg) for arg in code_args})
    code_set = {
        CONFIG["code"],
        *(item["code"] for item in TEST_CASES),
    }
    if should_include_benchmark():
        code_set.add(CONFIG["benchmark_code"])
    return sorted(code_set)


def resolve_all_sh_main_codes(source_name: str) -> list[str]:
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
    if source_name == SYNC_SOURCE_BAOSTOCK:
        stock_df = fetch_sh_main_stock_pool_from_baostock()
    elif source_name == SYNC_SOURCE_AKSHARE:
        stock_df = get_a_share_code_name_df_and_filter()
    elif source_name == SYNC_SOURCE_TUSHARE:
        stock_df = fetch_sh_main_stock_pool_from_tushare()
    else:
        try:
            stock_df = fetch_sh_main_stock_pool_from_baostock()
        except Exception as exc:
            logger.warning("Baostock 股票池获取失败，回退到 Akshare: %s", exc)
            try:
                stock_df = get_a_share_code_name_df_and_filter()
            except Exception as ak_exc:
                logger.warning("Akshare 股票池获取失败，回退到 Tushare: %s", ak_exc)
                stock_df = fetch_sh_main_stock_pool_from_tushare()

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


def fetch_sh_main_stock_pool_from_baostock() -> pd.DataFrame:
    ensure_baostock_login()
    query_day = date.today().isoformat()
    rs = bs.query_all_stock(day=query_day)
    if getattr(rs, "error_code", "") != "0":
        raise RuntimeError(f"Baostock query_all_stock 失败: {rs.error_msg}")

    data_list: list[list[str]] = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list:
        return pd.DataFrame(columns=["code", "name"])

    raw_df = pd.DataFrame(data_list, columns=rs.fields)
    name_column = "code_name" if "code_name" in raw_df.columns else "name"
    if name_column not in raw_df.columns:
        raw_df[name_column] = ""

    normalized_df = raw_df.rename(columns={name_column: "name"}).copy()
    normalized_df["code"] = normalized_df["code"].astype(str)
    normalized_df = normalized_df[normalized_df["code"].str.startswith("sh.")]
    normalized_df["code"] = normalized_df["code"].str.split(".").str[-1]
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


def resolve_effective_adjust_flag(config_adjust_flag: str) -> str:
    return config_adjust_flag


def normalize_adjust_flag(adjust_flag: str) -> str:
    return str(adjust_flag or "").strip().lower()


def is_unadjusted_flag(adjust_flag: str) -> bool:
    return normalize_adjust_flag(adjust_flag) in {"", "3", "bfq", "cq", "raw", "none"}


def to_baostock_adjust_flag(adjust_flag: str) -> str:
    normalized_flag = normalize_adjust_flag(adjust_flag)
    if normalized_flag == "hfq":
        return "1"
    if normalized_flag == "qfq":
        return "2"
    if is_unadjusted_flag(normalized_flag):
        return "3"
    raise ValueError(f"不支持的 Baostock 复权口径: {adjust_flag}")


def to_akshare_adjust_flag(adjust_flag: str) -> str:
    normalized_flag = normalize_adjust_flag(adjust_flag)
    if normalized_flag in {"hfq", "qfq"}:
        return normalized_flag
    return ""


def is_tushare_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    keywords = [
        "频率超限",
        "每分钟最多访问",
        "too many requests",
        "rate limit",
        "rate exceeded",
        "50次/分钟",
    ]
    return any(keyword in message for keyword in keywords)


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
        "频率超限",
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
            if is_tushare_rate_limit_error(exc):
                wait_seconds = max(wait_seconds, TUSHARE_DAILY_WINDOW_SECONDS + 2)
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


def enforce_tushare_daily_rate_limit() -> None:
    while True:
        now = time.time()
        while _TUSHARE_DAILY_CALL_TIMESTAMPS and (
            now - _TUSHARE_DAILY_CALL_TIMESTAMPS[0] >= TUSHARE_DAILY_WINDOW_SECONDS
        ):
            _TUSHARE_DAILY_CALL_TIMESTAMPS.popleft()

        if len(_TUSHARE_DAILY_CALL_TIMESTAMPS) < TUSHARE_DAILY_MAX_CALLS_PER_MINUTE:
            _TUSHARE_DAILY_CALL_TIMESTAMPS.append(now)
            return

        wait_seconds = (
            TUSHARE_DAILY_WINDOW_SECONDS
            - (now - _TUSHARE_DAILY_CALL_TIMESTAMPS[0])
            + 1
        )
        wait_seconds = max(wait_seconds, 1)
        logger.warning(
            "Tushare daily 调用已接近频控阈值，等待 %.1f 秒后继续",
            wait_seconds,
        )
        time.sleep(wait_seconds)


def fetch_stock_history(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
    source_name: str = SYNC_SOURCE_AUTO,
) -> pd.DataFrame:
    full_code = to_full_code(symbol)
    ts_code = to_tushare_code(full_code)
    source_errors: list[str] = []

    baostock_source = (
        "baostock",
        lambda: fetch_stock_history_from_baostock(
            full_code=full_code,
            start_date=start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
        ),
    )
    akshare_sources = [
        (
            "akshare-eastmoney",
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
            "akshare-sina",
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
    tushare_source = (
        "tushare-daily",
        lambda: fetch_stock_history_from_tushare(
            ts_code=ts_code,
            full_code=full_code,
            start_date=start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
        ),
    )

    if source_name == SYNC_SOURCE_BAOSTOCK:
        stock_sources = [baostock_source]
    elif source_name == SYNC_SOURCE_AKSHARE:
        stock_sources = akshare_sources
    elif source_name == SYNC_SOURCE_TUSHARE:
        if not is_unadjusted_flag(adjust_flag):
            raise RuntimeError(f"Tushare 仅支持不复权日线，不支持 {adjust_flag}")
        stock_sources = [tushare_source]
    else:
        stock_sources = [baostock_source, *akshare_sources]
        if is_unadjusted_flag(adjust_flag):
            stock_sources.append(tushare_source)
        else:
            logger.info("%s 请求 %s 复权口径，跳过 Tushare daily 兜底", full_code, adjust_flag)

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


def fetch_stock_history_from_baostock(
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    ensure_baostock_login()
    query_result = retry_request_call(
        lambda: bs.query_history_k_data_plus(
            full_code,
            (
                "date,code,open,high,low,close,preclose,volume,amount,"
                "adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,"
                "pcfNcfTTM,isST"
            ),
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=to_baostock_adjust_flag(adjust_flag),
        ),
        action_name=f"拉取股票 {full_code} Baostock 日线",
    )
    if getattr(query_result, "error_code", "") != "0":
        raise RuntimeError(
            f"Baostock 拉取股票 {full_code} 失败: "
            f"{query_result.error_code} {query_result.error_msg}"
        )

    data_rows: list[list[str]] = []
    while query_result.next():
        data_rows.append(query_result.get_row_data())
    raw_df = pd.DataFrame(data_rows, columns=query_result.fields)
    return normalize_baostock_history_df(
        raw_df,
        full_code=full_code,
        adjust_flag=adjust_flag,
    )


def fetch_stock_history_from_tushare(
    ts_code: str,
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    if not is_unadjusted_flag(adjust_flag):
        raise RuntimeError(f"Tushare 仅支持 daily 原始日线，不支持 {adjust_flag} 复权口径")
    if ts is None:
        raise RuntimeError("未安装 tushare")

    start_date_compact = start_date.replace("-", "")
    end_date_compact = end_date.replace("-", "")

    raw_df = fetch_tushare_daily_data(
        ts_codes=[ts_code],
        start_date=start_date_compact,
        end_date=end_date_compact,
        action_name=f"拉取股票 {ts_code} Tushare 日线",
    )
    return normalize_tushare_stock_history_df(
        raw_df=raw_df,
        full_code=full_code,
        adjust_flag=adjust_flag,
    )


def fetch_tushare_daily_data(
    ts_codes: list[str],
    start_date: str,
    end_date: str,
    action_name: str,
) -> pd.DataFrame:
    if ts is None:
        raise RuntimeError("未安装 tushare")
    if not ts_codes:
        return pd.DataFrame()

    pro = get_tushare_pro_client()

    def _request():
        enforce_tushare_daily_rate_limit()
        return pro.daily(
            ts_code=",".join(ts_codes),
            start_date=start_date,
            end_date=end_date,
        )

    return retry_request_call(_request, action_name=action_name)


def fetch_stock_history_from_eastmoney(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    market_prefix = "sh" if symbol.startswith("6") else "sz"
    referer_url = f"https://quote.eastmoney.com/concept/{market_prefix}{symbol}.html"
    market_code = "1" if symbol.startswith("6") else "0"
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0", "3": "0", "cq": "0"}
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": adjust_dict.get(normalize_adjust_flag(adjust_flag), "0"),
        "secid": f"{market_code}.{symbol}",
        "beg": start_date.replace("-", ""),
        "end": end_date.replace("-", ""),
    }

    def request_with_cookie(cookie_str: str = "", session: requests.Session | None = None):
        request_func = (session or requests).get
        response = retry_request_call(
            lambda: request_func(
                url,
                params=params,
                headers=build_eastmoney_headers(referer_url, cookie_str),
                timeout=30,
            ),
            action_name=f"拉取股票 {symbol} 东方财富日线",
        )
        response.raise_for_status()
        return response

    cookie_str = load_eastmoney_cookie()
    try:
        response = request_with_cookie(cookie_str=cookie_str)
        data_json = response.json()
        if not data_json.get("data") or not data_json["data"].get("klines"):
            raise RuntimeError("东方财富接口返回空数据")
    except Exception as exc:
        logger.warning("东方财富接口首次请求失败，尝试刷新 cookie 后重试: %s", exc)
        session, refreshed_cookie = refresh_eastmoney_cookie()
        response = request_with_cookie(cookie_str=refreshed_cookie, session=session)
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
            adjust=to_akshare_adjust_flag(adjust_flag),
        ),
        action_name=f"拉取股票 {full_code} 新浪日线",
    )


def fetch_index_history(
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
    source_name: str = SYNC_SOURCE_AUTO,
) -> pd.DataFrame:
    source_errors: list[str] = []
    baostock_source = (
        "baostock",
        lambda: fetch_index_history_from_baostock(
            full_code=full_code,
            start_date=start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
        ),
    )
    akshare_source = (
        "eastmoney",
        lambda: normalize_index_history_df(
            fetch_index_history_from_eastmoney(
                symbol=full_code.split(".", 1)[1],
                start_date=start_date,
                end_date=end_date,
            ),
            full_code=full_code,
            adjust_flag=adjust_flag,
        ),
    )

    if source_name == SYNC_SOURCE_BAOSTOCK:
        index_sources = [baostock_source]
    elif source_name == SYNC_SOURCE_AKSHARE:
        index_sources = [akshare_source]
    elif source_name == SYNC_SOURCE_TUSHARE:
        raise RuntimeError("Tushare 暂不支持指数日线同步，请改用 Baostock 或 Akshare")
    else:
        index_sources = [baostock_source, akshare_source]

    for source_name, fetcher in index_sources:
        try:
            result_df = fetcher()
            if result_df.empty:
                logger.warning("%s 使用 %s 未拉到指数数据，尝试下一个源", full_code, source_name)
                source_errors.append(f"{source_name}: empty")
                continue
            logger.info("%s 使用 %s 拉取指数成功", full_code, source_name)
            return result_df
        except Exception as exc:
            logger.warning("%s 使用 %s 拉取指数失败: %s", full_code, source_name, exc)
            source_errors.append(f"{source_name}: {exc}")

    raise RuntimeError(f"{full_code} 所有指数数据源均失败: {' | '.join(source_errors)}")


def fetch_index_history_from_baostock(
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    ensure_baostock_login()
    query_result = retry_request_call(
        lambda: bs.query_history_k_data_plus(
            full_code,
            "date,code,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=to_baostock_adjust_flag(adjust_flag),
        ),
        action_name=f"拉取指数 {full_code} Baostock 日线",
    )
    if getattr(query_result, "error_code", "") != "0":
        raise RuntimeError(
            f"Baostock 拉取指数 {full_code} 失败: "
            f"{query_result.error_code} {query_result.error_msg}"
        )

    data_rows: list[list[str]] = []
    while query_result.next():
        data_rows.append(query_result.get_row_data())
    raw_df = pd.DataFrame(data_rows, columns=query_result.fields)
    return normalize_baostock_history_df(
        raw_df,
        full_code=full_code,
        adjust_flag=adjust_flag,
    )


def fetch_index_history_from_eastmoney(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    referer_url = EASTMONEY_COOKIE_BOOTSTRAP_URL
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
            def request_with_cookie(cookie_str: str = "", session: requests.Session | None = None):
                request_func = (session or requests).get
                response = retry_request_call(
                    lambda: request_func(
                        url,
                        params=params,
                        headers=build_eastmoney_headers(referer_url, cookie_str),
                        timeout=30,
                    ),
                    action_name=f"拉取指数 {symbol} 东方财富日线",
                )
                response.raise_for_status()
                return response

            cookie_str = load_eastmoney_cookie()
            try:
                response = request_with_cookie(cookie_str=cookie_str)
                data_json = response.json()
                if not data_json.get("data") or not data_json["data"].get("klines"):
                    raise RuntimeError("东方财富指数接口返回空数据")
            except Exception as exc:
                logger.warning("东方财富指数接口首次请求失败，尝试刷新 cookie 后重试: %s", exc)
                session, refreshed_cookie = refresh_eastmoney_cookie()
                response = request_with_cookie(cookie_str=refreshed_cookie, session=session)
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


def normalize_baostock_history_df(
    raw_df: pd.DataFrame,
    full_code: str,
    adjust_flag: str,
) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    normalized_df = raw_df.copy()
    normalized_df["date"] = pd.to_datetime(
        normalized_df["date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    normalized_df["code"] = full_code
    normalized_df["adjustflag"] = adjust_flag
    if "turn" not in normalized_df.columns:
        normalized_df["turn"] = 0
    if "pctChg" not in normalized_df.columns:
        normalized_df["pctChg"] = 0
    return finalize_history_df(normalized_df)


def normalize_tushare_stock_history_df(
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
    normalized_df["peTTM"] = pd.NA
    normalized_df["psTTM"] = pd.NA
    normalized_df["pbMRQ"] = pd.NA
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


def sync_one_code(
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
    source_name: str,
) -> str:
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
            source_name=source_name,
        )
    else:
        symbol = full_code.split(".", 1)[1]
        new_df = fetch_stock_history(
            symbol=symbol,
            start_date=request_start_date,
            end_date=end_date,
            adjust_flag=adjust_flag,
            source_name=source_name,
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
    source_name: str,
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
            logger.info("%s 同步 %s/%s: %s", batch_name, index, len(target_codes), full_code)
            result = sync_one_code(
                full_code=full_code,
                start_date=start_date,
                end_date=end_date,
                adjust_flag=adjust_flag,
                source_name=source_name,
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
    code_args, sync_all_sh_main, source_name = parse_cli_options()
    adjust_flag = resolve_effective_adjust_flag(CONFIG["adjust_flag"])
    start_date = resolve_sync_start_date()
    end_date = resolve_sync_end_date()
    target_codes = resolve_target_codes(code_args, sync_all_sh_main, source_name)

    if source_name == SYNC_SOURCE_AUTO:
        logger.info("使用 Baostock -> Akshare -> Tushare 的优先级同步所需数据")
    else:
        logger.info("按用户指定的数据源同步: %s", source_name)
    logger.info("目标代码: %s", ", ".join(target_codes))
    logger.info("同步区间: %s ~ %s", start_date, end_date)
    logger.info("数据复权口径: %s", adjust_flag)

    total_success, total_skipped, _, failed_codes = run_sync_batch(
        target_codes,
        start_date=start_date,
        end_date=end_date,
        adjust_flag=adjust_flag,
        source_name=source_name,
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
            source_name=source_name,
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

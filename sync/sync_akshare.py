from __future__ import annotations

import atexit
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from collections import deque
from datetime import date
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
from requests import exceptions as requests_exceptions

try:
    import browser_cookie3
except ImportError:
    browser_cookie3 = None

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

from backtest.backtest_v1 import CONFIG, TEST_CASES
from utils.ak_share_utils import get_a_share_code_name_df_and_filter
from utils.project_utils import (
    SUPPORTED_ADJUST_FLAGS,
    BASE_DAILY_COLUMNS,
    build_adjusted_daily_column_map,
    build_unified_daily_columns,
    get_daily_csv_path,
)
from utils.stock_utils import stock_is_ignored

logger = logging.getLogger(__name__)

SH_MAIN_BOARD_PREFIXES = ("600", "601", "603", "605")
EXCLUDED_NAME_KEYWORDS = ("融创",)
REQUEST_RETRY_TIMES = 3
REQUEST_RETRY_SLEEP_SECONDS = 2
CODE_SYNC_INTERVAL_SECONDS = 0.5
FAILED_QUEUE_RETRY_ROUNDS = 1
FAILED_QUEUE_RETRY_COOLDOWN_SECONDS = 8
EASTMONEY_COOKIE_BOOTSTRAP_URLS = (
    "https://quote.eastmoney.com/",
    "https://quote.eastmoney.com/center/gridlist.html",
    "https://quote.eastmoney.com/concept/sz000014.html",
)
EASTMONEY_COOKIE_CHROME_URL = "https://quote.eastmoney.com/"
EASTMONEY_COOKIE_CHROME_APP = "Google Chrome"
EASTMONEY_COOKIE_CHROME_TIMEOUT_SECONDS = 45
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
    "ex_right_close",
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
UNIFIED_STORAGE_COLUMNS = build_unified_daily_columns()

_TUSHARE_PRO_CLIENT = None
_TUSHARE_DAILY_CALL_TIMESTAMPS: deque[float] = deque()
_BAOSTOCK_LOGGED_IN = False
_BAOSTOCK_DISABLED = False
SYNC_SOURCE_AUTO = "auto"
SYNC_SOURCE_EASTMONEY = "eastmoney"
SYNC_SOURCE_BAOSTOCK = "baostock"
SYNC_SOURCE_AKSHARE = "akshare"
SYNC_SOURCE_TUSHARE = "tushare"
SYNC_SOURCE_THS = "ths"
SYNC_SOURCE_CHOICES = {
    SYNC_SOURCE_AUTO,
    SYNC_SOURCE_EASTMONEY,
    SYNC_SOURCE_BAOSTOCK,
    SYNC_SOURCE_AKSHARE,
    SYNC_SOURCE_TUSHARE,
    SYNC_SOURCE_THS,
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


def build_eastmoney_quote_url(full_code: str) -> str:
    market, symbol = full_code.split(".", 1)
    return f"https://quote.eastmoney.com/{market}{symbol}.html"


def resolve_eastmoney_cookie_bootstrap_urls(full_code: str) -> list[str]:
    urls = [build_eastmoney_quote_url(full_code), *EASTMONEY_COOKIE_BOOTSTRAP_URLS]
    deduplicated_urls: list[str] = []
    for url in urls:
        if url not in deduplicated_urls:
            deduplicated_urls.append(url)
    return deduplicated_urls


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


def build_ths_stock_page_url(symbol: str) -> str:
    return f"https://stockpage.10jqka.com.cn/{symbol}/"


def build_ths_headers(referer_url: str) -> dict[str, str]:
    return {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        "referer": referer_url,
        "accept": "application/javascript, application/json, text/plain, */*",
    }


def get_eastmoney_cookie_chrome_app_name() -> str:
    chrome_app_name = os.getenv("EASTMONEY_COOKIE_CHROME_APP", EASTMONEY_COOKIE_CHROME_APP).strip()
    if not chrome_app_name:
        return EASTMONEY_COOKIE_CHROME_APP
    return chrome_app_name


def refresh_eastmoney_cookie_from_chrome_profile() -> str:
    if browser_cookie3 is None:
        raise RuntimeError("未安装 browser_cookie3，无法直接读取 Chrome cookie")

    try:
        cookie_jar = browser_cookie3.chrome(domain_name=".eastmoney.com")
    except Exception as exc:
        raise RuntimeError(f"读取 Chrome cookie 库失败: {exc}") from exc

    cookie_str = format_cookie_header(cookie_jar)
    if not cookie_str:
        raise RuntimeError("Chrome cookie 库未返回有效的东方财富 cookie")
    write_eastmoney_cookie(cookie_str)
    logger.info("已从 Chrome cookie 库刷新东方财富 cookie")
    return cookie_str


def build_eastmoney_cookie_chrome_applescript(chrome_app_name: str, target_url: str) -> str:
    escaped_app_name = chrome_app_name.replace("\\", "\\\\").replace('"', '\\"')
    escaped_target_url = target_url.replace("\\", "\\\\").replace('"', '\\"')
    return f"""
set chromeAppName to "{escaped_app_name}"
set targetURL to "{escaped_target_url}"
using terms from application "Google Chrome"
    tell application chromeAppName
        activate
        if (count of windows) = 0 then
            make new window
        end if
        set targetWindow to front window
        open location targetURL
        repeat 60 times
            delay 0.5
            try
                set cookieText to execute javascript "document.cookie" in active tab of targetWindow
                if cookieText is not "" then
                    return cookieText
                end if
            end try
        end repeat
        return ""
    end tell
end using terms from
""".strip()


def refresh_eastmoney_cookie_from_chrome() -> str:
    profile_error: Exception | None = None
    try:
        return refresh_eastmoney_cookie_from_chrome_profile()
    except Exception as exc:
        profile_error = exc
        logger.warning("从 Chrome cookie 库获取东方财富 cookie 失败: %s", exc)

    if sys.platform != "darwin":
        raise RuntimeError(f"从 Chrome 自动获取东方财富 cookie 失败: {profile_error}")
    if shutil.which("osascript") is None:
        raise RuntimeError(f"从 Chrome 自动获取东方财富 cookie 失败: {profile_error}")

    chrome_app_name = get_eastmoney_cookie_chrome_app_name()
    script = build_eastmoney_cookie_chrome_applescript(
        chrome_app_name=chrome_app_name,
        target_url=EASTMONEY_COOKIE_CHROME_URL,
    )
    try:
        result = subprocess.run(
            ["osascript", "-"],
            input=script,
            text=True,
            capture_output=True,
            timeout=EASTMONEY_COOKIE_CHROME_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("从 Chrome 获取东方财富 cookie 超时") from exc

    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip() or f"退出码 {result.returncode}"
        raise RuntimeError(f"Chrome AppleScript 执行失败: {error_message}")

    cookie_str = result.stdout.strip()
    if not cookie_str:
        raise RuntimeError("Chrome 页面未返回有效 cookie")
    write_eastmoney_cookie(cookie_str)
    logger.info("已从 Chrome 浏览器刷新东方财富 cookie")
    return cookie_str


def refresh_eastmoney_cookie(bootstrap_urls: list[str]) -> tuple[requests.Session, str]:
    last_error: Exception | None = None
    try:
        cookie_str = refresh_eastmoney_cookie_from_chrome()
        return requests.Session(), cookie_str
    except Exception as exc:
        last_error = exc
        logger.warning("从 Chrome 浏览器获取东方财富 cookie 失败: %s", exc)

    for bootstrap_url in bootstrap_urls:
        session = requests.Session()
        try:
            response = retry_request_call(
                lambda: session.get(
                    bootstrap_url,
                    headers=build_eastmoney_headers(bootstrap_url),
                    timeout=30,
                ),
                action_name=f"访问东方财富页面获取 cookie: {bootstrap_url}",
            )
            response.raise_for_status()
            cookie_str = format_cookie_header(session.cookies)
            if not cookie_str:
                raise RuntimeError(f"页面 {bootstrap_url} 未返回有效 cookie")
            write_eastmoney_cookie(cookie_str)
            logger.info("东方财富 cookie 来源页面: %s", bootstrap_url)
            return session, cookie_str
        except Exception as exc:
            last_error = exc
            logger.warning("从页面 %s 获取东方财富 cookie 失败: %s", bootstrap_url, exc)

    if last_error is not None:
        logger.warning("所有东方财富 cookie 页面都失败，改用无 cookie 新会话继续重试: %s", last_error)
        return requests.Session(), ""

    logger.warning("未配置可用的东方财富 cookie 页面，改用无 cookie 新会话继续重试")
    return requests.Session(), ""


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
    if source_name == SYNC_SOURCE_EASTMONEY:
        stock_df = get_a_share_code_name_df_and_filter()
    elif source_name == SYNC_SOURCE_BAOSTOCK:
        stock_df = fetch_sh_main_stock_pool_from_baostock()
    elif source_name == SYNC_SOURCE_AKSHARE:
        stock_df = get_a_share_code_name_df_and_filter()
    elif source_name == SYNC_SOURCE_TUSHARE:
        stock_df = fetch_sh_main_stock_pool_from_tushare()
    else:
        try:
            stock_df = get_a_share_code_name_df_and_filter()
        except Exception as eastmoney_exc:
            logger.warning("东方财富股票池获取失败，回退到 Baostock: %s", eastmoney_exc)
            try:
                stock_df = fetch_sh_main_stock_pool_from_baostock()
            except Exception as exc:
                logger.warning("Baostock 股票池获取失败，回退到 Tushare: %s", exc)
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
    return normalize_adjust_flag(config_adjust_flag)


def normalize_adjust_flag(adjust_flag: str) -> str:
    normalized_flag = str(adjust_flag or "").strip().lower()
    if normalized_flag == "dypre":
        return "qfq"
    return normalized_flag


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


def to_ths_adjust_flag(adjust_flag: str) -> str:
    normalized_flag = normalize_adjust_flag(adjust_flag)
    if normalized_flag == "qfq":
        return "01"
    if normalized_flag == "hfq":
        return "02"
    if is_unadjusted_flag(normalized_flag):
        return "00"
    raise ValueError(f"不支持的同花顺复权口径: {adjust_flag}")


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
    eastmoney_source = (
        "eastmoney-direct",
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
    )
    ths_source = (
        "ths-direct",
        lambda: normalize_ths_stock_history_df(
            fetch_stock_history_from_ths(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust_flag=adjust_flag,
            ),
            full_code=full_code,
            adjust_flag=adjust_flag,
        ),
    )
    akshare_sources = [
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

    if source_name == SYNC_SOURCE_TUSHARE:
        if not is_unadjusted_flag(adjust_flag):
            raise RuntimeError(f"Tushare 仅支持不复权日线，不支持 {adjust_flag}")
        stock_sources = [tushare_source]
    elif source_name == SYNC_SOURCE_EASTMONEY:
        stock_sources = [eastmoney_source, ths_source, *akshare_sources, baostock_source]
        if is_unadjusted_flag(adjust_flag):
            stock_sources.append(tushare_source)
    elif source_name == SYNC_SOURCE_THS:
        stock_sources = [ths_source, eastmoney_source, *akshare_sources, baostock_source]
        if is_unadjusted_flag(adjust_flag):
            stock_sources.append(tushare_source)
    elif source_name == SYNC_SOURCE_BAOSTOCK:
        stock_sources = [baostock_source, eastmoney_source, ths_source, *akshare_sources]
        if is_unadjusted_flag(adjust_flag):
            stock_sources.append(tushare_source)
    elif source_name == SYNC_SOURCE_AKSHARE:
        stock_sources = [*akshare_sources, eastmoney_source, ths_source, baostock_source]
        if is_unadjusted_flag(adjust_flag):
            stock_sources.append(tushare_source)
    else:
        stock_sources = [ths_source, eastmoney_source, *akshare_sources, baostock_source]
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


def parse_eastmoney_kline_df(klines: list[str]) -> pd.DataFrame:
    if not klines:
        return pd.DataFrame()

    expected_columns = [
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
    row_values = [item.split(",") for item in klines]
    column_count = len(row_values[0])
    if column_count < len(expected_columns):
        raise RuntimeError(
            f"东方财富 K 线字段数量不足: 期望至少 {len(expected_columns)} 列，实际 {column_count} 列"
        )
    return pd.DataFrame(
        [row[: len(expected_columns)] for row in row_values],
        columns=expected_columns,
    )


def fetch_stock_history_from_eastmoney(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    full_code = to_full_code(symbol)
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
        session, refreshed_cookie = refresh_eastmoney_cookie(
            resolve_eastmoney_cookie_bootstrap_urls(full_code)
        )
        response = request_with_cookie(cookie_str=refreshed_cookie, session=session)
        data_json = response.json()
        if not data_json.get("data") or not data_json["data"].get("klines"):
            return pd.DataFrame()

    return parse_eastmoney_kline_df(data_json["data"]["klines"])


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


def parse_ths_history_df(response_text: str) -> pd.DataFrame:
    text = str(response_text or "").strip()
    if not text:
        return pd.DataFrame(
            columns=["date", "open", "high", "low", "close", "volume", "amount"]
        )
    if "Nginx forbidden" in text or "访问频率过快" in text:
        raise RuntimeError("同花顺返回了访问限制页面")

    payload_start = text.find("{")
    payload_end = text.rfind("}")
    if payload_start < 0 or payload_end < payload_start:
        raise RuntimeError("同花顺日线响应无法解析 JSON")

    payload = json.loads(text[payload_start : payload_end + 1])
    data_text = payload.get("data")
    if data_text is None and len(payload) == 1:
        only_value = next(iter(payload.values()))
        if isinstance(only_value, dict):
            data_text = only_value.get("data")
    if not data_text:
        return pd.DataFrame(
            columns=["date", "open", "high", "low", "close", "volume", "amount"]
        )

    rows: list[dict[str, str]] = []
    for raw_line in str(data_text).split(";"):
        line = raw_line.strip()
        if not line:
            continue
        parts = [item.strip() for item in line.split(",")]
        if len(parts) < 7:
            continue
        trade_date = parts[0]
        if len(trade_date) != 8 or not trade_date.isdigit():
            continue
        rows.append(
            {
                "date": (
                    f"{trade_date[0:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                ),
                "open": parts[1],
                "high": parts[2],
                "low": parts[3],
                "close": parts[4],
                "volume": parts[5],
                "amount": parts[6],
            }
        )

    return pd.DataFrame(
        rows,
        columns=["date", "open", "high", "low", "close", "volume", "amount"],
    )


def fetch_stock_history_from_ths(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
) -> pd.DataFrame:
    referer_url = build_ths_stock_page_url(symbol)
    url = (
        f"https://d.10jqka.com.cn/v6/line/hs_{symbol}/"
        f"{to_ths_adjust_flag(adjust_flag)}/last36000.js"
    )
    response = retry_request_call(
        lambda: requests.get(
            url,
            headers=build_ths_headers(referer_url),
            timeout=30,
        ),
        action_name=f"拉取股票 {symbol} 同花顺日线",
    )
    response.raise_for_status()
    raw_df = parse_ths_history_df(response.text)
    if raw_df.empty:
        return raw_df
    date_mask = (raw_df["date"] >= start_date) & (raw_df["date"] <= end_date)
    return raw_df.loc[date_mask].reset_index(drop=True)


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

    if source_name == SYNC_SOURCE_TUSHARE:
        raise RuntimeError("Tushare 暂不支持指数日线同步，请改用 Baostock 或 Akshare")
    elif source_name == SYNC_SOURCE_BAOSTOCK:
        index_sources = [baostock_source]
    else:
        index_sources = [akshare_source, baostock_source]

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
    full_code = to_full_code(symbol)
    referer_url = build_eastmoney_quote_url(full_code)
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
                session, refreshed_cookie = refresh_eastmoney_cookie(
                    resolve_eastmoney_cookie_bootstrap_urls(full_code)
                )
                response = request_with_cookie(cookie_str=refreshed_cookie, session=session)
                data_json = response.json()
                if not data_json.get("data") or not data_json["data"].get("klines"):
                    continue

            return parse_eastmoney_kline_df(data_json["data"]["klines"])
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

    column_aliases = {
        "date": ("date", "Date", "日期"),
        "open": ("open", "Open", "开盘"),
        "high": ("high", "High", "最高"),
        "low": ("low", "Low", "最低"),
        "close": ("close", "Close", "收盘"),
        "volume": ("volume", "Volume", "成交量"),
        "amount": ("amount", "Amount", "成交额"),
        "turnover": ("turnover", "Turnover", "换手率"),
    }
    rename_map: dict[str, str] = {}
    for normalized_name, candidate_names in column_aliases.items():
        for candidate_name in candidate_names:
            if candidate_name in raw_df.columns:
                rename_map[candidate_name] = normalized_name
                break

    normalized_df = raw_df.rename(columns=rename_map).copy()
    required_columns = ["date", "open", "high", "low", "close", "volume"]
    missing_columns = [column for column in required_columns if column not in normalized_df.columns]
    if missing_columns:
        raise RuntimeError(f"Sina 日线字段缺失: {', '.join(missing_columns)}")
    if "amount" not in normalized_df.columns:
        normalized_df["amount"] = 0
    if "turnover" not in normalized_df.columns:
        normalized_df["turnover"] = 0
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


def normalize_ths_stock_history_df(
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
    normalized_df["turn"] = 0
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


def merge_ex_right_close_by_date(
    target_df: pd.DataFrame,
    source_df: pd.DataFrame | None,
) -> pd.DataFrame:
    frame = target_df.copy()
    if frame.empty:
        if "ex_right_close" not in frame.columns:
            frame["ex_right_close"] = pd.Series(dtype=float)
        return frame

    if "ex_right_close" not in frame.columns:
        frame["ex_right_close"] = pd.NA

    if source_df is None or source_df.empty:
        return frame

    ex_right_frame = source_df.copy()
    required_columns = {"date", "close"}
    if not required_columns.issubset(ex_right_frame.columns):
        return frame

    ex_right_frame = ex_right_frame[["date", "close"]].copy()
    ex_right_frame["date"] = pd.to_datetime(
        ex_right_frame["date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    ex_right_frame["close"] = pd.to_numeric(ex_right_frame["close"], errors="coerce")
    ex_right_frame = ex_right_frame.dropna(subset=["date"])
    ex_right_frame = ex_right_frame.drop_duplicates(subset=["date"], keep="last")
    ex_right_frame = ex_right_frame.rename(columns={"close": "_ex_right_close"})

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame = frame.merge(ex_right_frame, on="date", how="left")
    frame["ex_right_close"] = pd.to_numeric(frame["ex_right_close"], errors="coerce")
    frame["ex_right_close"] = frame["ex_right_close"].fillna(frame["_ex_right_close"])
    return frame.drop(columns=["_ex_right_close"])


def read_local_ex_right_history(full_code: str) -> pd.DataFrame:
    csv_path = get_daily_csv_path(full_code, "cq")
    if not csv_path.exists():
        return pd.DataFrame(columns=["date", "close"])

    local_df = pd.read_csv(csv_path)
    if local_df.empty:
        return pd.DataFrame(columns=["date", "close"])
    if {"date", "close"} <= set(local_df.columns):
        return local_df[["date", "close"]].copy()
    if {"date", "cq_close"} <= set(local_df.columns):
        return local_df[["date", "cq_close"]].rename(columns={"cq_close": "close"}).copy()
    return pd.DataFrame(columns=["date", "close"])


def attach_ex_right_close_for_sync(
    history_df: pd.DataFrame,
    *,
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
    source_name: str,
    is_index: bool = False,
) -> pd.DataFrame:
    frame = history_df.copy()
    if frame.empty:
        if "ex_right_close" not in frame.columns:
            frame["ex_right_close"] = pd.Series(dtype=float)
        return frame

    if is_index or is_unadjusted_flag(adjust_flag):
        frame["ex_right_close"] = pd.to_numeric(frame["close"], errors="coerce")
        return frame

    frame = merge_ex_right_close_by_date(frame, read_local_ex_right_history(full_code))
    missing_mask = pd.isna(frame["ex_right_close"])
    if not missing_mask.any():
        return frame

    try:
        if is_index:
            ex_right_df = fetch_index_history(
                full_code=full_code,
                start_date=start_date,
                end_date=end_date,
                adjust_flag="cq",
                source_name=source_name,
            )
        else:
            ex_right_df = fetch_stock_history(
                symbol=full_code.split(".", 1)[1],
                start_date=start_date,
                end_date=end_date,
                adjust_flag="cq",
                source_name=source_name,
            )
    except Exception as exc:
        logger.warning("%s 拉取除权价格失败，保留空值: %s", full_code, exc)
        return frame

    return merge_ex_right_close_by_date(frame, ex_right_df)


def backfill_ex_right_close_from_local_history(history_df: pd.DataFrame) -> pd.DataFrame:
    frame = history_df.copy()
    if frame.empty:
        if "ex_right_close" not in frame.columns:
            frame["ex_right_close"] = pd.Series(dtype=float)
        return frame

    if "ex_right_close" not in frame.columns:
        frame["ex_right_close"] = pd.NA

    adjust_flag = str(frame.get("adjustflag", pd.Series(dtype=str)).iloc[0] or "").strip()
    if is_unadjusted_flag(adjust_flag):
        frame["ex_right_close"] = pd.to_numeric(frame["close"], errors="coerce")
        return frame

    code_series = frame.get("code", pd.Series(dtype=str)).dropna()
    if code_series.empty:
        return frame

    full_code = str(code_series.iloc[0]).strip()
    if not full_code:
        return frame

    if pd.notna(pd.to_numeric(frame["ex_right_close"], errors="coerce")).all():
        return frame
    return merge_ex_right_close_by_date(frame, read_local_ex_right_history(full_code))


def finalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    normalized_df["open"] = pd.to_numeric(normalized_df["open"], errors="coerce")
    normalized_df["high"] = pd.to_numeric(normalized_df["high"], errors="coerce")
    normalized_df["low"] = pd.to_numeric(normalized_df["low"], errors="coerce")
    normalized_df["close"] = pd.to_numeric(normalized_df["close"], errors="coerce")
    if "ex_right_close" not in normalized_df.columns:
        normalized_df["ex_right_close"] = pd.NA
    normalized_df["ex_right_close"] = pd.to_numeric(
        normalized_df["ex_right_close"], errors="coerce"
    )
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
        return pd.DataFrame(columns=UNIFIED_STORAGE_COLUMNS)
    existing_df = pd.read_csv(csv_path)
    if existing_df.empty:
        return pd.DataFrame(columns=UNIFIED_STORAGE_COLUMNS)
    return existing_df


def extract_adjusted_history(existing_df: pd.DataFrame, adjust_flag: str) -> pd.DataFrame:
    if existing_df is None or existing_df.empty:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    if set(STORAGE_COLUMNS).issubset(existing_df.columns):
        frame = existing_df[STORAGE_COLUMNS].copy()
        frame["adjustflag"] = adjust_flag
        return frame

    column_map = build_adjusted_daily_column_map(adjust_flag)
    extracted_df = pd.DataFrame()
    if "date" in existing_df.columns:
        extracted_df["date"] = existing_df["date"]
    if "code" in existing_df.columns:
        extracted_df["code"] = existing_df["code"]

    for normalized_column in BASE_DAILY_COLUMNS:
        storage_column = column_map[normalized_column]
        if storage_column in existing_df.columns:
            extracted_df[normalized_column] = existing_df[storage_column]

    if extracted_df.empty or "date" not in extracted_df.columns:
        return pd.DataFrame(columns=STORAGE_COLUMNS)

    if "code" not in extracted_df.columns:
        extracted_df["code"] = pd.NA
    if "adjustflag" not in extracted_df.columns:
        extracted_df["adjustflag"] = adjust_flag

    for column in STORAGE_COLUMNS:
        if column not in extracted_df.columns:
            extracted_df[column] = pd.NA
    return extracted_df[STORAGE_COLUMNS].dropna(subset=["date"]).reset_index(drop=True)


def compute_incremental_start_date(existing_df: pd.DataFrame, default_start_date: str) -> str:
    if existing_df.empty or "date" not in existing_df.columns:
        return default_start_date

    latest_date = pd.to_datetime(existing_df["date"], errors="coerce").dropna()
    if latest_date.empty:
        return default_start_date

    # 不复权数据也要回刷最后一个交易日，避免盘中/盘后修正永远覆盖不到。
    return latest_date.max().date().isoformat()


def should_full_refresh_history(adjust_flag: str) -> bool:
    return not is_unadjusted_flag(adjust_flag)


def merge_and_save_history(
    csv_path: Path,
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    full_refresh: bool = False,
) -> int:
    if full_refresh:
        combined_df = new_df.copy()
    else:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["date", "code"], keep="last")
    combined_df = combined_df.sort_values("date").reset_index(drop=True)
    combined_df = backfill_ex_right_close_from_local_history(combined_df)
    combined_df["preclose"] = pd.to_numeric(combined_df["close"], errors="coerce").shift(1)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(csv_path, index=False, encoding="utf-8")
    return len(new_df)


def merge_history_frames(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    full_refresh: bool = False,
) -> pd.DataFrame:
    if full_refresh:
        combined_df = new_df.copy()
    else:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["date", "code"], keep="last")
    combined_df = combined_df.sort_values("date").reset_index(drop=True)
    combined_df["preclose"] = pd.to_numeric(combined_df["close"], errors="coerce").shift(1)
    return combined_df


def build_unified_history_df(
    full_code: str,
    histories_by_adjust: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    date_values: set[str] = set()
    for history_df in histories_by_adjust.values():
        if history_df is None or history_df.empty:
            continue
        date_values.update(str(item) for item in history_df["date"].dropna().tolist())

    if not date_values:
        return pd.DataFrame(columns=UNIFIED_STORAGE_COLUMNS)

    unified_df = pd.DataFrame({"date": sorted(date_values)})
    unified_df["code"] = full_code

    for adjust_flag in SUPPORTED_ADJUST_FLAGS:
        history_df = histories_by_adjust.get(adjust_flag)
        column_map = build_adjusted_daily_column_map(adjust_flag)
        if history_df is None or history_df.empty:
            for storage_column in column_map.values():
                unified_df[storage_column] = pd.NA
            continue

        prefixed_df = history_df.copy()
        prefixed_df["date"] = pd.to_datetime(
            prefixed_df["date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        rename_map = {
            column: storage_column
            for column, storage_column in column_map.items()
            if column in prefixed_df.columns
        }
        prefixed_df = prefixed_df.rename(columns=rename_map)
        merge_columns = ["date", *rename_map.values()]
        prefixed_df = prefixed_df[merge_columns].drop_duplicates(subset=["date"], keep="last")
        unified_df = unified_df.merge(prefixed_df, on="date", how="left")

    for column in UNIFIED_STORAGE_COLUMNS:
        if column not in unified_df.columns:
            unified_df[column] = pd.NA
    return unified_df[UNIFIED_STORAGE_COLUMNS].sort_values("date").reset_index(drop=True)


def sync_one_code(
    full_code: str,
    start_date: str,
    end_date: str,
    adjust_flag: str,
    source_name: str,
) -> str:
    csv_path = get_daily_csv_path(full_code, adjust_flag)
    existing_bundle_df = read_existing_history(csv_path)
    merged_histories: dict[str, pd.DataFrame] = {}
    total_write_count = 0

    for current_adjust_flag in SUPPORTED_ADJUST_FLAGS:
        existing_df = extract_adjusted_history(existing_bundle_df, current_adjust_flag)
        full_refresh = should_full_refresh_history(current_adjust_flag)
        request_start_date = (
            start_date
            if full_refresh
            else compute_incremental_start_date(existing_df, start_date)
        )

        if request_start_date > end_date:
            logger.info("%s %s 已是最新，跳过该口径", full_code, current_adjust_flag)
            merged_histories[current_adjust_flag] = existing_df
            continue

        sync_mode = "全量刷新" if full_refresh else "增量回刷"
        logger.info(
            "开始同步 %s %s, 模式=%s, 区间 %s ~ %s",
            full_code,
            current_adjust_flag,
            sync_mode,
            request_start_date,
            end_date,
        )

        if full_code == CONFIG["benchmark_code"]:
            new_df = fetch_index_history(
                full_code=full_code,
                start_date=request_start_date,
                end_date=end_date,
                adjust_flag=current_adjust_flag,
                source_name=source_name,
            )
        else:
            symbol = full_code.split(".", 1)[1]
            new_df = fetch_stock_history(
                symbol=symbol,
                start_date=request_start_date,
                end_date=end_date,
                adjust_flag=current_adjust_flag,
                source_name=source_name,
            )

        if new_df.empty:
            if existing_df.empty:
                logger.warning("%s %s 没有拉到数据", full_code, current_adjust_flag)
                return "empty"
            merged_histories[current_adjust_flag] = existing_df
            continue

        total_write_count += len(new_df)
        merged_histories[current_adjust_flag] = merge_history_frames(
            existing_df,
            new_df,
            full_refresh=full_refresh,
        )

    cq_history_df = merged_histories.get("cq", pd.DataFrame(columns=STORAGE_COLUMNS)).copy()
    if not cq_history_df.empty:
        cq_history_df["ex_right_close"] = pd.to_numeric(cq_history_df["close"], errors="coerce")
        merged_histories["cq"] = cq_history_df
        cq_ex_right_df = cq_history_df[["date", "close"]].copy()
        for current_adjust_flag in ("qfq", "hfq"):
            if current_adjust_flag not in merged_histories:
                continue
            merged_histories[current_adjust_flag] = merge_ex_right_close_by_date(
                merged_histories[current_adjust_flag],
                cq_ex_right_df,
            )

    unified_df = build_unified_history_df(full_code, merged_histories)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    unified_df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info("%s 同步完成，合并写入 %s 条", full_code, total_write_count)
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
        logger.info("使用 同花顺 -> 东方财富直连 -> Akshare -> Baostock -> Tushare 的优先级同步所需数据")
    else:
        logger.info("优先使用用户指定的数据源同步，失败后按固定顺序回退: %s", source_name)
    logger.info("目标代码: %s", ", ".join(target_codes))
    logger.info("同步区间: %s ~ %s", start_date, end_date)
    logger.info("数据写入口径: cq + qfq + hfq（当前默认回测口径: %s）", adjust_flag)

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

from __future__ import annotations

from pathlib import Path

DEFAULT_STOCK_NAMES = {
    "sz.002409": "雅克科技",
    "sz.000021": "深科技",
    "sh.600378": "昊华科技",
    "sh.603650": "彤程新材",
    "sh.600110": "诺德股份",
    "sh.601991": "大唐发电",
    "sz.001696": "宗申动力",
    "sh.603283": "赛腾股份",
    "sh.603601": "再升科技",
    # --- 其他 ---
    "sz.002392": "北京利尔",
    "sh.600162": "香江控股",
}

DEFAULT_STOCK_CODES = tuple(DEFAULT_STOCK_NAMES.keys())
DEFAULT_PRIMARY_STOCK_CODE = DEFAULT_STOCK_CODES[0]
DEFAULT_BASE_STRATEGY_ID = "Versatile"
DEFAULT_BASE_STRATEGY_NAME = "Versatile"
DEFAULT_BASE_STRATEGY_SOURCE = "backtest/versatile.py"


def normalize_strategy_source_module(source: str) -> str:
    normalized = str(source or "").strip().replace("\\", "/")
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    normalized = normalized.strip("/")
    if not normalized:
        raise ValueError("默认策略源码不能为空")

    module_parts = [part for part in normalized.split("/") if part]
    if not module_parts:
        raise ValueError("默认策略源码格式错误")
    return ".".join(module_parts)


DEFAULT_BASE_STRATEGY_MODULE_NAME = normalize_strategy_source_module(
    DEFAULT_BASE_STRATEGY_SOURCE
)
DEFAULT_BASE_STRATEGY_FILE_STEM = Path(
    DEFAULT_BASE_STRATEGY_SOURCE.replace("\\", "/")
).stem


def build_default_stock_test_cases() -> list[dict[str, str]]:
    return [{"code": code, "expect": ""} for code in DEFAULT_STOCK_CODES]

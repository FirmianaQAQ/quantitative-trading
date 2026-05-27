from __future__ import annotations

from pathlib import Path

DEFAULT_STOCK_NAMES = {
    "sz.000100": "TCL科技",
    "sz.000725": "京东方A",
    "sh.601991": "大唐发电",
    "sh.600236": "桂冠电力",
    "sh.600036": "招商银行",
    "sh.605006": "山东玻纤",
    "sh.600029": "南方航空",
    "sh.600580": "卧龙电驱",
    "sz.001308": "康冠科技",
    "sh.600726": "华电能源",
    "sz.002421": "达实智能",
    "sh.600578": "京能电力",
    "sh.600396": "华电辽能",
    "sh.600186": "莲花控股",
    "sz.002185": "华天科技",
    "sh.600388": "龙净环保",
    "sz.002962": "五方光电",
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

from __future__ import annotations

DEFAULT_STOCK_NAMES = {
    "sh.600580": "卧龙电驱",
    "sz.000100": "TCL科技",
    "sz.000725": "京东方A",
    "sz.001308": "康冠科技",
    "sz.002594": "比亚迪",
    "sh.600255": "鑫科材料",
    "sh.002340": "格林美",
}

DEFAULT_STOCK_CODES = tuple(DEFAULT_STOCK_NAMES.keys())
DEFAULT_PRIMARY_STOCK_CODE = DEFAULT_STOCK_CODES[0]


def build_default_stock_test_cases() -> list[dict[str, str]]:
    return [{"code": code, "expect": ""} for code in DEFAULT_STOCK_CODES]

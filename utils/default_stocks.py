from __future__ import annotations

DEFAULT_STOCK_NAMES = {
    "sz.000100": "TCL科技",
    "sz.000725": "京东方A",
    "sh.601991": "大唐发电",
    "sh.600236": "桂冠电力",
    "sh.600036": "招商银行",
    "sh.605006": "山东玻纤",
    "sh.600029": "南方航空",
    "sh.002624": "完美世界",
    "sh.600690": "海尔智家",
    "sh.600406": "国电南瑞",
    "sh.600580": "卧龙电驱",
    "sz.001308": "康冠科技",
}

DEFAULT_STOCK_CODES = tuple(DEFAULT_STOCK_NAMES.keys())
DEFAULT_PRIMARY_STOCK_CODE = DEFAULT_STOCK_CODES[0]
DEFAULT_BASE_STRATEGY_NAME = "宽论策略"
DEFAULT_BASE_STRATEGY_BRIEF = "BMK"


def build_default_stock_test_cases() -> list[dict[str, str]]:
    return [{"code": code, "expect": ""} for code in DEFAULT_STOCK_CODES]

from __future__ import annotations

import socket
from pathlib import Path

socket.setdefaulttimeout(120)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_PATH = PROJECT_ROOT / "data"
DEFAULT_A_SHARE_ADJUST = "hfq"
ignore_stock_code = {
    "300385",  # 雪浪环境，长期停盘
}
ignore_st = True  # 是否在分析中默认忽略 ST 股票，这种股票通常有特殊风险，可能不适合大多数投资策略
ignore_stock_code_prefixes = {  # 是否忽略特定前缀的股票代码，如科创板和创业板
    "688", # 科创板 涨跌停±20% 要求50万+2年经验
    "300","301", # 创业板 涨跌停±20% 要求10万+1年经验
    "8", "4", # 北交所 涨跌停±30% 要求10万+1年经验
}

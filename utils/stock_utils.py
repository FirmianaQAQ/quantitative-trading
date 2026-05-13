from utils.config import ignore_stock_code, ignore_st, ignore_stock_code_prefixes


def stock_is_ignored(stock_code: str, stock_name: str, market="") -> bool:
    """
    判断是否忽略该股票
    market是股票市场，sh=上海证券交易所，sz=深圳证券交易所，bj=北京证券交易所
    """
    if ignore_st and "ST" in stock_name:
        return True
    if market and market not in ("sh", "sz", "bj"):
        return True
    for prefix in ignore_stock_code_prefixes:
        if stock_code.startswith(prefix):
            return True
    return stock_code in ignore_stock_code

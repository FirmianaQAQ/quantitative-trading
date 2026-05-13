
from datetime import date, datetime
import pandas as pd

def normalize_date(base_date: date | datetime | str | None = None) -> date:
    """将输入的日期规范化为 date 对象，支持多种输入类型"""
    if base_date is None:
        return date.today()
    if isinstance(base_date, datetime):
        return base_date.date()
    if isinstance(base_date, date):
        return base_date
    return pd.to_datetime(base_date).date()

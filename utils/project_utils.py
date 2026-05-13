from pathlib import Path

import pandas as pd

from utils.config import DATA_PATH

ADJUST_FLAG_NAMES = {
    "1": "hfq",  # 后复权
    "2": "qfq",  # 前复权
    "3": "cq",  # 不复权
}


def get_daily_csv_path(code: str, adjust_flag: str) -> Path:
    """
    获取CSV文件路径

    Args:
        code: 股票代码
        adjust_flag: 复权标志


    Returns:
        Path: CSV文件路径
    """
    adjust_name = ADJUST_FLAG_NAMES.get(adjust_flag, adjust_flag)
    daily_dir = get_daily_dir()
    return daily_dir / f"{code}_{adjust_name}.csv"


def get_daily_dir() -> Path:
    """
    获取日线数据存储目录

    Returns:
        Path: 日线数据目录路径
    """
    daily_dir = DATA_PATH / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    return daily_dir


def load_daily_data(code: str, adjust_flag: str) -> pd.DataFrame:
    """加载日线数据并进行基本清洗"""
    csv_path = get_daily_csv_path(code, adjust_flag)
    if not csv_path.exists():
        raise FileNotFoundError(f"找不到日线数据文件: {csv_path}")

    df = pd.read_csv(csv_path)
    required_columns = ["date", "open", "high", "low", "close", "volume", "turn"]
    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"日线数据缺少字段: {missing_columns}")

    numeric_columns = ["open", "high", "low", "close", "volume", "turn"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=required_columns)
    df = df.sort_values("date").reset_index(drop=True)
    df = df[required_columns]
    return df

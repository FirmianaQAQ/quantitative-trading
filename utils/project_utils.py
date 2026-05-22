from pathlib import Path

import pandas as pd

from utils.config import DATA_PATH

SUPPORTED_ADJUST_FLAGS = ("cq", "qfq", "hfq")
BASE_DAILY_COLUMNS = [
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
REQUIRED_PRICE_COLUMNS = ["date", "open", "high", "low", "close", "volume", "turn"]

ADJUST_FLAG_NAMES = {
    "1": "hfq",  # 后复权
    "2": "qfq",  # 前复权
    "3": "cq",  # 不复权
    "dypre": "qfq",  # 动态前复权在当前策略里等价映射到前复权列
}


def normalize_adjust_flag_name(adjust_flag: str) -> str:
    normalized = ADJUST_FLAG_NAMES.get(str(adjust_flag or "").strip(), str(adjust_flag or "").strip())
    normalized = normalized.lower()
    if normalized in {"", "bfq", "raw", "none"}:
        return "cq"
    if normalized not in SUPPORTED_ADJUST_FLAGS:
        raise ValueError(f"不支持的复权口径: {adjust_flag}")
    return normalized


def build_adjusted_daily_column_map(adjust_flag: str) -> dict[str, str]:
    normalized_flag = normalize_adjust_flag_name(adjust_flag)
    return {
        column: f"{normalized_flag}_{column}"
        for column in BASE_DAILY_COLUMNS
    }


def _build_selected_adjusted_frame(df: pd.DataFrame, adjust_flag: str) -> pd.DataFrame:
    adjust_column_map = build_adjusted_daily_column_map(adjust_flag)
    selected_df = pd.DataFrame()
    if "date" in df.columns:
        selected_df["date"] = df["date"]

    for normalized_column, storage_column in adjust_column_map.items():
        if storage_column in df.columns:
            selected_df[normalized_column] = df[storage_column]

    return selected_df


def _build_dypre_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    qfq_df = _build_selected_adjusted_frame(df, "qfq")
    cq_df = _build_selected_adjusted_frame(df, "cq")
    required_columns = set(REQUIRED_PRICE_COLUMNS)
    if not required_columns.issubset(qfq_df.columns):
        missing_columns = sorted(required_columns - set(qfq_df.columns))
        raise ValueError(f"Dypre 日线缺少前复权字段: {missing_columns}")
    if not required_columns.issubset(cq_df.columns):
        missing_columns = sorted(required_columns - set(cq_df.columns))
        raise ValueError(f"Dypre 日线缺少不复权字段: {missing_columns}")

    # 某些增量同步场景下，最新交易日可能已经写入了 cq / hfq，
    # 但 qfq 仍为空。这里保留 qfq 优先级，只在 qfq 缺值时用同日 cq 回填，
    # 避免最新一根被静默丢弃，导致报告和策略停留在前一日。
    selected_df = (
        qfq_df.set_index("date")
        .combine_first(cq_df.set_index("date"))
        .reset_index()
    )
    selected_df["raw_open"] = pd.to_numeric(cq_df["open"], errors="coerce")
    selected_df["raw_high"] = pd.to_numeric(cq_df["high"], errors="coerce")
    selected_df["raw_low"] = pd.to_numeric(cq_df["low"], errors="coerce")
    selected_df["raw_close"] = pd.to_numeric(cq_df["close"], errors="coerce")
    selected_df["raw_preclose"] = pd.to_numeric(cq_df.get("preclose"), errors="coerce")
    selected_df["ex_right_close"] = pd.to_numeric(cq_df["close"], errors="coerce")

    signal_close = pd.to_numeric(selected_df["close"], errors="coerce")
    raw_close = pd.to_numeric(selected_df["raw_close"], errors="coerce")
    signal_factor = (signal_close / raw_close).where(raw_close > 0)
    signal_factor = signal_factor.replace([float("inf"), float("-inf")], pd.NA)
    signal_factor = signal_factor.ffill().fillna(1.0)
    position_adjust_ratio = (signal_factor / signal_factor.shift(1)).replace(
        [float("inf"), float("-inf")],
        pd.NA,
    )
    position_adjust_ratio = position_adjust_ratio.fillna(1.0)

    selected_df["signal_factor"] = pd.to_numeric(signal_factor, errors="coerce").fillna(1.0)
    selected_df["position_adjust_ratio"] = pd.to_numeric(
        position_adjust_ratio,
        errors="coerce",
    ).fillna(1.0)
    return selected_df


def build_unified_daily_columns() -> list[str]:
    columns = ["date", "code"]
    for adjust_flag in SUPPORTED_ADJUST_FLAGS:
        column_map = build_adjusted_daily_column_map(adjust_flag)
        columns.extend(column_map[column] for column in BASE_DAILY_COLUMNS)
    return columns


def get_daily_csv_path(code: str, adjust_flag: str) -> Path:
    """
    获取CSV文件路径

    Args:
        code: 股票代码
        adjust_flag: 复权标志


    Returns:
        Path: CSV文件路径
    """
    daily_dir = get_daily_dir()
    return daily_dir / f"{code}.csv"


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
    raw_adjust_flag = str(adjust_flag or "").strip().lower()
    if raw_adjust_flag == "dypre":
        selected_df = _build_dypre_price_frame(df)
    else:
        selected_df = _build_selected_adjusted_frame(df, adjust_flag)

    missing_columns = [column for column in REQUIRED_PRICE_COLUMNS if column not in selected_df.columns]
    if missing_columns:
        raise ValueError(f"日线数据缺少字段: {missing_columns}")

    optional_columns = [
        column
        for column in [
            "ex_right_close",
            "raw_open",
            "raw_high",
            "raw_low",
            "raw_close",
            "raw_preclose",
            "signal_factor",
            "position_adjust_ratio",
        ]
        if column in selected_df.columns
    ]
    numeric_columns = ["open", "high", "low", "close", "volume", "turn", *optional_columns]
    for column in numeric_columns:
        selected_df[column] = pd.to_numeric(selected_df[column], errors="coerce")

    selected_df["date"] = pd.to_datetime(selected_df["date"], errors="coerce")
    selected_df = selected_df.dropna(subset=REQUIRED_PRICE_COLUMNS)
    selected_df = selected_df.sort_values("date").reset_index(drop=True)
    selected_df = selected_df[[*REQUIRED_PRICE_COLUMNS, *optional_columns]]
    return selected_df

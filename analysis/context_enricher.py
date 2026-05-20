from __future__ import annotations

from datetime import timedelta
from typing import Any

import akshare as ak
import pandas as pd


DEFAULT_NEWS_LOOKBACK_DAYS = 20
DEFAULT_NEWS_LIMIT = 10


def enrich_single_stock_context(
    config: dict[str, Any],
    df: pd.DataFrame,
) -> dict[str, Any]:
    full_code = str(config.get("code") or "").strip().lower()
    if not full_code:
        return {
            "as_of_date": None,
            "news": _build_unavailable_payload("缺少股票代码"),
            "fund_flow": _build_unavailable_payload("缺少股票代码"),
            "financials": _build_unavailable_payload("缺少股票代码"),
        }

    market, symbol = _split_full_code(full_code)
    filtered_df = _filter_df(
        df,
        from_date=config.get("from_date"),
        to_date=config.get("to_date"),
    )
    if filtered_df.empty:
        as_of_date = None
    else:
        as_of_date = pd.Timestamp(filtered_df["date"].iloc[-1]).normalize()

    return {
        "as_of_date": _format_date(as_of_date),
        "news": _fetch_news_context(symbol=symbol, as_of_date=as_of_date),
        "fund_flow": _fetch_fund_flow_context(
            symbol=symbol,
            market=market,
            as_of_date=as_of_date,
        ),
        "financials": _fetch_financial_context(
            symbol=symbol,
            as_of_date=as_of_date,
        ),
    }


def _fetch_news_context(
    *,
    symbol: str,
    as_of_date: pd.Timestamp | None,
    lookback_days: int = DEFAULT_NEWS_LOOKBACK_DAYS,
    limit: int = DEFAULT_NEWS_LIMIT,
) -> dict[str, Any]:
    if as_of_date is None:
        return _build_unavailable_payload("缺少回测结束日期")

    try:
        raw_df = ak.stock_news_em(symbol=symbol)
    except Exception as exc:
        return _build_unavailable_payload(str(exc))

    if raw_df is None or raw_df.empty:
        return {
            "status": "empty",
            "source": "akshare.stock_news_em",
            "as_of_date": _format_date(as_of_date),
            "lookback_days": lookback_days,
            "items": [],
        }

    news_df = raw_df.copy()
    datetime_col = _find_column(
        news_df,
        ["发布时间", "发布时间 ", "时间", "日期"],
    )
    if datetime_col is None:
        return _build_unavailable_payload("新闻数据缺少发布时间字段")

    news_df["_published_at"] = pd.to_datetime(
        news_df[datetime_col],
        errors="coerce",
    )
    start_at = as_of_date - timedelta(days=lookback_days)
    cutoff_at = as_of_date + timedelta(days=1) - timedelta(seconds=1)
    news_df = news_df.dropna(subset=["_published_at"])
    news_df = news_df[
        (news_df["_published_at"] >= start_at)
        & (news_df["_published_at"] <= cutoff_at)
    ].sort_values("_published_at", ascending=False)

    items: list[dict[str, Any]] = []
    for _, row in news_df.head(limit).iterrows():
        title = _pick_row_value(row, ["新闻标题", "标题"])
        if not title:
            continue
        items.append(
            {
                "published_at": _format_datetime(row["_published_at"]),
                "source": _pick_row_value(row, ["文章来源", "来源"]),
                "title": title,
                "url": _pick_row_value(row, ["新闻链接", "链接", "网址"]),
            }
        )

    return {
        "status": "ok",
        "source": "akshare.stock_news_em",
        "as_of_date": _format_date(as_of_date),
        "lookback_days": lookback_days,
        "items": items,
    }


def _fetch_fund_flow_context(
    *,
    symbol: str,
    market: str,
    as_of_date: pd.Timestamp | None,
) -> dict[str, Any]:
    if as_of_date is None:
        return _build_unavailable_payload("缺少回测结束日期")

    try:
        raw_df = ak.stock_individual_fund_flow(stock=symbol, market=market)
    except Exception as exc:
        return _build_unavailable_payload(str(exc))

    if raw_df is None or raw_df.empty:
        return {
            "status": "empty",
            "source": "akshare.stock_individual_fund_flow",
            "as_of_date": _format_date(as_of_date),
        }

    fund_df = raw_df.copy()
    date_col = _find_column(fund_df, ["日期", "交易日期"])
    if date_col is None:
        return _build_unavailable_payload("资金流数据缺少日期字段")

    fund_df["_trade_date"] = pd.to_datetime(fund_df[date_col], errors="coerce")
    fund_df = fund_df.dropna(subset=["_trade_date"])
    fund_df = fund_df[fund_df["_trade_date"] <= as_of_date].sort_values("_trade_date")
    if fund_df.empty:
        return {
            "status": "empty",
            "source": "akshare.stock_individual_fund_flow",
            "as_of_date": _format_date(as_of_date),
        }

    latest_row = fund_df.iloc[-1]
    main_net_col = _find_column(
        fund_df,
        ["主力净流入-净额", "主力净流入净额", "主力净额", "主力净流入"],
    )
    super_ratio_col = _find_column(
        fund_df,
        ["超大单净流入-净占比", "超大单净占比", "超大单净流入净占比"],
    )
    large_ratio_col = _find_column(
        fund_df,
        ["大单净流入-净占比", "大单净占比", "大单净流入净占比"],
    )
    main_ratio_col = _find_column(
        fund_df,
        ["主力净流入-净占比", "主力净占比", "主力净流入净占比"],
    )

    return {
        "status": "ok",
        "source": "akshare.stock_individual_fund_flow",
        "as_of_date": _format_date(as_of_date),
        "latest_trade_date": _format_date(latest_row["_trade_date"]),
        "main_net_inflow_today": _to_float(latest_row.get(main_net_col)),
        "main_net_inflow_3d": _sum_tail(fund_df, main_net_col, 3),
        "main_net_inflow_5d": _sum_tail(fund_df, main_net_col, 5),
        "main_net_inflow_10d": _sum_tail(fund_df, main_net_col, 10),
        "main_net_inflow_ratio_today_pct": _to_float(latest_row.get(main_ratio_col)),
        "super_large_order_ratio_today_pct": _to_float(latest_row.get(super_ratio_col)),
        "large_order_ratio_today_pct": _to_float(latest_row.get(large_ratio_col)),
    }


def _fetch_financial_context(
    *,
    symbol: str,
    as_of_date: pd.Timestamp | None,
) -> dict[str, Any]:
    if as_of_date is None:
        return _build_unavailable_payload("缺少回测结束日期")

    try:
        abstract_df = ak.stock_financial_abstract(symbol=symbol)
    except Exception as exc:
        abstract_df = pd.DataFrame()
        abstract_error = str(exc)
    else:
        abstract_error = None

    try:
        indicator_df = ak.stock_financial_analysis_indicator(symbol=symbol)
    except Exception as exc:
        indicator_df = pd.DataFrame()
        indicator_error = str(exc)
    else:
        indicator_error = None

    latest_abstract = _find_latest_row_by_date(abstract_df, as_of_date)
    latest_indicator = _find_latest_row_by_date(indicator_df, as_of_date)

    if latest_abstract is None and latest_indicator is None:
        error_message = abstract_error or indicator_error or "没有可用的财务数据"
        return _build_unavailable_payload(error_message)

    latest_report_date = _coalesce(
        _extract_row_date_text(latest_abstract),
        _extract_row_date_text(latest_indicator),
    )
    return {
        "status": "ok",
        "sources": [
            source
            for source in [
                "akshare.stock_financial_abstract" if latest_abstract is not None else "",
                "akshare.stock_financial_analysis_indicator" if latest_indicator is not None else "",
            ]
            if source
        ],
        "as_of_date": _format_date(as_of_date),
        "report_date": latest_report_date,
        "revenue_yoy_pct": _extract_metric(
            latest_abstract,
            ["营业总收入同比", "营业收入同比", "总营收同比"],
        ),
        "net_profit_yoy_pct": _extract_metric(
            latest_abstract,
            ["净利润同比", "归母净利润同比", "扣非净利润同比"],
        ),
        "net_profit_pct": _extract_metric(
            latest_abstract,
            ["净利润", "归母净利润", "扣非净利润"],
        ),
        "roe_pct": _extract_metric(
            latest_indicator,
            ["净资产收益率(%)", "净资产收益率", "ROE"],
        ),
        "gross_margin_pct": _extract_metric(
            latest_indicator,
            ["销售毛利率(%)", "毛利率", "销售毛利率"],
        ),
        "debt_ratio_pct": _extract_metric(
            latest_indicator,
            ["资产负债率(%)", "资产负债率"],
        ),
        "operating_cashflow_per_share": _extract_metric(
            latest_indicator,
            ["每股经营性现金流(元)", "每股经营现金流", "每股经营活动产生的现金流量净额"],
        ),
    }


def _filter_df(
    df: pd.DataFrame,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    filtered = df.copy()
    filtered["date"] = pd.to_datetime(filtered["date"], errors="coerce")
    filtered = filtered.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if from_date:
        filtered = filtered[filtered["date"] >= pd.Timestamp(from_date)]
    if to_date:
        filtered = filtered[filtered["date"] <= pd.Timestamp(to_date)]
    return filtered.reset_index(drop=True)


def _split_full_code(full_code: str) -> tuple[str, str]:
    normalized = str(full_code or "").strip().lower()
    if "." in normalized:
        market, symbol = normalized.split(".", 1)
        if market not in {"sh", "sz", "bj"} or not symbol:
            raise ValueError(f"不支持的股票代码: {full_code}")
        return market, symbol
    if len(normalized) == 6 and normalized.isdigit():
        market = "sh" if normalized.startswith("6") else "sz"
        return market, normalized
    raise ValueError(f"不支持的股票代码: {full_code}")


def _build_unavailable_payload(error_message: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "error": str(error_message),
    }


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    exact_map = {str(col).strip(): col for col in df.columns}
    for candidate in candidates:
        if candidate in exact_map:
            return exact_map[candidate]
    for column in df.columns:
        normalized = str(column).strip()
        for candidate in candidates:
            if candidate in normalized:
                return column
    return None


def _pick_row_value(row: pd.Series, candidates: list[str]) -> str | None:
    if row is None:
        return None
    for candidate in candidates:
        for column in row.index:
            normalized = str(column).strip()
            if candidate == normalized or candidate in normalized:
                value = row.get(column)
                if value is None or pd.isna(value):
                    continue
                text = str(value).strip()
                if text:
                    return text
    return None


def _find_latest_row_by_date(
    df: pd.DataFrame,
    as_of_date: pd.Timestamp,
) -> pd.Series | None:
    if df is None or df.empty:
        return None

    working_df = df.copy()
    date_col = _find_column(
        working_df,
        ["报告期", "截止日期", "日期", "报告日期", "时间"],
    )
    if date_col is None:
        return working_df.iloc[0]

    working_df["_report_date"] = pd.to_datetime(
        working_df[date_col],
        errors="coerce",
    )
    working_df = working_df.dropna(subset=["_report_date"])
    working_df = working_df[working_df["_report_date"] <= as_of_date].sort_values(
        "_report_date",
        ascending=False,
    )
    if working_df.empty:
        return None
    return working_df.iloc[0]


def _extract_row_date_text(row: pd.Series | None) -> str | None:
    if row is None:
        return None
    value = row.get("_report_date")
    if value is not None and not pd.isna(value):
        return _format_date(pd.Timestamp(value))
    text = _pick_row_value(row, ["报告期", "截止日期", "日期", "报告日期", "时间"])
    return text or None


def _extract_metric(row: pd.Series | None, candidates: list[str]) -> float | None:
    if row is None:
        return None
    for candidate in candidates:
        for column in row.index:
            normalized = str(column).strip()
            if candidate == normalized or candidate in normalized:
                return _to_float(row.get(column))
    return None


def _sum_tail(df: pd.DataFrame, column: str | None, window: int) -> float | None:
    if not column or column not in df.columns:
        return None
    series = pd.to_numeric(df[column], errors="coerce").dropna().tail(window)
    if series.empty:
        return None
    return round(float(series.sum()), 4)


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text in {"--", "nan", "None"}:
        return None
    try:
        return round(float(text), 4)
    except (TypeError, ValueError):
        return None


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in {None, ""}:
            return value
    return None


def _format_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _format_datetime(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")

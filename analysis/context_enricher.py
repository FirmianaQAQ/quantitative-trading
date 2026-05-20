from __future__ import annotations

from datetime import timedelta
from typing import Any

import akshare as ak
import pandas as pd


DEFAULT_NEWS_LOOKBACK_DAYS = 20
DEFAULT_NEWS_LIMIT = 10
_POSITIVE_NEWS_KEYWORDS = {
    "业绩预增": ["预增", "大增", "扭亏", "增长", "创新高"],
    "订单合同": ["中标", "订单", "签约", "合同", "落地"],
    "股东支持": ["回购", "增持", "分红", "注销"],
    "产品突破": ["新品", "发布", "突破", "合作", "量产"],
}
_NEGATIVE_NEWS_KEYWORDS = {
    "业绩承压": ["预亏", "亏损", "下滑", "减少", "暴跌"],
    "风险处罚": ["风险", "处罚", "立案", "问询", "违规", "诉讼"],
    "股东减持": ["减持", "清仓", "质押", "冻结"],
    "项目受阻": ["终止", "失败", "取消", "延期", "违约"],
}
_NEUTRAL_NEWS_THEMES = {
    "财报披露": ["年报", "季报", "一季报", "半年报", "三季报"],
    "资本动作": ["融资", "定增", "发债", "收购", "重组"],
    "行业政策": ["政策", "补贴", "监管", "行业", "会议"],
}


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


def build_strategy_enhancement_patch(
    base_plan: dict[str, Any] | None,
    external_context: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(base_plan, dict) or not base_plan:
        return {}

    action = str(base_plan.get("action", "observe")).strip().lower() or "observe"
    reason_parts = [str(base_plan.get("reason", "")).strip()] if base_plan.get("reason") else []
    summary = str(base_plan.get("summary", "")).strip()
    score = 0

    news_context = (external_context or {}).get("news", {})
    fund_context = (external_context or {}).get("fund_flow", {})
    financial_context = (external_context or {}).get("financials", {})

    news_score = int(news_context.get("aggregate_sentiment_score") or 0)
    negative_count = int(news_context.get("negative_count") or 0)
    positive_count = int(news_context.get("positive_count") or 0)
    if negative_count >= 2:
        news_score -= 1
    if positive_count >= 2:
        news_score += 1
    if bool(news_context.get("has_risk_theme")):
        news_score -= 1
    if bool(news_context.get("has_shareholder_reduction")):
        news_score -= 1
    if bool(news_context.get("has_order_contract")):
        news_score += 1
    score += news_score
    news_summary = str(news_context.get("aggregate_summary", "")).strip()
    if news_summary:
        reason_parts.append(f"新闻面：{news_summary}")

    fund_score = 0
    main_inflow_5d = _to_float(fund_context.get("main_net_inflow_5d"))
    main_ratio_today = _to_float(fund_context.get("main_net_inflow_ratio_today_pct"))
    if main_inflow_5d is not None:
        if main_inflow_5d > 0:
            fund_score += 1
        elif main_inflow_5d < 0:
            fund_score -= 1
    if main_ratio_today is not None:
        if main_ratio_today >= 1:
            fund_score += 1
        elif main_ratio_today <= -1:
            fund_score -= 1
    main_inflow_10d = _to_float(fund_context.get("main_net_inflow_10d"))
    if main_inflow_10d is not None:
        if main_inflow_10d > 0 and (main_inflow_5d or 0) > 0:
            fund_score += 1
        elif main_inflow_10d < 0 and (main_inflow_5d or 0) < 0:
            fund_score -= 1
    score += fund_score
    fund_label = _describe_fund_flow_bias(fund_context)
    if fund_label:
        reason_parts.append(f"资金面：{fund_label}")

    financial_score = 0
    revenue_yoy = _to_float(financial_context.get("revenue_yoy_pct"))
    net_profit_yoy = _to_float(financial_context.get("net_profit_yoy_pct"))
    roe_pct = _to_float(financial_context.get("roe_pct"))
    debt_ratio_pct = _to_float(financial_context.get("debt_ratio_pct"))
    if revenue_yoy is not None:
        financial_score += 1 if revenue_yoy > 0 else -1
    if net_profit_yoy is not None:
        financial_score += 1 if net_profit_yoy > 0 else -1
    if roe_pct is not None:
        if roe_pct >= 8:
            financial_score += 1
        elif roe_pct <= 5:
            financial_score -= 1
    if debt_ratio_pct is not None and debt_ratio_pct >= 65:
        financial_score -= 1
    if revenue_yoy is not None and net_profit_yoy is not None:
        if revenue_yoy > 0 and net_profit_yoy < 0:
            financial_score -= 1
        if revenue_yoy > 0 and net_profit_yoy > 0:
            financial_score += 1
    score += financial_score
    financial_label = _describe_financial_bias(financial_context)
    if financial_label:
        reason_parts.append(f"财报面：{financial_label}")

    if score <= -3:
        if action in {"buy", "watch_buy", "observe"}:
            action = "observe"
            summary = "外部因子明显转弱，暂缓偏多信号，先等待风险释放。"
        elif action == "hold":
            action = "sell"
            summary = "外部因子明显转弱，持仓优先收紧风险敞口。"
    elif score == -2:
        if action == "buy":
            action = "watch_buy"
            summary = "技术面仍有买点雏形，但外部因子偏谨慎，先观察确认。"
        elif action == "watch_buy":
            action = "observe"
            summary = "技术面有转暖迹象，但外部因子尚未配合，继续等待。"
        elif action == "hold":
            summary = "可以继续持有，但外部因子转弱，建议收紧止盈止损。"
    elif score >= 3:
        if action == "observe":
            action = "watch_buy"
            summary = "外部因子与技术面同步转暖，可从观察升级为关注买点。"
        elif action == "watch_buy":
            action = "buy"
            summary = "外部因子与技术面共振，买点确认度提升。"
        elif action == "hold":
            summary = "趋势与外部因子共振，继续持有的胜率更高。"
    elif score == 2:
        if action == "observe":
            action = "watch_buy"
            summary = "外部因子偏正面，若技术面继续确认，可转入关注买点。"
        elif action == "hold":
            summary = "技术面仍可持有，外部因子提供一定支撑。"

    enhancement_label = _describe_enhancement_bias(score)
    reason_parts.append(f"综合修正：{enhancement_label}")
    display_action = {
        "buy": "优化买入",
        "sell": "优化卖出",
        "hold": "优化持有",
        "watch_buy": "优化观察",
        "observe": "优化观望",
    }.get(action, "优化建议")
    return {
        "action": action,
        "display_action": display_action,
        "title": display_action,
        "summary": summary or str(base_plan.get("summary", "")).strip(),
        "reason": "；".join([part for part in reason_parts if part]),
        "enhancement_score": score,
        "enhancement_label": enhancement_label,
        "news_sentiment_label": news_context.get("aggregate_sentiment_label"),
        "fund_flow_label": fund_label,
        "financial_label": financial_label,
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
        news_signal = _classify_news_title(title)
        items.append(
            {
                "published_at": _format_datetime(row["_published_at"]),
                "source": _pick_row_value(row, ["文章来源", "来源"]),
                "title": title,
                "url": _pick_row_value(row, ["新闻链接", "链接", "网址"]),
                "sentiment_label": news_signal["sentiment_label"],
                "sentiment_score": news_signal["sentiment_score"],
                "theme": news_signal["theme"],
                "summary": news_signal["summary"],
            }
        )

    aggregate_summary = _summarize_news_items(items)

    return {
        "status": "ok",
        "source": "akshare.stock_news_em",
        "as_of_date": _format_date(as_of_date),
        "lookback_days": lookback_days,
        "items": items,
        "aggregate_sentiment_score": aggregate_summary["score"],
        "aggregate_sentiment_label": aggregate_summary["label"],
        "theme_tags": aggregate_summary["themes"],
        "positive_count": aggregate_summary["positive_count"],
        "negative_count": aggregate_summary["negative_count"],
        "has_risk_theme": aggregate_summary["has_risk_theme"],
        "has_shareholder_reduction": aggregate_summary["has_shareholder_reduction"],
        "has_order_contract": aggregate_summary["has_order_contract"],
        "aggregate_summary": aggregate_summary["summary"],
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
        "bias_label": _describe_financial_bias(
            {
                "revenue_yoy_pct": _extract_metric(
                    latest_abstract,
                    ["营业总收入同比", "营业收入同比", "总营收同比"],
                ),
                "net_profit_yoy_pct": _extract_metric(
                    latest_abstract,
                    ["净利润同比", "归母净利润同比", "扣非净利润同比"],
                ),
                "roe_pct": _extract_metric(
                    latest_indicator,
                    ["净资产收益率(%)", "净资产收益率", "ROE"],
                ),
                "debt_ratio_pct": _extract_metric(
                    latest_indicator,
                    ["资产负债率(%)", "资产负债率"],
                ),
            }
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


def _classify_news_title(title: str) -> dict[str, Any]:
    text = str(title or "").strip()
    score = 0
    theme = "日常披露"
    matched_keyword = ""

    for candidate_theme, keywords in _POSITIVE_NEWS_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                score = 1
                theme = candidate_theme
                matched_keyword = keyword
                break
        if matched_keyword:
            break

    if not matched_keyword:
        for candidate_theme, keywords in _NEGATIVE_NEWS_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text:
                    score = -1
                    theme = candidate_theme
                    matched_keyword = keyword
                    break
            if matched_keyword:
                break

    if not matched_keyword:
        for candidate_theme, keywords in _NEUTRAL_NEWS_THEMES.items():
            for keyword in keywords:
                if keyword in text:
                    theme = candidate_theme
                    matched_keyword = keyword
                    break
            if matched_keyword:
                break

    sentiment_label = "中性"
    summary = "标题偏中性，更多用于补充事件背景。"
    if score > 0:
        sentiment_label = "偏利多"
        summary = f"标题偏利多，重点在{theme}，关键词“{matched_keyword}”。"
    elif score < 0:
        sentiment_label = "偏利空"
        summary = f"标题偏利空，重点在{theme}，关键词“{matched_keyword}”。"
    elif theme != "日常披露":
        summary = f"标题偏中性，主题聚焦{theme}。"

    return {
        "sentiment_score": score,
        "sentiment_label": sentiment_label,
        "theme": theme,
        "summary": summary,
    }


def _summarize_news_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    if not items:
        return {
            "score": 0,
            "label": "中性",
            "themes": [],
            "positive_count": 0,
            "negative_count": 0,
            "has_risk_theme": False,
            "has_shareholder_reduction": False,
            "has_order_contract": False,
            "summary": "近窗口内没有可用新闻。",
        }

    score = int(sum(int(item.get("sentiment_score") or 0) for item in items[:5]))
    themes = [str(item.get("theme", "")).strip() for item in items if item.get("theme")]
    positive_count = sum(1 for item in items[:5] if int(item.get("sentiment_score") or 0) > 0)
    negative_count = sum(1 for item in items[:5] if int(item.get("sentiment_score") or 0) < 0)
    unique_themes: list[str] = []
    for theme in themes:
        if theme and theme not in unique_themes:
            unique_themes.append(theme)

    label = _describe_enhancement_bias(score)
    top_themes = "、".join(unique_themes[:3]) if unique_themes else "日常披露"
    return {
        "score": score,
        "label": label,
        "themes": unique_themes[:5],
        "positive_count": positive_count,
        "negative_count": negative_count,
        "has_risk_theme": "风险处罚" in unique_themes or "项目受阻" in unique_themes,
        "has_shareholder_reduction": "股东减持" in unique_themes,
        "has_order_contract": "订单合同" in unique_themes,
        "summary": f"近窗口新闻整体{label}，主题集中在{top_themes}。",
    }


def _describe_fund_flow_bias(fund_context: dict[str, Any]) -> str:
    main_inflow_5d = _to_float(fund_context.get("main_net_inflow_5d"))
    main_ratio_today = _to_float(fund_context.get("main_net_inflow_ratio_today_pct"))
    if main_inflow_5d is None and main_ratio_today is None:
        return ""
    if (main_inflow_5d or 0) > 0 and (main_ratio_today or 0) >= 0:
        return "近端资金面偏流入，主力承接相对占优"
    if (main_inflow_5d or 0) < 0 and (main_ratio_today or 0) <= 0:
        return "近端资金面偏流出，主力承接偏弱"
    return "资金流方向分化，短线确认度一般"


def _describe_financial_bias(financial_context: dict[str, Any]) -> str:
    revenue_yoy = _to_float(financial_context.get("revenue_yoy_pct"))
    net_profit_yoy = _to_float(financial_context.get("net_profit_yoy_pct"))
    roe_pct = _to_float(financial_context.get("roe_pct"))
    debt_ratio_pct = _to_float(financial_context.get("debt_ratio_pct"))

    positives = 0
    negatives = 0
    if revenue_yoy is not None:
        positives += int(revenue_yoy > 0)
        negatives += int(revenue_yoy < 0)
    if net_profit_yoy is not None:
        positives += int(net_profit_yoy > 0)
        negatives += int(net_profit_yoy < 0)
    if roe_pct is not None:
        positives += int(roe_pct >= 8)
        negatives += int(roe_pct <= 5)
    if debt_ratio_pct is not None:
        negatives += int(debt_ratio_pct >= 65)

    if positives > negatives:
        return "财报质量偏稳，基本面提供一定支撑"
    if negatives > positives:
        return "财报约束偏明显，需要警惕基本面拖累"
    if positives == negatives == 0:
        return ""
    return "财报表现中性，暂未形成明显方向"


def _describe_enhancement_bias(score: int) -> str:
    if score >= 3:
        return "偏积极"
    if score >= 1:
        return "小幅偏积极"
    if score <= -3:
        return "偏谨慎"
    if score <= -1:
        return "小幅偏谨慎"
    return "中性"


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

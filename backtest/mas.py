from __future__ import annotations

import copy
import math
import sys
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.service import maybe_generate_single_stock_analysis
from backtest.backtest_v1 import generate_html_report
from utils.a_share_costs import estimate_max_buy_size, estimate_trade_fee, validate_a_share_cost_config
from utils.default_stocks import (
    DEFAULT_PRIMARY_STOCK_CODE,
    DEFAULT_STOCK_CODES,
    DEFAULT_STOCK_NAMES,
)
from utils.project_utils import load_daily_data, normalize_adjust_flag_name


STRATEGY_ID = "MAS"
STRATEGY_FAMILY_ID = "MAS"


CONFIG: dict[str, Any] = {
    # 基础回测参数。
    # 默认回测股票代码，统一跟随 utils/default_stocks.py 的 DEFAULT_PRIMARY_STOCK_CODE。
    "code": DEFAULT_PRIMARY_STOCK_CODE,
    # 复权口径。MAS 默认用前复权 qfq；也支持 cq / dypre，不支持 hfq。
    "adjust_flag": "qfq",
    # 策略正式计入绩效的起始日期。
    "from_date": "2020-01-01",
    # 策略正式计入绩效的结束日期；None 表示使用数据最后一天。
    "to_date": None,
    # 指标预热起始日期，必须早于 from_date，避免 MA 和筹码计算预热不足。
    "data_from_date": "2019-01-01",
    # 初始资金。
    "cash": 100000.0,
    # 券商佣金率，按成交额双边收取。
    "commission": 0.0000854,
    # 卖出印花税率。
    "stamp_duty": 0.0005,
    # 双边过户费率；当前默认 0。
    "transfer_fee": 0.0,
    # 单笔最低佣金。
    "min_commission": 5.0,
    # A 股下单股数步长，通常为 100 股。
    "lot_size": 100,
    # 是否在终端打印逐日信号过滤和交易日志。
    "print_log": True,
    # 是否生成 HTML 回测报告，报告路径仍走原项目流程。
    "plot": True,
    # 报告里的对照基准代码；空字符串表示不展示基准。
    "benchmark_code": "sh.000001",
    # HTML 回测报告输出目录。
    "report_dir": "logs/backtest",
    # HTML 回测报告文件名前缀。
    "report_name": "mas_backtest",
    # 报告和菜单展示的策略名称。
    "strategy_name": "MAS",
    # 菜单和报告摘要里的策略简述。
    "strategy_brief": "银山谷 + 筹码集中 + 量比确认",
    # 当日建议的持仓状态视角：auto / empty / hold。
    "current_position": "auto",
    # 市场整体趋势：normal 常规过滤；uptrend 表示当前处于上升趋势，放宽入场阈值。
    "market_trend_mode": "normal",
    # 是否启用大模型分析，默认关闭，避免普通回测触发外部分析流程。
    "enable_llm_analysis": False,
    # 银山谷均线参数。默认对应 5 日、10 日、20 日均线。
    # 短期均线周期，对应银山谷里的 MA5。
    "ma_short": 5,
    # 中期均线周期，对应银山谷里的 MA10。
    "ma_mid": 10,
    # 长期均线周期，对应银山谷里的 MA20。
    "ma_long": 20,
    # MA5 上穿 MA10 后，至少持续站上 MA10 的确认天数。
    "short_mid_confirm_days": 2,
    # 第一段建仓允许在 MA5 上穿 MA10 后多少个交易日内触发。
    "stage1_entry_window": 3,
    # 三次上穿必须在该交易日跨度内完成，避免拖太久的松散形态。
    "silver_valley_max_span": 10,
    # 形态流畅度。ma_short_slope_period 内 MA5 平均斜率需要达到阈值。
    # 计算 MA5 平均斜率的回看周期。
    "ma_short_slope_period": 5,
    # MA5 平均日斜率下限，适度放宽，减少趋势初期被误杀。
    "ma_short_min_avg_slope_pct": 0.0035,
    # 是否要求 MA10 和 MA20 斜率同步向上。
    "require_mid_long_slope_up": True,
    # MA10 单日斜率下限，0 表示至少不下降。
    "ma_mid_min_slope_pct": 0.0,
    # MA20 单日斜率下限，0 表示至少不下降。
    "ma_long_min_slope_pct": 0.0,
    # 边长比例验证。中期均线上穿段涨幅必须强于短线段，避免短炒式假突破。
    # 是否启用银山谷三角边长比例校验。
    "edge_ratio_enabled": True,
    # MA10 上穿段涨幅需要达到 MA5 上穿段涨幅的倍数。
    "edge_mid_gain_multiplier": 1.0,
    # 筹码集中度参数。集中度单位为百分比，越低表示筹码越集中。
    # 是否启用筹码集中度过滤。
    "chip_enabled": True,
    # 筹码分布回看周期。
    "chip_period": 30,
    # 筹码分布价格网格数量，越大越细，但计算越慢。
    "chip_bins": 70,
    # 集中度计算使用的筹码比例，0.90 表示统计 90% 筹码分布区间。
    "chip_concentration_threshold": 0.90,
    # 买入允许的 90% 筹码集中度上限，放宽到 15% 左右。
    "chip_concentration_max_pct": 15.0,
    # 观察池上限，进一步放宽到 18% 左右。
    "chip_watch_max_pct": 18.0,
    # 规避阈值，集中度大于等于该值直接放弃。
    "chip_avoid_min_pct": 20.0,
    # 是否要求筹码分布呈单峰密集形态。
    "chip_single_peak_enabled": True,
    # 识别有效峰值的相对高度，0.55 表示峰值至少达到最高峰的 55%。
    "chip_peak_min_height_ratio": 0.55,
    # 允许的有效峰值数量，默认只接受单峰。
    "chip_max_peak_count": 1,
    # 本地 turn 字段为空或全 0 时，用成交量相对 20 日均量估算换手率。
    # 换手率来源：auto 自动选择，data 强制用 turn，estimate 强制估算。
    "turnover_source": "auto",
    # 估算换手率的基础值，成交量等于均量时约等于该值。
    "turnover_estimate_base_pct": 0.025,
    # 估算换手率时使用的成交量均线周期。
    "turnover_estimate_ma_period": 20,
    # 估算换手率下限，避免筹码完全不滚动。
    "turnover_estimate_min_pct": 0.001,
    # 估算换手率上限，避免异常成交量导致筹码瞬间全换手。
    "turnover_estimate_max_pct": 0.12,
    # 位置过滤。低位或涨幅温和才允许介入，防止高位筹码集中陷阱。
    # 是否启用股价位置过滤。
    "position_filter_enabled": True,
    # 近期涨幅回看周期，默认约 3 个月交易日。
    "recent_gain_lookback": 30,
    # 近期涨幅买入上限，超过该值认为已经不够低位。
    "recent_gain_max_pct": 0.40,
    # 近期涨幅放弃阈值，超过该值直接视为高位风险。
    "recent_gain_abandon_pct": 1.00,
    # 相对高低位计算周期。
    "relative_low_lookback": 30,
    # 当前价在区间高低位中的最大允许位置，0.65 表示不追到区间高位。
    "relative_position_max_pct": 0.75,
    # 量能确认。三角成型附近需要明显放量，量比确认大资金介入。
    # 是否启用量能过滤。
    "volume_confirm_enabled": True,
    # 计算均量和量比的基础周期。
    "volume_ma_period": 20,
    # 统计三角形成前后放量的回看窗口。
    "volume_expand_lookback": 5,
    # 近 3-5 日均量相对 20 日均量的最低放大倍数。
    "volume_expand_min_ratio": 1.10,
    # 当日量比下限，适度放宽，减少趋势延续段的误杀。
    "volume_ratio_min": 1.30,
    # 第一段建仓是否必须满足放量确认。
    "stage1_require_volume_confirm": False,
    # 第二段建仓是否必须满足放量确认。
    "stage2_require_volume_confirm": True,
    # 第三段回踩加仓是否必须满足放量确认。
    "stage3_require_volume_confirm": False,
    # 基本面兜底。默认关闭；开启后若数据缺列会快速失败，避免伪过滤。
    # 是否启用基本面过滤；当前本地日线通常没有这些字段，所以默认关闭。
    "fundamental_filter_enabled": False,
    # 业绩字段名，开启基本面过滤时会读取该列。
    "fundamental_profit_column": "profit_ttm",
    # 商誉占比字段名，开启基本面过滤时会读取该列。
    "fundamental_goodwill_column": "goodwill_ratio",
    # 股东减持标记字段名，开启基本面过滤时会读取该列。
    "fundamental_reduction_column": "shareholder_reduction_flag",
    # 最低盈利要求，低于该值会被过滤。
    "fundamental_min_profit": 0.0,
    # 最高商誉占比，超过该值会被过滤。
    "fundamental_max_goodwill_ratio": 0.30,
    # 分步建仓。三个阶段合计默认 30%，满足单票最大仓位约束。
    # 单只股票最大持仓占总资产比例。
    "max_position_pct": 0.30,
    # 第一段：MA5 上穿 MA10 并确认后的建仓比例。
    "stage1_position_pct": 0.10,
    # 第二段：完整银山谷成型且量能确认后的加仓比例。
    "stage2_position_pct": 0.10,
    # 第三段：回踩 MA10 不破并再次拉升后的加仓比例。
    "stage3_position_pct": 0.10,
    # 买入成交估算价格缓冲，1.0 表示按收盘价估算。
    "buy_price_buffer": 1.0,
    # 卖出成交估算价格缓冲，1.0 表示按收盘价估算。
    "sell_price_buffer": 1.0,
    # 第三段建仓：回踩 MA10 不破并再次拉升。
    # 判断“回踩 MA10”的允许上方偏离，0.01 表示最低价接近 MA10 上方 1% 内即可。
    "pullback_tolerance_pct": 0.01,
    # 判断“未跌破 MA10”的容忍度，0.005 表示收盘价可略低于 MA10 0.5%。
    "pullback_break_tolerance_pct": 0.005,
    # 卖出与风控。
    # 连续多少天收盘跌破 MA10 后清仓。
    "sell_below_mid_ma_days": 1,
    # 单笔硬止损比例，亏损达到该值无条件清仓。
    "hard_stop_loss_pct": 0.6,
    # 是否启用 MA5 死叉 MA20 清仓。
    "dead_cross_exit_enabled": True,
    # 是否启用持仓后的筹码派发预警。
    "chip_warning_enabled": True,
    # 持仓盈利后，集中度扩大到该值以上视为派发风险。
    "chip_warning_concentration_pct": 15.0,
    # 筹码派发预警生效所需的最低浮盈，避免低位震荡误杀。
    "chip_warning_min_profit_pct": 0.20,
    # 参数优化。默认关闭；开启后只遍历 MAS 自己的配置项。
    # 是否开启参数优化模式；开启后不会生成 HTML 报告。
    "optimize": False,
    # MA5 优化范围，格式 start:end:step。
    "opt_ma_short": "4:8:1",
    # MA10 优化范围，格式 start:end:step。
    "opt_ma_mid": "9:13:1",
    # MA20 优化范围，格式 start:end:step。
    "opt_ma_long": "18:30:2",
    # 筹码集中度买入阈值优化范围。
    "opt_chip_concentration_max_pct": "8:12:2",
    # 量比阈值优化范围。
    "opt_volume_ratio_min": "1.20:1.80:0.15",
    # 参数优化结果展示前 N 名。
    "top": 10,
}


TEST_CASES = [
    {"code": code, "label": DEFAULT_STOCK_NAMES.get(code, code), "expect": ""}
    for code in DEFAULT_STOCK_CODES
]


@dataclass
class MASRuntimeState:
    cash: float
    shares: int = 0
    cost_basis: float = 0.0
    stage: int = 0
    entry_price: float | None = None
    entry_date: pd.Timestamp | None = None
    entry_concentration_pct: float | None = None
    first_cross_idx: int | None = None
    second_cross_idx: int | None = None
    third_cross_idx: int | None = None
    buy_markers: list[tuple[pd.Timestamp, float]] = field(default_factory=list)
    sell_markers: list[tuple[pd.Timestamp, float]] = field(default_factory=list)
    buy_trade_records: list[dict[str, Any]] = field(default_factory=list)
    sell_trade_records: list[dict[str, Any]] = field(default_factory=list)
    log_messages: list[str] = field(default_factory=list)
    equity_records: list[tuple[pd.Timestamp, float]] = field(default_factory=list)
    daily_position_ratios: list[float] = field(default_factory=list)
    completed_trade_pnls: list[float] = field(default_factory=list)
    buy_signals_total: int = 0
    buy_signals_blocked: int = 0
    position_days_total: int = 0
    idle_cash_days_total: int = 0
    seen_short_long_cross: bool = False


def validate_config(config: dict[str, Any]) -> None:
    required_keys = set(CONFIG)
    missing_keys = sorted(required_keys - set(config))
    if missing_keys:
        raise ValueError(f"MAS 配置缺少字段: {missing_keys}")

    if float(config["cash"]) <= 0:
        raise ValueError("cash 必须大于 0")
    validate_a_share_cost_config(config)

    normalized_adjust_flag = normalize_adjust_flag_name(str(config["adjust_flag"]))
    if normalized_adjust_flag == "hfq":
        raise ValueError("MAS 不支持后复权（hfq），请使用 qfq、cq 或 dypre")

    ma_short = int(config["ma_short"])
    ma_mid = int(config["ma_mid"])
    ma_long = int(config["ma_long"])
    if not (0 < ma_short < ma_mid < ma_long):
        raise ValueError("MAS 均线周期必须满足 0 < ma_short < ma_mid < ma_long")

    positive_int_keys = [
        "short_mid_confirm_days",
        "stage1_entry_window",
        "silver_valley_max_span",
        "ma_short_slope_period",
        "chip_period",
        "chip_bins",
        "turnover_estimate_ma_period",
        "recent_gain_lookback",
        "relative_low_lookback",
        "volume_ma_period",
        "volume_expand_lookback",
        "sell_below_mid_ma_days",
        "lot_size",
        "top",
    ]
    for key in positive_int_keys:
        if int(config[key]) <= 0:
            raise ValueError(f"{key} 必须大于 0")

    ratio_keys = [
        "max_position_pct",
        "stage1_position_pct",
        "stage2_position_pct",
        "stage3_position_pct",
        "hard_stop_loss_pct",
        "pullback_tolerance_pct",
        "pullback_break_tolerance_pct",
    ]
    for key in ratio_keys:
        if float(config[key]) < 0 or float(config[key]) >= 1:
            raise ValueError(f"{key} 必须大于等于 0 且小于 1")

    staged_position_pct = (
        float(config["stage1_position_pct"])
        + float(config["stage2_position_pct"])
        + float(config["stage3_position_pct"])
    )
    if staged_position_pct > float(config["max_position_pct"]) + 1e-8:
        raise ValueError("三段建仓比例合计不能大于 max_position_pct")

    if float(config["buy_price_buffer"]) <= 0:
        raise ValueError("buy_price_buffer 必须大于 0")
    if float(config["sell_price_buffer"]) <= 0:
        raise ValueError("sell_price_buffer 必须大于 0")
    if str(config["turnover_source"]).strip().lower() not in {"auto", "data", "estimate"}:
        raise ValueError("turnover_source 仅支持 auto、data、estimate")
    if str(config["market_trend_mode"]).strip().lower() not in {"normal", "uptrend"}:
        raise ValueError("market_trend_mode 仅支持 normal、uptrend")

    if float(config["chip_concentration_max_pct"]) > float(config["chip_watch_max_pct"]):
        raise ValueError("chip_concentration_max_pct 不能大于 chip_watch_max_pct")
    if float(config["chip_watch_max_pct"]) > float(config["chip_avoid_min_pct"]):
        raise ValueError("chip_watch_max_pct 不能大于 chip_avoid_min_pct")


def run_backtest(config: dict[str, Any], df: pd.DataFrame | None = None) -> dict[str, Any]:
    validate_config(config)
    source_df = df if df is not None else load_daily_data(config["code"], config["adjust_flag"])

    if config.get("optimize"):
        run_optimization(config, source_df)
        return {}

    prepared_df = _prepare_signal_frame(source_df, config)
    trade_df = _filter_trade_frame(prepared_df, config)
    if trade_df.empty:
        raise ValueError("MAS 回测区间内没有可用数据")

    print(f"开始回测: 股票={config['code']}，初始资金={float(config['cash']):.2f}")
    state = _execute_strategy(prepared_df, trade_df.index, config)
    summary = _build_summary(state, config)
    _print_summary(summary)

    ai_report_path = maybe_generate_single_stock_analysis(config, summary, trade_df)
    if config.get("plot"):
        report_data = _build_report_data(trade_df, state, summary, config)
        generate_html_report(
            report_data,
            config,
            state.log_messages,
            ai_report_path=ai_report_path,
        )

    return summary


def run_optimization(config: dict[str, Any], df: pd.DataFrame) -> None:
    ma_short_values = _parse_number_range(config["opt_ma_short"], int, "opt_ma_short")
    ma_mid_values = _parse_number_range(config["opt_ma_mid"], int, "opt_ma_mid")
    ma_long_values = _parse_number_range(config["opt_ma_long"], int, "opt_ma_long")
    chip_values = _parse_number_range(
        config["opt_chip_concentration_max_pct"],
        float,
        "opt_chip_concentration_max_pct",
    )
    volume_ratio_values = _parse_number_range(
        config["opt_volume_ratio_min"],
        float,
        "opt_volume_ratio_min",
    )

    combinations = [
        item
        for item in product(
            ma_short_values,
            ma_mid_values,
            ma_long_values,
            chip_values,
            volume_ratio_values,
        )
        if item[0] < item[1] < item[2]
    ]
    if not combinations:
        raise ValueError("MAS 参数优化没有可用组合")

    print(f"开始 MAS 参数优化: 股票={config['code']}，参数组合数={len(combinations)}")
    results: list[dict[str, Any]] = []
    for ma_short, ma_mid, ma_long, chip_max, volume_ratio_min in combinations:
        loop_config = copy.deepcopy(config)
        loop_config.update(
            {
                "ma_short": ma_short,
                "ma_mid": ma_mid,
                "ma_long": ma_long,
                "chip_concentration_max_pct": chip_max,
                "volume_ratio_min": volume_ratio_min,
                "plot": False,
                "print_log": False,
                "enable_llm_analysis": False,
                "optimize": False,
            }
        )
        try:
            summary = run_backtest(loop_config, df)
        except Exception as exc:
            print(f"跳过无效组合 MA={ma_short}/{ma_mid}/{ma_long}: {exc}")
            continue
        summary["optimization_score"] = _compute_optimization_score(summary)
        results.append(summary)

    top_results = sorted(
        results,
        key=lambda item: (
            item.get("optimization_score", float("-inf")),
            item.get("annual_return_pct") or float("-inf"),
            -(item.get("max_drawdown_pct") or 0.0),
        ),
        reverse=True,
    )[: int(config["top"])]
    if not top_results:
        raise ValueError("MAS 参数优化没有产出有效结果")

    print("MAS 参数优化结果:")
    for index, item in enumerate(top_results, start=1):
        print(
            f"{index}. MA={item['fast_period']}/{item['mid_period']}/{item['slow_period']} "
            f"筹码阈值={item['chip_concentration_max_pct']:.2f}% "
            f"量比阈值={item['volume_ratio_min']:.2f} "
            f"综合评分={item['optimization_score']:.2f} "
            f"年化={_format_optional_percent(item['annual_return_pct'])} "
            f"最大回撤={_format_optional_percent(item['max_drawdown_pct'])} "
            f"胜率={item['win_rate_pct']:.2f}% "
            f"交易次数={item['trades_total']}"
        )


def main(config: dict[str, Any]) -> None:
    run_backtest(config)


def _prepare_signal_frame(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date").reset_index(drop=True)
    frame = _filter_source_frame(frame, config)

    numeric_columns = ["open", "high", "low", "close", "volume", "turn"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["date", "open", "high", "low", "close", "volume"])
    frame = frame.reset_index(drop=True)
    if frame.empty:
        raise ValueError("MAS 没有可用日线数据")

    ma_short = int(config["ma_short"])
    ma_mid = int(config["ma_mid"])
    ma_long = int(config["ma_long"])
    frame["ma_short"] = frame["close"].rolling(ma_short, min_periods=ma_short).mean()
    frame["ma_mid"] = frame["close"].rolling(ma_mid, min_periods=ma_mid).mean()
    frame["ma_long"] = frame["close"].rolling(ma_long, min_periods=ma_long).mean()
    frame["prev_ma_short"] = frame["ma_short"].shift(1)
    frame["prev_ma_mid"] = frame["ma_mid"].shift(1)
    frame["prev_ma_long"] = frame["ma_long"].shift(1)
    frame["prev_close"] = frame["close"].shift(1)

    frame["cross_short_mid_up"] = (
        (frame["prev_ma_short"] <= frame["prev_ma_mid"])
        & (frame["ma_short"] > frame["ma_mid"])
    )
    frame["cross_short_long_up"] = (
        (frame["prev_ma_short"] <= frame["prev_ma_long"])
        & (frame["ma_short"] > frame["ma_long"])
    )
    frame["cross_mid_long_up"] = (
        (frame["prev_ma_mid"] <= frame["prev_ma_long"])
        & (frame["ma_mid"] > frame["ma_long"])
    )
    frame["dead_cross_short_long"] = (
        (frame["prev_ma_short"] >= frame["prev_ma_long"])
        & (frame["ma_short"] < frame["ma_long"])
    )

    slope_period = int(config["ma_short_slope_period"])
    frame["ma_short_avg_slope_pct"] = frame["ma_short"].pct_change(slope_period) / slope_period
    frame["ma_mid_slope_pct"] = frame["ma_mid"].pct_change()
    frame["ma_long_slope_pct"] = frame["ma_long"].pct_change()
    frame["below_mid_ma"] = frame["close"] < frame["ma_mid"]
    frame["below_mid_ma_count"] = (
        frame["below_mid_ma"].astype(int)
        .rolling(int(config["sell_below_mid_ma_days"]), min_periods=1)
        .sum()
    )

    volume_ma_period = int(config["volume_ma_period"])
    volume_expand_lookback = int(config["volume_expand_lookback"])
    frame["volume_ma"] = frame["volume"].rolling(volume_ma_period, min_periods=1).mean().shift(1)
    frame["volume_ratio"] = frame["volume"] / frame["volume_ma"].replace(0, np.nan)
    frame["recent_volume_mean"] = (
        frame["volume"].rolling(volume_expand_lookback, min_periods=1).mean()
    )
    frame["volume_expand_ratio"] = frame["recent_volume_mean"] / frame["volume_ma"].replace(0, np.nan)

    recent_gain_lookback = int(config["recent_gain_lookback"])
    relative_low_lookback = int(config["relative_low_lookback"])
    frame["recent_base_close"] = frame["close"].shift(recent_gain_lookback)
    frame["recent_gain_pct"] = frame["close"] / frame["recent_base_close"] - 1
    frame["recent_gain_pct"] = frame["recent_gain_pct"].fillna(frame["close"] / frame["close"].cummin() - 1)
    frame["relative_low"] = frame["low"].rolling(relative_low_lookback, min_periods=1).min()
    frame["relative_high"] = frame["high"].rolling(relative_low_lookback, min_periods=1).max()
    price_range = (frame["relative_high"] - frame["relative_low"]).replace(0, np.nan)
    frame["relative_position_pct"] = ((frame["close"] - frame["relative_low"]) / price_range).fillna(0)

    frame["mas_turnover"] = _build_turnover_series(frame, config)
    _attach_chip_metrics(frame, config)
    _attach_fundamental_filter(frame, config)
    if "ex_right_close" not in frame.columns:
        frame["ex_right_close"] = frame["close"]
    return frame.reset_index(drop=True)


def _filter_source_frame(frame: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    filtered = frame.copy()
    if config.get("data_from_date"):
        filtered = filtered[filtered["date"] >= pd.Timestamp(config["data_from_date"])]
    if config.get("to_date"):
        filtered = filtered[filtered["date"] <= pd.Timestamp(config["to_date"])]
    return filtered.reset_index(drop=True)


def _filter_trade_frame(frame: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    filtered = frame.copy()
    if config.get("from_date"):
        filtered = filtered[filtered["date"] >= pd.Timestamp(config["from_date"])]
    if config.get("to_date"):
        filtered = filtered[filtered["date"] <= pd.Timestamp(config["to_date"])]
    return filtered


def _resolve_market_trend_mode(config: dict[str, Any]) -> str:
    return str(config.get("market_trend_mode", "normal")).strip().lower()


def _resolve_market_trend_label(config: dict[str, Any]) -> str:
    return "整体上升趋势" if _resolve_market_trend_mode(config) == "uptrend" else "常规过滤"


def _build_effective_entry_thresholds(config: dict[str, Any]) -> dict[str, float]:
    thresholds = {
        "chip_concentration_max_pct": float(config["chip_concentration_max_pct"]),
        "chip_watch_max_pct": float(config["chip_watch_max_pct"]),
        "recent_gain_max_pct": float(config["recent_gain_max_pct"]),
        "recent_gain_abandon_pct": float(config["recent_gain_abandon_pct"]),
        "relative_position_max_pct": float(config["relative_position_max_pct"]),
        "volume_expand_min_ratio": float(config["volume_expand_min_ratio"]),
        "volume_ratio_min": float(config["volume_ratio_min"]),
        "ma_short_min_avg_slope_pct": float(config["ma_short_min_avg_slope_pct"]),
    }
    if _resolve_market_trend_mode(config) != "uptrend":
        return thresholds

    chip_avoid_min_pct = float(config["chip_avoid_min_pct"])
    thresholds["chip_watch_max_pct"] = min(thresholds["chip_watch_max_pct"] + 2.0, chip_avoid_min_pct - 0.1)
    thresholds["chip_concentration_max_pct"] = min(
        thresholds["chip_concentration_max_pct"] + 2.0,
        thresholds["chip_watch_max_pct"],
    )
    thresholds["recent_gain_max_pct"] += 0.10
    thresholds["recent_gain_abandon_pct"] += 0.15
    thresholds["relative_position_max_pct"] = min(thresholds["relative_position_max_pct"] + 0.10, 0.95)
    thresholds["volume_expand_min_ratio"] = max(thresholds["volume_expand_min_ratio"] - 0.10, 1.0)
    thresholds["volume_ratio_min"] = max(thresholds["volume_ratio_min"] - 0.15, 1.0)
    thresholds["ma_short_min_avg_slope_pct"] = max(thresholds["ma_short_min_avg_slope_pct"] - 0.001, 0.0)
    return thresholds


def _build_turnover_series(frame: pd.DataFrame, config: dict[str, Any]) -> pd.Series:
    source = str(config["turnover_source"]).strip().lower()
    raw_turnover = pd.to_numeric(frame.get("turn"), errors="coerce").fillna(0.0)
    if raw_turnover[raw_turnover > 0].median() > 1:
        raw_turnover = raw_turnover / 100.0

    use_estimate = source == "estimate" or (
        source == "auto" and raw_turnover.fillna(0).le(0).all()
    )
    if not use_estimate:
        return raw_turnover.clip(lower=0.0, upper=0.95)

    ma_period = int(config["turnover_estimate_ma_period"])
    volume_ma = frame["volume"].rolling(ma_period, min_periods=1).mean().replace(0, np.nan)
    estimated = (
        frame["volume"]
        / volume_ma
        * float(config["turnover_estimate_base_pct"])
    )
    return estimated.fillna(float(config["turnover_estimate_min_pct"])).clip(
        lower=float(config["turnover_estimate_min_pct"]),
        upper=float(config["turnover_estimate_max_pct"]),
    )


def _attach_chip_metrics(frame: pd.DataFrame, config: dict[str, Any]) -> None:
    frame["chip_concentration_pct"] = np.nan
    frame["chip_low_price"] = np.nan
    frame["chip_high_price"] = np.nan
    frame["chip_profit_ratio_pct"] = np.nan
    frame["chip_avg_cost"] = np.nan
    frame["chip_single_peak"] = False

    if not bool(config["chip_enabled"]):
        frame["chip_single_peak"] = True
        return

    period = int(config["chip_period"])
    bins = int(config["chip_bins"])
    threshold = float(config["chip_concentration_threshold"])
    for idx in range(len(frame)):
        start_idx = max(0, idx - period + 1)
        window = frame.iloc[start_idx : idx + 1]
        price_grid, chips = _compute_chip_distribution(window, bins)
        close = float(frame.at[idx, "close"])
        concentration, low_price, high_price = _compute_chip_concentration(
            price_grid,
            chips,
            close,
            threshold,
        )
        frame.at[idx, "chip_concentration_pct"] = concentration
        frame.at[idx, "chip_low_price"] = low_price
        frame.at[idx, "chip_high_price"] = high_price
        frame.at[idx, "chip_profit_ratio_pct"] = float(chips[price_grid < close].sum() * 100.0)
        frame.at[idx, "chip_avg_cost"] = float((price_grid * chips).sum())
        frame.at[idx, "chip_single_peak"] = _is_single_peak(chips, config)


def _compute_chip_distribution(window: pd.DataFrame, bins: int) -> tuple[np.ndarray, np.ndarray]:
    price_min = float(window["low"].min())
    price_max = float(window["high"].max())
    if not math.isfinite(price_min) or not math.isfinite(price_max) or price_min <= 0:
        price_min = float(window["close"].min())
        price_max = float(window["close"].max())
    if price_max <= price_min:
        price_max = price_min * 1.001

    price_grid = np.linspace(price_min, price_max, bins)
    chips = np.zeros(bins)
    for _, row in window.iterrows():
        high = float(row["high"])
        low = float(row["low"])
        turnover = min(max(float(row.get("mas_turnover", 0.0) or 0.0), 0.0), 0.95)
        new_chips = np.zeros(bins)
        if high <= low:
            nearest_idx = int(np.abs(price_grid - float(row["close"])).argmin())
            new_chips[nearest_idx] = turnover
        else:
            mask = (price_grid >= low) & (price_grid <= high)
            if mask.any():
                new_chips[mask] = turnover / int(mask.sum())
            else:
                nearest_idx = int(np.abs(price_grid - ((high + low) / 2)).argmin())
                new_chips[nearest_idx] = turnover
        chips = chips * (1.0 - turnover) + new_chips

    total = float(chips.sum())
    if total <= 0:
        chips = np.ones(bins) / bins
    else:
        chips = chips / total
    return price_grid, chips


def _compute_chip_concentration(
    price_grid: np.ndarray,
    chips: np.ndarray,
    current_price: float,
    threshold: float,
) -> tuple[float, float, float]:
    cumsum = np.cumsum(chips)
    lower_idx = min(int(np.searchsorted(cumsum, (1 - threshold) / 2)), len(price_grid) - 1)
    upper_idx = min(int(np.searchsorted(cumsum, 1 - (1 - threshold) / 2)), len(price_grid) - 1)
    low_price = float(price_grid[lower_idx])
    high_price = float(price_grid[upper_idx])
    concentration = (high_price - low_price) / max(float(current_price), 1e-8) * 100.0
    return concentration, low_price, high_price


def _is_single_peak(chips: np.ndarray, config: dict[str, Any]) -> bool:
    if not bool(config["chip_single_peak_enabled"]):
        return True
    if len(chips) < 3:
        return True
    max_chip = float(chips.max())
    if max_chip <= 0:
        return False
    min_height = max_chip * float(config["chip_peak_min_height_ratio"])
    peak_count = 0
    for idx in range(1, len(chips) - 1):
        if chips[idx] >= chips[idx - 1] and chips[idx] >= chips[idx + 1] and chips[idx] >= min_height:
            peak_count += 1
    if peak_count == 0:
        peak_count = 1
    return peak_count <= int(config["chip_max_peak_count"])


def _attach_fundamental_filter(frame: pd.DataFrame, config: dict[str, Any]) -> None:
    if not bool(config["fundamental_filter_enabled"]):
        frame["fundamental_pass"] = True
        return

    required_columns = [
        str(config["fundamental_profit_column"]),
        str(config["fundamental_goodwill_column"]),
        str(config["fundamental_reduction_column"]),
    ]
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(
            "MAS 基本面兜底已开启，但本地数据缺少字段: "
            f"{missing_columns}。请补齐字段或关闭 fundamental_filter_enabled。"
        )

    profit = pd.to_numeric(frame[required_columns[0]], errors="coerce")
    goodwill = pd.to_numeric(frame[required_columns[1]], errors="coerce")
    reduction = pd.to_numeric(frame[required_columns[2]], errors="coerce").fillna(0)
    frame["fundamental_pass"] = (
        (profit >= float(config["fundamental_min_profit"]))
        & (goodwill <= float(config["fundamental_max_goodwill_ratio"]))
        & (reduction <= 0)
    )


def _execute_strategy(
    frame: pd.DataFrame,
    trade_indices: pd.Index,
    config: dict[str, Any],
) -> MASRuntimeState:
    state = MASRuntimeState(cash=float(config["cash"]))
    trade_index_set = set(int(item) for item in trade_indices)
    last_short_mid_cross_idx: int | None = None

    for idx, row in frame.iterrows():
        if bool(row.get("cross_short_mid_up", False)):
            last_short_mid_cross_idx = idx

        if idx not in trade_index_set:
            continue

        close = float(row["close"])
        date = pd.Timestamp(row["date"])
        equity_before = _current_equity(state, close)

        if state.shares > 0:
            sell_reason = _resolve_sell_reason(frame, idx, state, config)
            if sell_reason:
                _sell_all(state, row, config, sell_reason)
        else:
            state.stage = 0
            state.seen_short_long_cross = False

        if state.shares == 0:
            stage_signal = _resolve_stage1_signal(frame, idx, last_short_mid_cross_idx, config)
        else:
            stage_signal = _resolve_followup_stage_signal(frame, idx, state, config)

        if stage_signal is not None:
            state.buy_signals_total += 1
            allowed, reasons = _evaluate_entry_filters(frame, idx, config, stage_signal)
            if allowed:
                bought = _buy_stage(state, row, config, stage_signal)
                if bought and stage_signal == 1:
                    state.first_cross_idx = last_short_mid_cross_idx
                    state.entry_price = close
                    state.entry_date = date
                    state.entry_concentration_pct = _safe_float(row.get("chip_concentration_pct"))
            else:
                state.buy_signals_blocked += 1
                _log(
                    state,
                    date,
                    f"MAS第{stage_signal}段买入信号被过滤: {'；'.join(reasons)}",
                    config,
                )

        equity_after = _current_equity(state, close)
        state.equity_records.append((date, equity_after))
        if state.shares > 0:
            state.position_days_total += 1
        else:
            state.idle_cash_days_total += 1
        state.daily_position_ratios.append(
            0.0 if equity_after <= 0 else (state.shares * close / equity_after)
        )

    return state


def _resolve_stage1_signal(
    frame: pd.DataFrame,
    idx: int,
    last_short_mid_cross_idx: int | None,
    config: dict[str, Any],
) -> int | None:
    if last_short_mid_cross_idx is None:
        return None
    days_since_cross = idx - last_short_mid_cross_idx + 1
    if days_since_cross < int(config["short_mid_confirm_days"]):
        return None
    if days_since_cross > int(config["stage1_entry_window"]):
        return None
    window = frame.iloc[last_short_mid_cross_idx : idx + 1]
    if window["ma_short"].isna().any() or window["ma_mid"].isna().any():
        return None
    if not (window["ma_short"] > window["ma_mid"]).all():
        return None
    return 1


def _resolve_followup_stage_signal(
    frame: pd.DataFrame,
    idx: int,
    state: MASRuntimeState,
    config: dict[str, Any],
) -> int | None:
    row = frame.iloc[idx]
    if state.stage == 1:
        if bool(row.get("cross_short_long_up", False)):
            state.seen_short_long_cross = True
            state.second_cross_idx = idx
        if (
            bool(row.get("cross_mid_long_up", False))
            and state.seen_short_long_cross
            and _is_valley_span_valid(idx, state, config)
        ):
            state.third_cross_idx = idx
            return 2
    if state.stage == 2 and _is_stage3_pullback_rebound(frame, idx, config):
        return 3
    return None


def _is_valley_span_valid(idx: int, state: MASRuntimeState, config: dict[str, Any]) -> bool:
    if state.first_cross_idx is None:
        return False
    return idx - state.first_cross_idx + 1 <= int(config["silver_valley_max_span"])


def _is_stage3_pullback_rebound(frame: pd.DataFrame, idx: int, config: dict[str, Any]) -> bool:
    if idx <= 0:
        return False
    row = frame.iloc[idx]
    prev_row = frame.iloc[idx - 1]
    ma_mid = _safe_float(row.get("ma_mid"))
    if ma_mid is None:
        return False
    touched_mid_ma = float(row["low"]) <= ma_mid * (1 + float(config["pullback_tolerance_pct"]))
    did_not_break = float(row["close"]) >= ma_mid * (1 - float(config["pullback_break_tolerance_pct"]))
    rebound = float(row["close"]) > float(prev_row["close"])
    trend_ok = (
        _safe_float(row.get("ma_short")) is not None
        and _safe_float(row.get("ma_long")) is not None
        and float(row["ma_short"]) > ma_mid > float(row["ma_long"])
    )
    return touched_mid_ma and did_not_break and rebound and trend_ok


def _evaluate_entry_filters(
    frame: pd.DataFrame,
    idx: int,
    config: dict[str, Any],
    stage_signal: int,
) -> tuple[bool, list[str]]:
    row = frame.iloc[idx]
    reasons: list[str] = []
    thresholds = _build_effective_entry_thresholds(config)

    if not bool(row.get("fundamental_pass", True)):
        reasons.append("基本面兜底不通过")

    if bool(config["chip_enabled"]):
        concentration = _safe_float(row.get("chip_concentration_pct"))
        if concentration is None:
            reasons.append("筹码集中度不可用")
        elif concentration >= float(config["chip_avoid_min_pct"]):
            reasons.append(f"90%筹码集中度 {concentration:.2f}% >= 规避阈值")
        elif concentration > thresholds["chip_concentration_max_pct"]:
            if concentration <= thresholds["chip_watch_max_pct"]:
                reasons.append(f"90%筹码集中度 {concentration:.2f}% 仅适合观察")
            else:
                reasons.append(f"90%筹码集中度 {concentration:.2f}% 超过买入阈值")
        if bool(config["chip_single_peak_enabled"]) and not bool(row.get("chip_single_peak", False)):
            reasons.append("筹码分布不是单峰密集")

    if bool(config["position_filter_enabled"]):
        recent_gain = _safe_float(row.get("recent_gain_pct"))
        relative_position = _safe_float(row.get("relative_position_pct"))
        if recent_gain is not None and recent_gain > thresholds["recent_gain_abandon_pct"]:
            reasons.append(f"近阶段涨幅 {recent_gain:.2%} 已达到放弃买入区")
        elif recent_gain is not None and recent_gain > thresholds["recent_gain_max_pct"]:
            reasons.append(f"近阶段涨幅 {recent_gain:.2%} 超过低位过滤阈值")
        if relative_position is not None and relative_position > thresholds["relative_position_max_pct"]:
            reasons.append(f"价格相对区间位置 {relative_position:.2%} 偏高")

    volume_required = (
        bool(config["volume_confirm_enabled"])
        and (
            (stage_signal == 1 and bool(config["stage1_require_volume_confirm"]))
            or (stage_signal == 2 and bool(config["stage2_require_volume_confirm"]))
            or (stage_signal == 3 and bool(config["stage3_require_volume_confirm"]))
        )
    )
    if volume_required:
        volume_ratio = _safe_float(row.get("volume_ratio"))
        volume_expand_ratio = _safe_float(row.get("volume_expand_ratio"))
        if volume_ratio is None or volume_ratio < thresholds["volume_ratio_min"]:
            reasons.append("当日量比不足")
        if volume_expand_ratio is None or volume_expand_ratio < thresholds["volume_expand_min_ratio"]:
            reasons.append("近3-5日放量不足")

    short_slope = _safe_float(row.get("ma_short_avg_slope_pct"))
    if short_slope is None or short_slope < thresholds["ma_short_min_avg_slope_pct"]:
        reasons.append("MA5 平均斜率不足")
    if bool(config["require_mid_long_slope_up"]):
        mid_slope = _safe_float(row.get("ma_mid_slope_pct"))
        long_slope = _safe_float(row.get("ma_long_slope_pct"))
        if mid_slope is None or mid_slope < float(config["ma_mid_min_slope_pct"]):
            reasons.append("MA10 斜率未向上")
        if long_slope is None or long_slope < float(config["ma_long_min_slope_pct"]):
            reasons.append("MA20 斜率未向上")

    if stage_signal == 2 and bool(config["edge_ratio_enabled"]):
        edge_ok, edge_reason = _validate_edge_ratio(frame, idx, config)
        if not edge_ok:
            reasons.append(edge_reason)

    return not reasons, reasons


def _validate_edge_ratio(
    frame: pd.DataFrame,
    idx: int,
    config: dict[str, Any],
) -> tuple[bool, str]:
    short_mid_crosses = frame.index[(frame.index <= idx) & frame["cross_short_mid_up"]].tolist()
    short_long_crosses = frame.index[(frame.index <= idx) & frame["cross_short_long_up"]].tolist()
    if not short_mid_crosses or not short_long_crosses:
        return False, "银山谷边长数据不足"
    first_idx = int(short_mid_crosses[-1])
    second_idx = int(short_long_crosses[-1])
    if not (first_idx < second_idx <= idx):
        return False, "银山谷三次交叉顺序不完整"

    first_short = _safe_float(frame.at[first_idx, "ma_short"])
    second_short = _safe_float(frame.at[second_idx, "ma_short"])
    first_mid = _safe_float(frame.at[first_idx, "ma_mid"])
    third_mid = _safe_float(frame.at[idx, "ma_mid"])
    if None in {first_short, second_short, first_mid, third_mid}:
        return False, "银山谷边长均线数据不足"

    short_gain = second_short / first_short - 1
    mid_gain = third_mid / first_mid - 1
    required_mid_gain = short_gain * float(config["edge_mid_gain_multiplier"])
    if mid_gain <= required_mid_gain:
        return False, "MA10 上穿段涨幅未强于 MA5 上穿段"
    return True, ""


def _resolve_sell_reason(
    frame: pd.DataFrame,
    idx: int,
    state: MASRuntimeState,
    config: dict[str, Any],
) -> str | None:
    row = frame.iloc[idx]
    close = float(row["close"])
    average_cost = state.cost_basis / state.shares if state.shares > 0 else 0.0
    if average_cost > 0 and close / average_cost - 1 <= -float(config["hard_stop_loss_pct"]):
        return "硬性止损触发"
    if float(row.get("below_mid_ma_count", 0)) >= int(config["sell_below_mid_ma_days"]):
        return f"连续{int(config['sell_below_mid_ma_days'])}日收盘跌破MA{int(config['ma_mid'])}"
    if bool(config["dead_cross_exit_enabled"]) and bool(row.get("dead_cross_short_long", False)):
        return f"MA{int(config['ma_short'])}死叉MA{int(config['ma_long'])}"
    if _is_chip_warning(row, state, config):
        return "高位筹码集中度放大，疑似派发"
    return None


def _is_chip_warning(
    row: pd.Series,
    state: MASRuntimeState,
    config: dict[str, Any],
) -> bool:
    if not bool(config["chip_warning_enabled"]) or state.entry_price is None:
        return False
    concentration = _safe_float(row.get("chip_concentration_pct"))
    if concentration is None:
        return False
    profit_pct = float(row["close"]) / state.entry_price - 1
    entry_concentration = state.entry_concentration_pct
    return (
        profit_pct >= float(config["chip_warning_min_profit_pct"])
        and concentration >= float(config["chip_warning_concentration_pct"])
        and (
            entry_concentration is None
            or entry_concentration <= float(config["chip_concentration_max_pct"])
        )
    )


def _buy_stage(
    state: MASRuntimeState,
    row: pd.Series,
    config: dict[str, Any],
    stage_signal: int,
) -> bool:
    price = float(row["close"]) * float(config["buy_price_buffer"])
    close = float(row["close"])
    equity = _current_equity(state, close)
    current_position_value = state.shares * close
    max_allowed_value = equity * float(config["max_position_pct"])
    stage_position_pct = float(config[f"stage{stage_signal}_position_pct"])
    stage_budget = min(
        state.cash,
        equity * stage_position_pct,
        max(0.0, max_allowed_value - current_position_value),
    )
    size = estimate_max_buy_size(
        available_cash=stage_budget,
        price=price,
        lot_size=int(config["lot_size"]),
        cash_usage_ratio=1.0,
        config=config,
    )
    if size <= 0:
        state.buy_signals_blocked += 1
        _log(state, pd.Timestamp(row["date"]), f"MAS第{stage_signal}段买入资金不足或不足一手", config)
        return False

    fee = estimate_trade_fee(size, price, is_sell=False, config=config)
    total_cost = size * price + fee
    if total_cost > state.cash + 1e-8:
        state.buy_signals_blocked += 1
        _log(state, pd.Timestamp(row["date"]), f"MAS第{stage_signal}段买入现金不足", config)
        return False

    state.cash -= total_cost
    state.shares += int(size)
    state.cost_basis += total_cost
    state.stage = max(state.stage, stage_signal)
    state.buy_markers.append((pd.Timestamp(row["date"]), close))
    state.buy_trade_records.append(
        {
            "date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"),
            "stage": stage_signal,
            "signal_price": close,
            "trade_price": price,
            "size": int(size),
            "turnover": size * price,
            "commission": fee,
            "cash_after_trade": state.cash,
            "chip_concentration_pct": _safe_float(row.get("chip_concentration_pct")),
            "volume_ratio": _safe_float(row.get("volume_ratio")),
        }
    )
    _log(
        state,
        pd.Timestamp(row["date"]),
        f"MAS第{stage_signal}段买入成交 size={int(size)} price={price:.2f} fee={fee:.2f}",
        config,
    )
    return True


def _sell_all(
    state: MASRuntimeState,
    row: pd.Series,
    config: dict[str, Any],
    reason: str,
) -> None:
    if state.shares <= 0:
        return
    price = float(row["close"]) * float(config["sell_price_buffer"])
    close = float(row["close"])
    size = int(state.shares)
    fee = estimate_trade_fee(size, price, is_sell=True, config=config)
    proceeds = size * price - fee
    pnl = proceeds - state.cost_basis
    state.cash += proceeds
    state.completed_trade_pnls.append(pnl)
    state.sell_markers.append((pd.Timestamp(row["date"]), close))
    state.sell_trade_records.append(
        {
            "date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"),
            "reason": reason,
            "trade_price": price,
            "size": size,
            "commission": fee,
            "net_profit": pnl,
        }
    )
    _log(
        state,
        pd.Timestamp(row["date"]),
        f"MAS卖出成交 reason={reason} size={size} price={price:.2f} pnl={pnl:.2f}",
        config,
    )
    state.shares = 0
    state.cost_basis = 0.0
    state.stage = 0
    state.entry_price = None
    state.entry_date = None
    state.entry_concentration_pct = None
    state.first_cross_idx = None
    state.second_cross_idx = None
    state.third_cross_idx = None
    state.seen_short_long_cross = False


def _build_summary(
    state: MASRuntimeState,
    config: dict[str, Any],
) -> dict[str, Any]:
    equity_series = _build_equity_series(state)
    initial_value = float(config["cash"])
    final_value = float(equity_series.iloc[-1]) if not equity_series.empty else initial_value
    total_return_pct = (final_value / initial_value - 1) * 100.0
    daily_returns = equity_series.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)

    annual_return_pct = None
    if not equity_series.empty and len(equity_series) > 1:
        days = max((equity_series.index[-1] - equity_series.index[0]).days, 1)
        annual_return_pct = ((final_value / initial_value) ** (365.0 / days) - 1) * 100.0

    rolling_peak = equity_series.cummax()
    drawdown = equity_series / rolling_peak - 1
    max_drawdown_pct = abs(float(drawdown.min())) * 100.0 if not drawdown.empty else 0.0
    max_drawdown_amount = float((rolling_peak - equity_series).max()) if not equity_series.empty else 0.0
    drawdown_max_len = _compute_max_drawdown_duration(drawdown)
    sharpe_ratio = _compute_sharpe_ratio(daily_returns)

    trades_total = len(state.completed_trade_pnls)
    trades_won = sum(1 for item in state.completed_trade_pnls if item > 0)
    trades_lost = sum(1 for item in state.completed_trade_pnls if item <= 0)
    net_profit = sum(state.completed_trade_pnls)
    avg_trade_profit = net_profit / trades_total if trades_total else None
    win_rate_pct = trades_won / trades_total * 100.0 if trades_total else 0.0
    avg_capital_usage_pct = (
        sum(state.daily_position_ratios) / len(state.daily_position_ratios) * 100.0
        if state.daily_position_ratios
        else 0.0
    )

    return {
        "initial_value": round(initial_value, 2),
        "final_value": round(final_value, 2),
        "total_return_pct": round(total_return_pct, 2),
        "annual_return_pct": _round_optional(annual_return_pct),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "drawdown_max_len": drawdown_max_len,
        "max_drawdown_amount": round(max_drawdown_amount, 2),
        "sharpe_ratio": _round_optional(sharpe_ratio),
        "trades_total": trades_total,
        "trades_won": trades_won,
        "trades_lost": trades_lost,
        "win_rate_pct": round(win_rate_pct, 2),
        "net_profit": round(net_profit, 2),
        "avg_trade_profit": _round_optional(avg_trade_profit),
        "position_days_total": state.position_days_total,
        "idle_cash_days_total": state.idle_cash_days_total,
        "buy_signals_total": state.buy_signals_total,
        "buy_signals_blocked": state.buy_signals_blocked,
        "capital_usage_tracking_days": len(state.daily_position_ratios),
        "capital_usage_days_weighted": round(sum(state.daily_position_ratios), 2),
        "capital_idle_days_weighted": round(
            sum(1 - item for item in state.daily_position_ratios),
            2,
        ),
        "avg_capital_usage_pct": round(avg_capital_usage_pct, 2),
        "latest_buy_turnover": _round_optional(
            state.buy_trade_records[-1]["turnover"] if state.buy_trade_records else None
        ),
        "avg_buy_turnover": _round_optional(
            sum(item["turnover"] for item in state.buy_trade_records) / len(state.buy_trade_records)
            if state.buy_trade_records
            else None
        ),
        "market_trend_mode": _resolve_market_trend_mode(config),
        "market_trend_label": _resolve_market_trend_label(config),
        "fast_period": int(config["ma_short"]),
        "mid_period": int(config["ma_mid"]),
        "slow_period": int(config["ma_long"]),
        "chip_concentration_max_pct": float(config["chip_concentration_max_pct"]),
        "volume_ratio_min": float(config["volume_ratio_min"]),
        "next_trade_plan_by_position": _build_next_trade_plan(state),
    }


def _build_report_data(
    trade_df: pd.DataFrame,
    state: MASRuntimeState,
    summary: dict[str, Any],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    price_df = trade_df.copy().reset_index(drop=True)
    indicator_lines = [
        {
            "name": f"MA{int(config['ma_short'])}",
            "data": _series_to_chart_values(price_df["ma_short"]),
        },
        {
            "name": f"MA{int(config['ma_mid'])}",
            "data": _series_to_chart_values(price_df["ma_mid"]),
        },
        {
            "name": f"MA{int(config['ma_long'])}",
            "data": _series_to_chart_values(price_df["ma_long"]),
        },
    ]
    buy_points = [
        [
            date.strftime("%Y-%m-%d"),
            round(float(price), 4),
            {
                key: value
                for key, value in state.buy_trade_records[index].items()
                if key != "date"
            },
        ]
        for index, (date, price) in enumerate(state.buy_markers)
    ]
    sell_points = [
        [date.strftime("%Y-%m-%d"), round(float(price), 4)]
        for date, price in state.sell_markers
    ]
    advice_entries = _build_advice_entries(price_df, state, summary)
    kline_payload = {
        "x_axis": price_df["date"].dt.strftime("%Y-%m-%d").tolist(),
        "candles": price_df[["open", "close", "low", "high"]].round(4).values.tolist(),
        "ex_right_closes": _series_to_chart_values(price_df["ex_right_close"]),
        "volumes": price_df["volume"].fillna(0).astype(float).round(4).tolist(),
        "buy_points": buy_points,
        "sell_points": sell_points,
        "indicator_lines": indicator_lines,
        "advice_entries": advice_entries,
    }

    returns_series = _build_equity_series(state).pct_change().fillna(0.0)
    cumulative_returns = (1 + returns_series).cumprod() - 1
    cumulative_returns.name = "MAS"
    yearly_returns = (1 + returns_series).groupby(returns_series.index.year).prod() - 1
    yearly_returns.index = yearly_returns.index.astype(str)
    yearly_returns = yearly_returns * 100.0
    yearly_returns.name = "MAS"

    metrics = _build_metrics_payload(summary, config)
    return [
        {
            "chart_name": "指标概览",
            "chart_data": metrics,
        },
        {
            "chart_name": "买卖点",
            "subtitle": "MAS 银山谷策略信号：MA5/MA10/MA20 + 筹码集中 + 量比确认。",
            "chart_data": kline_payload,
        },
        {
            "chart_name": "累计收益率",
            "chart_data": cumulative_returns,
        },
        {
            "chart_name": "年末收益率",
            "chart_data": yearly_returns,
        },
        {
            "chart_name": "买入交易明细表",
            "subtitle": "逐笔展示 MAS 三段建仓的实际成交、费用、筹码集中度和量比。",
            "chart_data": _build_buy_detail_rows(state, config),
        },
    ]


def _build_metrics_payload(summary: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    total_days = summary["position_days_total"] + summary["idle_cash_days_total"]
    empty_plan = summary["next_trade_plan_by_position"]["empty"]
    hold_plan = summary["next_trade_plan_by_position"]["hold"]
    return {
        "股票代码": config["code"],
        "策略名称": config["strategy_name"],
        "市场环境": summary["market_trend_label"],
        "复权口径": config["adjust_flag"],
        "均线说明": f"MA{config['ma_short']}、MA{config['ma_mid']}、MA{config['ma_long']} 银山谷",
        "初始资金": _format_number(summary["initial_value"]),
        "期末资产": _format_number(summary["final_value"]),
        "总收益率": _format_percent(summary["total_return_pct"]),
        "年化收益率": _format_optional_percent(summary["annual_return_pct"]),
        "最大回撤": _format_optional_percent(summary["max_drawdown_pct"]),
        "最大回撤金额": _format_number(summary["max_drawdown_amount"]),
        "最大回撤周期": summary["drawdown_max_len"],
        "夏普比率": _format_optional_number(summary["sharpe_ratio"]),
        "总交易次数": summary["trades_total"],
        "盈利次数": summary["trades_won"],
        "亏损次数": summary["trades_lost"],
        "胜率": _format_percent(summary["win_rate_pct"]),
        "净利润": _format_number(summary["net_profit"]),
        "平均每笔净利润": _format_optional_number(summary["avg_trade_profit"]),
        "最近一次买入资金额": _format_optional_number(summary["latest_buy_turnover"]),
        "平均单次买入资金额": _format_optional_number(summary["avg_buy_turnover"]),
        "最近一次买入资金额占初始资金比例": (
            _format_percent(summary["latest_buy_turnover"] / summary["initial_value"] * 100)
            if summary["latest_buy_turnover"] is not None
            else "N/A"
        ),
        "买点触发次数": summary["buy_signals_total"],
        "补丁阻止买入次数": summary["buy_signals_blocked"],
        "资金占用天数": summary["position_days_total"],
        "资金占用天数占比": _format_percent(
            summary["position_days_total"] / total_days * 100 if total_days else 0
        ),
        "资金空闲天数": summary["idle_cash_days_total"],
        "资金空闲天数占比": _format_percent(
            summary["idle_cash_days_total"] / total_days * 100 if total_days else 0
        ),
        "平均资金占用率": _format_percent(summary["avg_capital_usage_pct"]),
        "资金加权占用天数": _format_number(summary["capital_usage_days_weighted"]),
        "资金加权空闲天数": _format_number(summary["capital_idle_days_weighted"]),
        "空仓-当日策略": empty_plan["display_action"],
        "空仓-预判摘要": empty_plan["summary"],
        "持仓-当日策略": hold_plan["display_action"],
        "持仓-预判摘要": hold_plan["summary"],
    }


def _build_buy_detail_rows(state: MASRuntimeState, config: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    initial_cash = float(config["cash"])
    for item in state.buy_trade_records:
        turnover = float(item["turnover"])
        rows.append(
            {
                "日期": item["date"],
                "建仓阶段": item["stage"],
                "信号价": _round_optional(item["signal_price"]),
                "成交价": _round_optional(item["trade_price"]),
                "买入数量": item["size"],
                "买入资金额": _round_optional(turnover),
                "手续费": _round_optional(item["commission"]),
                "买后现金余额": _round_optional(item["cash_after_trade"]),
                "占初始资金比例": _round_optional(turnover / initial_cash * 100.0),
                "90%筹码集中度": _round_optional(item.get("chip_concentration_pct")),
                "量比": _round_optional(item.get("volume_ratio")),
            }
        )
    return rows


def _build_advice_entries(
    price_df: pd.DataFrame,
    state: MASRuntimeState,
    summary: dict[str, Any],
) -> list[dict[str, Any]]:
    if price_df.empty:
        return []
    latest_date = price_df["date"].iloc[-1].strftime("%Y-%m-%d")
    buy_dates = {date.strftime("%Y-%m-%d") for date, _ in state.buy_markers}
    sell_dates = {date.strftime("%Y-%m-%d") for date, _ in state.sell_markers}
    entries: list[dict[str, Any]] = []
    for date in price_df["date"].dt.strftime("%Y-%m-%d").tolist():
        if date in sell_dates:
            action = "sell"
            title = "执行卖出"
            summary_text = "MAS 已触发离场信号，优先控制回撤或兑现收益。"
        elif date in buy_dates:
            action = "buy"
            title = "执行买入"
            summary_text = "MAS 已触发分段建仓信号，按计划仓位执行。"
        elif date == latest_date:
            plan = summary["next_trade_plan_by_position"]["hold"]
            action = plan["action"]
            title = plan["display_action"]
            summary_text = plan["summary"]
        else:
            continue
        entries.append(
            {
                "date": date,
                "action": action,
                "title": title,
                "summary": summary_text,
                "reason": summary_text,
            }
        )
    return entries


def _build_next_trade_plan(state: MASRuntimeState) -> dict[str, dict[str, str]]:
    if state.shares > 0:
        hold_plan = {
            "action": "hold",
            "display_action": "继续持有",
            "summary": "当前仍有 MAS 持仓，等待跌破 MA10、MA5 死叉 MA20 或筹码派发预警。",
            "reason": "持仓未触发离场条件。",
        }
        empty_plan = {
            "action": "observe",
            "display_action": "空仓观察",
            "summary": "当前策略已有持仓，空仓状态下等待下一次完整银山谷信号。",
            "reason": "避免同一形态重复追买。",
        }
    else:
        hold_plan = {
            "action": "observe",
            "display_action": "空仓观察",
            "summary": "当前没有 MAS 持仓，等待下一次银山谷、筹码集中和量能确认。",
            "reason": "没有持仓，不需要执行卖出动作。",
        }
        empty_plan = {
            "action": "observe",
            "display_action": "空仓观察",
            "summary": "等待 MA5/MA10/MA20 依次上穿、筹码集中度达标且量比确认。",
            "reason": "当前不追没有确认的信号。",
        }
    return {"empty": empty_plan, "hold": hold_plan}


def _build_equity_series(state: MASRuntimeState) -> pd.Series:
    if not state.equity_records:
        return pd.Series(dtype=float)
    series = pd.Series(
        [value for _, value in state.equity_records],
        index=pd.to_datetime([date for date, _ in state.equity_records]),
        dtype=float,
        name="MAS",
    )
    return series[~series.index.duplicated(keep="last")].sort_index()


def _current_equity(state: MASRuntimeState, close: float) -> float:
    return float(state.cash) + int(state.shares) * float(close)


def _compute_max_drawdown_duration(drawdown: pd.Series) -> int:
    max_duration = 0
    current_duration = 0
    for value in drawdown.fillna(0.0):
        if value < 0:
            current_duration += 1
            max_duration = max(max_duration, current_duration)
        else:
            current_duration = 0
    return max_duration


def _compute_sharpe_ratio(daily_returns: pd.Series) -> float | None:
    returns = daily_returns.dropna()
    if len(returns) < 2:
        return None
    std = float(returns.std(ddof=1))
    if std <= 0:
        return None
    return float(returns.mean() / std * math.sqrt(252))


def _compute_optimization_score(summary: dict[str, Any]) -> float:
    annual = float(summary.get("annual_return_pct") or 0.0)
    drawdown = float(summary.get("max_drawdown_pct") or 0.0)
    sharpe = float(summary.get("sharpe_ratio") or 0.0)
    trades = float(summary.get("trades_total") or 0.0)
    blocked = float(summary.get("buy_signals_blocked") or 0.0)
    return annual - drawdown + sharpe * 10.0 - trades * 0.1 - blocked * 0.02


def _parse_number_range(raw_value: Any, cast_type: type, name: str) -> list[Any]:
    parts = [item.strip() for item in str(raw_value).split(":")]
    if len(parts) not in {2, 3}:
        raise ValueError(f"{name} 格式应为 start:end[:step]")
    start = float(parts[0])
    end = float(parts[1])
    step = float(parts[2]) if len(parts) == 3 else 1.0
    if step <= 0:
        raise ValueError(f"{name} step 必须大于 0")
    values: list[Any] = []
    current = start
    epsilon = step / 1_000_000
    while current <= end + epsilon:
        values.append(cast_type(round(current, 10)))
        current += step
    return values


def _print_summary(summary: dict[str, Any]) -> None:
    print("回测结果:")
    print(f"  市场环境: {summary['market_trend_label']}")
    print(f"  均线周期: MA{summary['fast_period']}/MA{summary['mid_period']}/MA{summary['slow_period']}")
    print(f"  初始资金: {summary['initial_value']:.2f}")
    print(f"  期末资产: {summary['final_value']:.2f}")
    print(f"  总收益率: {summary['total_return_pct']:.2f}%")
    print(f"  年化收益率: {_format_optional_percent(summary['annual_return_pct'])}")
    print(f"  最大回撤: {_format_optional_percent(summary['max_drawdown_pct'])}")
    print(f"  最大回撤金额: {summary['max_drawdown_amount']:.2f}")
    print(f"  夏普比率: {_format_optional_number(summary['sharpe_ratio'])}")
    print(f"  总交易次数: {summary['trades_total']}")
    print(f"  胜率: {summary['win_rate_pct']:.2f}%")
    print(f"  净利润: {summary['net_profit']:.2f}")
    print(f"  买点触发次数: {summary['buy_signals_total']}")
    print(f"  买点过滤次数: {summary['buy_signals_blocked']}")
    print(f"  平均资金占用率: {summary['avg_capital_usage_pct']:.2f}%")


def _series_to_chart_values(series: pd.Series) -> list[float | None]:
    values: list[float | None] = []
    for item in series.tolist():
        if pd.isna(item):
            values.append(None)
        else:
            values.append(round(float(item), 4))
    return values


def _format_number(value: Any) -> str:
    return f"{float(value):,.2f}"


def _format_percent(value: Any) -> str:
    return f"{float(value):.2f}%"


def _format_optional_number(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):,.2f}"


def _format_optional_percent(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}%"


def _round_optional(value: Any, digits: int = 2) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return round(float(value), digits)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _log(state: MASRuntimeState, date: pd.Timestamp, text: str, config: dict[str, Any]) -> None:
    message = f"{date.date().isoformat()} {text}"
    state.log_messages.append(message)
    if bool(config.get("print_log", True)):
        print(message)


if __name__ == "__main__":
    main(CONFIG)

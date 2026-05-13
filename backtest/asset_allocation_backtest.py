from __future__ import annotations

from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent

TEST_CASES = [
    {
        "code": "asset_allocation_placeholder",
        "label": "大类资产配置 / 固收+（占位）",
        "required_codes": ["sz.000100"],
    }
]

CONFIG: dict[str, Any] = {
    "code": "asset_allocation_placeholder",
    "adjust_flag": "hfq",
    "cash": 100000.0,
    "print_log": False,
    "plot": True,
    "report_dir": "logs/backtest",
    "report_name": "asset_allocation_backtest",
    "strategy_name": "固收+策略占位",
}


def validate_config(config: dict[str, Any]) -> None:
    if config["code"] != "asset_allocation_placeholder":
        raise ValueError("当前占位版本仅支持 asset_allocation_placeholder")
    if float(config.get("cash", 0)) <= 0:
        raise ValueError("cash 必须大于 0")


def _write_placeholder_report(config: dict[str, Any], title: str, message: str) -> Path:
    report_dir = PROJECT_ROOT / config["report_dir"]
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{config['report_name']}-{config['code']}.html"
    report_path.write_text(
        f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'PingFang SC', sans-serif; background:#f7f7f7; color:#1f2937; padding:40px; }}
    .card {{ max-width: 820px; margin: 0 auto; background:#fff; border-radius:16px; padding:32px; box-shadow:0 12px 32px rgba(15,23,42,.08); }}
    h1 {{ margin-top:0; font-size:28px; }}
    p {{ line-height:1.8; font-size:16px; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>{title}</h1>
    <p>{message}</p>
  </div>
</body>
</html>
""",
        encoding="utf-8",
    )
    return report_path


def run_backtest(config: dict[str, Any], df: Any = None) -> dict[str, Any]:
    del df
    validate_config(config)
    title = "大类资产配置 / 固收+ 占位策略"
    message = (
        "当前仓库还没有债券、ETF、货基等跨资产数据，因此这里只先保留策略入口。"
        "后续补齐资产池和再平衡规则后，再把它升级为真正可回测版本。"
    )
    if config.get("plot", True):
        report_path = _write_placeholder_report(config, title, message)
        print(f"HTML 回测报告: {report_path}")

    summary = {
        "initial_value": round(float(config["cash"]), 2),
        "final_value": round(float(config["cash"]), 2),
        "total_return_pct": 0.0,
        "annual_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "drawdown_max_len": 0,
        "max_drawdown_amount": 0.0,
        "sharpe_ratio": 0.0,
        "trades_total": 0,
        "trades_won": 0,
        "trades_lost": 0,
        "win_rate_pct": 0.0,
        "net_profit": 0.0,
        "avg_trade_profit": 0.0,
        "position_days_total": 0,
        "idle_cash_days_total": 0,
    }
    print("回测结果:")
    print("  策略状态: 占位")
    print("  说明: 当前仅保留入口，等待后续补充跨资产数据与配置规则")
    return summary


def main(config: dict[str, Any]) -> None:
    run_backtest(config, None)


if __name__ == "__main__":
    main(CONFIG)

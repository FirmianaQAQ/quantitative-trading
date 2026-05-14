from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from contextlib import redirect_stdout
from pathlib import Path
import pandas as pd

from backtest.pair_trade_backtest import (
    _align_pair_data,
    _build_spread_price_frame,
    evaluate_pair_signal_quality,
)
from backtest.strategy_registry import (
    StrategySpec,
    get_required_codes,
    get_selection_label,
    get_strategy_spec,
    group_strategy_specs,
    list_family_strategy_specs,
    list_strategy_specs,
    supports_manual_code_input,
)
from utils.project_utils import get_daily_csv_path, load_daily_data
from utils.default_stocks import DEFAULT_STOCK_NAMES


PROJECT_ROOT = Path(__file__).resolve().parent
DAILY_DIR = PROJECT_ROOT / "data" / "daily"
BACK_MENU_VALUE = "__back__"
EXIT_ALL_MENU_VALUE = "__exit_all__"
MANUAL_MENU_VALUE = "__manual__"
RECOMMEND_MENU_VALUE = "__recommend__"
FULL_EXIT_CODE = 86
DEFAULT_CASH = 100000.0
DEFAULT_ADJUST_ORDER = ("hfq", "qfq", "cq")
DEFAULT_CURRENT_POSITION = "auto"
PAIR_AUTO_PREFIX = "pair_auto|"
MULTI_VERSION_FAMILY_IDS = {"simple_ma_backtest"}


def normalize_code(raw_code: str) -> str:
    code = raw_code.strip().lower()
    if not code:
        raise ValueError("股票代码不能为空")
    if "." in code:
        return code
    if len(code) != 6 or not code.isdigit():
        raise ValueError("股票代码格式错误，应为 sz.000725 或 000725")
    prefix = "sh" if code.startswith("6") else "sz"
    return f"{prefix}.{code}"


def parse_cash_input(raw_value: str) -> float:
    text = raw_value.strip().lower()
    if not text:
        return DEFAULT_CASH

    normalized = text.replace(",", "").replace(" ", "")
    if normalized.endswith("万"):
        normalized = normalized[:-1] + "w"
    if normalized.endswith("w"):
        value = float(normalized[:-1]) * 10000
    else:
        value = float(normalized)

    if value <= 0:
        raise ValueError("初始资金必须大于 0")
    return round(value, 2)


def supports_manual_pair_input(spec: StrategySpec) -> bool:
    return spec.family_id == "pair_trade_backtest"


def parse_manual_pair_selection(raw_value: str) -> str:
    normalized_text = raw_value.strip()
    if not normalized_text:
        raise ValueError("配对组合不能为空")

    if normalized_text.startswith(PAIR_AUTO_PREFIX):
        parts = normalized_text.split("|")
        if len(parts) != 3:
            raise ValueError("配对组合格式错误，应为 pair_auto|sz.000725|sz.002594")
        code_a = normalize_code(parts[1])
        code_b = normalize_code(parts[2])
    else:
        normalized_text = normalized_text.replace("，", ",").replace("/", ",")
        parts = [item.strip() for item in normalized_text.split(",") if item.strip()]
        if len(parts) == 1:
            parts = [item for item in normalized_text.split() if item]
        if len(parts) != 2:
            raise ValueError("请输入两只股票代码，格式如 sz.000725,sz.002594")
        code_a = normalize_code(parts[0])
        code_b = normalize_code(parts[1])

    if code_a == code_b:
        raise ValueError("配对组合里的两只股票不能相同")
    return build_pair_auto_code(code_a, code_b)


def get_stock_label(code: str) -> str:
    return DEFAULT_STOCK_NAMES.get(code, code)


def get_display_label(spec: StrategySpec, code: str) -> str:
    selection_label = get_selection_label(spec, code)
    if selection_label != code:
        return selection_label
    return get_stock_label(code)


def collect_stock_candidates(spec: StrategySpec) -> list[str]:
    codes: set[str] = set()
    code = spec.config.get("code")
    if code:
        codes.add(code)

    for item in spec.test_cases:
        candidate_code = item.get("code")
        if candidate_code:
            codes.add(candidate_code)

    return sorted(codes)


def is_multi_version_family(family_specs: list[StrategySpec]) -> bool:
    return bool(family_specs) and family_specs[0].family_id in MULTI_VERSION_FAMILY_IDS


def prompt_strategy_menu() -> str:
    grouped_specs = group_strategy_specs()
    while True:
        print()
        print("请选择策略大类：")
        for index, (family_name, family_specs) in enumerate(grouped_specs, start=1):
            family_suffix = "（联跑全部版本）" if is_multi_version_family(family_specs) else ""
            print(f"  {index}. {family_name}{family_suffix}")
        print("  b. 返回上一级")
        print("  q. 退出")

        choice = input("请输入编号: ").strip()
        lower_choice = choice.lower()
        if lower_choice == "b":
            return BACK_MENU_VALUE
        if lower_choice == "q":
            return EXIT_ALL_MENU_VALUE
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue

        selected_index = int(choice)
        if not 1 <= selected_index <= len(grouped_specs):
            print("编号超出范围，请重新输入")
            continue

        family_name, family_specs = grouped_specs[selected_index - 1]
        if is_multi_version_family(family_specs):
            print()
            print(f"已选择大类：{family_name}，将自动联跑全部版本")
            return family_specs[0].family_id
        while True:
            print()
            print(f"已选择大类：{family_name}")
            print("请选择具体策略版本：")
            for index, spec in enumerate(family_specs, start=1):
                print(f"  {index}. {spec.display_name} ({spec.brief_description})")
            print("  b. 返回上一级")
            print("  q. 退出")

            sub_choice = input("请输入编号: ").strip()
            lower_sub_choice = sub_choice.lower()
            if lower_sub_choice == "q":
                return EXIT_ALL_MENU_VALUE
            if lower_sub_choice == "b":
                break
            if not sub_choice.isdigit():
                print("输入无效，请输入数字编号")
                continue

            sub_index = int(sub_choice)
            if 1 <= sub_index <= len(family_specs):
                return family_specs[sub_index - 1].strategy_id
            print("编号超出范围，请重新输入")


def prompt_stock_menu(spec: StrategySpec) -> str:
    codes = collect_stock_candidates(spec)
    allow_manual = supports_manual_code_input(spec)
    allow_manual_pair = supports_manual_pair_input(spec)
    while True:
        print()
        print("请选择股票：")
        for index, code in enumerate(codes, start=1):
            print(f"  {index}. {code}  {get_display_label(spec, code)}")
        print("  r. 系统推荐前5")
        if allow_manual:
            print("  0. 手动输入")
        elif allow_manual_pair:
            print("  0. 手动输入两只股票组成配对")
        print("  b. 返回上一级")
        print("  q. 退出")

        choice = input("请输入编号: ").strip()
        lower_choice = choice.lower()
        if lower_choice == "b":
            return BACK_MENU_VALUE
        if lower_choice == "q":
            return EXIT_ALL_MENU_VALUE
        if lower_choice == "r":
            return RECOMMEND_MENU_VALUE
        if (allow_manual or allow_manual_pair) and choice == "0":
            return MANUAL_MENU_VALUE
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue

        selected_index = int(choice)
        if 1 <= selected_index <= len(codes):
            return codes[selected_index - 1]
        print("编号超出范围，请重新输入")


def choose_stock_interactively(spec: StrategySpec) -> str:
    while True:
        selected = prompt_stock_menu(spec)
        if selected == BACK_MENU_VALUE:
            return BACK_MENU_VALUE
        if selected == EXIT_ALL_MENU_VALUE:
            raise SystemExit(FULL_EXIT_CODE)
        if selected == MANUAL_MENU_VALUE:
            while True:
                if supports_manual_pair_input(spec):
                    raw_value = input(
                        "请输入两只股票代码，用逗号或空格分隔，例如 sz.000725, sz.002594: "
                    ).strip()
                else:
                    raw_value = input("请输入股票代码，例如 sz.000725 或 000725: ").strip()
                lower_raw_value = raw_value.lower()
                if lower_raw_value == "b":
                    break
                if lower_raw_value == "q":
                    raise SystemExit(FULL_EXIT_CODE)
                try:
                    if supports_manual_pair_input(spec):
                        return parse_manual_pair_selection(raw_value)
                    return normalize_code(raw_value)
                except ValueError as exc:
                    print(str(exc))
        if selected == RECOMMEND_MENU_VALUE:
            recommended_code = choose_recommended_stock(spec)
            if recommended_code is not None:
                return recommended_code
            continue
        return selected


def prompt_initial_cash() -> float:
    while True:
        raw_value = input("请输入初始资金，直接回车默认 10w: ").strip()
        if raw_value.lower() == "q":
            raise SystemExit(FULL_EXIT_CODE)
        try:
            return parse_cash_input(raw_value)
        except ValueError as exc:
            print(f"输入无效: {exc}")


def prompt_current_position() -> str:
    while True:
        print()
        print("请选择当前实际持仓状态：")
        print("  1. 自动按回测信号推断（默认）")
        print("  2. 当前空仓")
        print("  3. 当前持仓")
        print("  q. 退出")

        raw_value = input("请输入编号，直接回车默认 1: ").strip().lower()
        if not raw_value or raw_value == "1":
            return DEFAULT_CURRENT_POSITION
        if raw_value == "2":
            return "empty"
        if raw_value == "3":
            return "hold"
        if raw_value == "q":
            raise SystemExit(FULL_EXIT_CODE)
        print("输入无效，请重新输入")


def parse_cli_args() -> tuple[str | None, str | None]:
    if len(sys.argv) <= 1:
        return None, None

    strategy_ids = {spec.strategy_id for spec in list_strategy_specs()}
    first_arg = sys.argv[1].strip()
    if first_arg in strategy_ids:
        strategy_id = first_arg
        spec = get_strategy_spec(strategy_id)
        if supports_manual_pair_input(spec):
            if len(sys.argv) >= 4:
                stock_code = parse_manual_pair_selection(
                    f"{sys.argv[2].strip()},{sys.argv[3].strip()}"
                )
            elif len(sys.argv) >= 3:
                stock_code = parse_manual_pair_selection(sys.argv[2].strip())
            else:
                stock_code = None
        else:
            stock_code = sys.argv[2].strip() if len(sys.argv) >= 3 else None
        return strategy_id, stock_code

    return None, normalize_code(first_arg)


def normalize_cli_stock_selection(spec: StrategySpec, cli_stock_code: str | None) -> str | None:
    if not cli_stock_code:
        return None
    if supports_manual_pair_input(spec):
        return parse_manual_pair_selection(cli_stock_code)
    if supports_manual_code_input(spec):
        return normalize_code(cli_stock_code)
    return cli_stock_code


def choose_strategy_spec(cli_strategy_id: str | None) -> StrategySpec:
    if cli_strategy_id:
        return get_strategy_spec(cli_strategy_id)

    selected = prompt_strategy_menu()
    if selected == BACK_MENU_VALUE:
        raise SystemExit(0)
    if selected == EXIT_ALL_MENU_VALUE:
        raise SystemExit(FULL_EXIT_CODE)
    return get_strategy_spec(selected)


def resolve_config(
    spec: StrategySpec,
    stock_selection: str | tuple[str, str] | None,
    cash: float,
    current_position: str = DEFAULT_CURRENT_POSITION,
) -> dict:
    config = dict(spec.config)
    if isinstance(stock_selection, tuple):
        config["code"] = stock_selection[0]
        config["adjust_flag"] = stock_selection[1]
    elif stock_selection:
        config["code"] = stock_selection
    config["cash"] = cash

    config["plot"] = True
    config["print_log"] = False
    config["current_position"] = str(current_position or DEFAULT_CURRENT_POSITION).strip().lower()

    benchmark_code = config.get("benchmark_code", "")
    if benchmark_code:
        benchmark_csv = get_daily_csv_path(benchmark_code, config["adjust_flag"])
        if not benchmark_csv.exists():
            print(f"未找到基准数据 {benchmark_csv}，本次 GUI 报告将不展示基准曲线")
            config["benchmark_code"] = ""

    return config


def sync_single_stock_data(code: str) -> bool:
    print(f"未找到 {code} 的本地数据，正在尝试自动同步...")
    command = [
        str(PROJECT_ROOT / ".venv" / "bin" / "python"),
        str(PROJECT_ROOT / "sync" / "sync_akshare.py"),
        code,
    ]
    result = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    if result.returncode == 0:
        print(f"{code} 数据同步完成")
        return True
    print(f"{code} 数据同步失败，退出码={result.returncode}")
    return False


def build_report_path(config: dict) -> Path:
    report_dir = PROJECT_ROOT / config["report_dir"]
    filename = f"{config['report_name']}-{config['code']}.html"
    return report_dir / filename


def build_family_dashboard_path(family_id: str, code: str) -> Path:
    report_dir = PROJECT_ROOT / "logs" / "backtest"
    filename = f"{family_id}-family-{code}.html"
    return report_dir / filename


def _to_script_safe_json(value: str) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def write_family_dashboard_report(
    *,
    family_name: str,
    code: str,
    stock_label: str,
    cash: float,
    active_strategy_id: str,
    version_reports: list[tuple[StrategySpec, Path]],
) -> Path:
    output_path = build_family_dashboard_path(version_reports[0][0].family_id, code)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    embedded_report_html_by_id: dict[str, str] = {}
    strategy_ids = {item[0].strategy_id for item in version_reports}
    buttons_html = []
    panels_html = []
    for index, (spec, report_path) in enumerate(version_reports):
        is_active = spec.strategy_id == active_strategy_id or (
            active_strategy_id not in strategy_ids
            and index == 0
        )
        button_class = "version-tab is-active" if is_active else "version-tab"
        panel_style = "" if is_active else " style=\"display:none;\""
        embedded_report_html_by_id[spec.strategy_id] = report_path.read_text(
            encoding="utf-8"
        )
        buttons_html.append(
            f"""
            <button
              type="button"
              class="{button_class}"
              data-target="{spec.strategy_id}"
            >
              <span class="version-tab-title">{spec.display_name}</span>
              <span class="version-tab-desc">{spec.brief_description}</span>
            </button>
            """
        )
        panels_html.append(
            f"""
            <section class="version-panel" data-panel="{spec.strategy_id}"{panel_style}>
              <iframe
                class="version-frame"
                title="{spec.display_name}"
                loading="lazy"
              ></iframe>
            </section>
            """
        )

    embedded_reports_json = "{{{pairs}}}".format(
        pairs=", ".join(
            f"{json.dumps(strategy_id, ensure_ascii=False)}: {_to_script_safe_json(report_html)}"
            for strategy_id, report_html in embedded_report_html_by_id.items()
        )
    )

    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{family_name} 多版本对比</title>
  <style>
    :root {{
      --bg: #f4f7fb;
      --card: #ffffff;
      --text: #243043;
      --muted: #667085;
      --line: rgba(148, 163, 184, 0.22);
      --shadow: 0 14px 40px rgba(15, 23, 42, 0.10);
      --accent: #5470c6;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(84, 112, 198, 0.12), transparent 26%),
        radial-gradient(circle at top right, rgba(145, 204, 117, 0.10), transparent 24%),
        var(--bg);
    }}
    .page {{
      max-width: 1680px;
      margin: 0 auto;
      padding: 24px 20px 32px;
    }}
    .hero {{
      margin-bottom: 18px;
      padding: 22px 24px;
      border: 1px solid rgba(229, 231, 235, 0.9);
      border-radius: 24px;
      background: rgba(255, 255, 255, 0.92);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .hero h1 {{
      margin: 0;
      font-size: 30px;
      line-height: 1.2;
    }}
    .hero p {{
      margin: 10px 0 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.7;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 16px;
    }}
    .version-tab {{
      min-width: 180px;
      padding: 14px 16px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: linear-gradient(180deg, #ffffff, #f8fbff);
      box-shadow: 0 8px 24px rgba(148, 163, 184, 0.12);
      cursor: pointer;
      text-align: left;
      transition: all 0.18s ease;
    }}
    .version-tab:hover {{
      transform: translateY(-1px);
      box-shadow: 0 12px 28px rgba(84, 112, 198, 0.16);
    }}
    .version-tab.is-active {{
      border-color: rgba(84, 112, 198, 0.38);
      background: linear-gradient(180deg, rgba(84, 112, 198, 0.16), rgba(84, 112, 198, 0.06));
      box-shadow: 0 14px 30px rgba(84, 112, 198, 0.18);
    }}
    .version-tab-title {{
      display: block;
      font-size: 16px;
      font-weight: 700;
      color: var(--text);
    }}
    .version-tab-desc {{
      display: block;
      margin-top: 6px;
      font-size: 12px;
      color: var(--muted);
    }}
    .version-panel {{
      border: 1px solid rgba(229, 231, 235, 0.9);
      border-radius: 24px;
      overflow: hidden;
      background: var(--card);
      box-shadow: var(--shadow);
    }}
    .version-frame {{
      display: block;
      width: 100%;
      min-height: calc(100vh - 180px);
      height: 1800px;
      border: 0;
      background: #fff;
    }}
    @media (max-width: 768px) {{
      .page {{
        padding: 16px 12px 24px;
      }}
      .hero h1 {{
        font-size: 24px;
      }}
      .toolbar {{
        flex-direction: column;
      }}
      .version-tab {{
        width: 100%;
      }}
      .version-frame {{
        height: 1400px;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <h1>{family_name} 多版本对比</h1>
      <p>股票：{code} {stock_label} ｜ 初始资金：{cash:,.2f} ｜ 当前页面一次联跑并对比同一策略家族下的多个版本，方便直接切换查看。</p>
    </header>
    <div class="toolbar">
      {''.join(buttons_html)}
    </div>
    <main>
      {''.join(panels_html)}
    </main>
  </div>
  <script>
    (function () {{
      const tabs = Array.from(document.querySelectorAll('.version-tab'));
      const panels = Array.from(document.querySelectorAll('.version-panel'));
      const embeddedReports = {embedded_reports_json};

      function ensurePanelLoaded(panel) {{
        if (!panel) return;
        const iframe = panel.querySelector('.version-frame');
        if (!iframe || iframe.dataset.loaded === 'true') return;
        iframe.srcdoc = embeddedReports[panel.dataset.panel] || '<!DOCTYPE html><html lang="zh-CN"><body><p>未找到内嵌报告内容。</p></body></html>';
        iframe.dataset.loaded = 'true';
      }}

      function activate(target) {{
        tabs.forEach((tab) => {{
          tab.classList.toggle('is-active', tab.dataset.target === target);
        }});
        panels.forEach((panel) => {{
          const isActive = panel.dataset.panel === target;
          panel.style.display = isActive ? '' : 'none';
          if (isActive) {{
            ensurePanelLoaded(panel);
          }}
        }});
      }}
      tabs.forEach((tab) => {{
        tab.addEventListener('click', function () {{
          activate(tab.dataset.target);
        }});
      }});
      const activeTab = tabs.find((tab) => tab.classList.contains('is-active')) || tabs[0];
      if (activeTab) {{
        activate(activeTab.dataset.target);
      }}
    }})();
  </script>
</body>
</html>
"""
    output_path.write_text(html_text, encoding="utf-8")
    return output_path


def try_open_report(report_path: Path) -> None:
    if os.getenv("OPEN_GUI", "1") != "1":
        return
    if sys.platform != "darwin":
        return
    if not report_path.exists():
        return

    open_commands = [
        ["open", "-a", "Google Chrome", str(report_path)],
        ["open", "-a", "Safari", str(report_path)],
        ["open", str(report_path)],
    ]
    for command in open_commands:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return

    print(f"自动打开 GUI 失败，请手动打开报告: {report_path}")


def collect_available_adjust_flags(code: str) -> list[str]:
    if not DAILY_DIR.exists():
        return []
    adjust_flags: set[str] = set()
    for path in DAILY_DIR.glob(f"{code}_*.csv"):
        stem = path.stem
        if "_" not in stem:
            continue
        code_part, adjust_flag = stem.rsplit("_", 1)
        if code_part == code and adjust_flag:
            adjust_flags.add(adjust_flag)
    return sorted(
        adjust_flags,
        key=lambda flag: (
            0
            if flag == DEFAULT_ADJUST_ORDER[0]
            else (
                DEFAULT_ADJUST_ORDER.index(flag)
                if flag in DEFAULT_ADJUST_ORDER
                else len(DEFAULT_ADJUST_ORDER)
            ),
            flag,
        ),
    )


def collect_recommendation_adjust_flags(spec: StrategySpec, code: str) -> list[str]:
    required_codes = get_required_codes(spec, code)
    if not required_codes:
        return []

    available_flag_sets = [
        set(collect_available_adjust_flags(required_code)) for required_code in required_codes
    ]
    if not available_flag_sets or any(not item for item in available_flag_sets):
        return []

    common_flags = set.intersection(*available_flag_sets)
    preferred_flag = str(spec.config.get("adjust_flag", ""))
    sort_order = []
    if preferred_flag:
        sort_order.append(preferred_flag)
    sort_order.extend(flag for flag in DEFAULT_ADJUST_ORDER if flag != preferred_flag)

    return sorted(
        common_flags,
        key=lambda flag: (
            sort_order.index(flag) if flag in sort_order else len(sort_order),
            flag,
        ),
    )


def collect_local_stock_codes(adjust_flag: str) -> list[str]:
    if not DAILY_DIR.exists():
        return []

    codes: set[str] = set()
    for path in DAILY_DIR.glob(f"*_{adjust_flag}.csv"):
        stem = path.stem
        if "_" not in stem:
            continue
        code, _flag = stem.rsplit("_", 1)
        if code.startswith(("sh.", "sz.")):
            codes.add(code)
    return sorted(codes)


def build_pair_auto_code(code_a: str, code_b: str) -> str:
    left, right = sorted((code_a, code_b))
    return f"{PAIR_AUTO_PREFIX}{left}|{right}"


def build_pair_recommendation_candidates(spec: StrategySpec) -> list[tuple[str, str]]:
    ranked_pairs: list[tuple[float, str, str]] = []
    candidate_flags = [str(spec.config.get("adjust_flag", ""))]
    candidate_flags.extend(
        flag for flag in DEFAULT_ADJUST_ORDER if flag not in candidate_flags
    )

    for adjust_flag in candidate_flags:
        if not adjust_flag:
            continue
        codes = collect_local_stock_codes(adjust_flag)
        if len(codes) < 2:
            continue

        price_series: list[pd.Series] = []
        for code in codes:
            try:
                df = load_daily_data(code, adjust_flag)
            except Exception:
                continue
            if df.empty or "date" not in df.columns or "close" not in df.columns:
                continue
            series = (
                df[["date", "close"]]
                .copy()
                .assign(date=lambda item: pd.to_datetime(item["date"]))
                .drop_duplicates(subset=["date"], keep="last")
                .set_index("date")["close"]
                .sort_index()
                .tail(500)
                .rename(code)
            )
            if len(series) < 120:
                continue
            price_series.append(series)

        if len(price_series) < 2:
            continue

        price_df = pd.concat(price_series, axis=1, sort=False)
        returns_df = price_df.pct_change(fill_method=None)
        corr_df = returns_df.corr(min_periods=120)
        columns = list(corr_df.columns)
        for left_index in range(len(columns)):
            for right_index in range(left_index + 1, len(columns)):
                correlation = corr_df.iat[left_index, right_index]
                if pd.isna(correlation) or float(correlation) < float(
                    spec.config.get("selection_min_correlation", 0.75)
                ):
                    continue
                left_code = columns[left_index]
                right_code = columns[right_index]
                try:
                    _aligned_a, _aligned_b, merged_df = _align_pair_data(
                        left_code,
                        right_code,
                        adjust_flag,
                    )
                    signal_frame = _build_spread_price_frame(
                        merged_df,
                        int(spec.config.get("lookback", 60)),
                    )
                    quality = evaluate_pair_signal_quality(
                        signal_frame,
                        selection_window=int(spec.config.get("selection_window", 500)),
                        min_correlation=float(
                            spec.config.get("selection_min_correlation", 0.75)
                        ),
                        min_zero_crossings=int(
                            spec.config.get("selection_min_zero_crossings", 6)
                        ),
                        max_half_life=float(
                            spec.config.get("selection_max_half_life", 60)
                        ),
                    )
                except Exception:
                    continue
                if quality is None:
                    continue
                ranked_pairs.append(
                    (
                        float(quality["score"]),
                        build_pair_auto_code(left_code, right_code),
                        adjust_flag,
                    )
                )

    ranked_pairs.sort(key=lambda item: item[0], reverse=True)
    deduped_candidates: list[tuple[str, str]] = []
    seen_pair_codes: set[str] = set()
    for _score, pair_code, adjust_flag in ranked_pairs:
        if pair_code in seen_pair_codes:
            continue
        seen_pair_codes.add(pair_code)
        deduped_candidates.append((pair_code, adjust_flag))
        if len(deduped_candidates) >= 30:
            break
    return deduped_candidates


def build_recommendation_candidates(spec: StrategySpec) -> list[tuple[str, str]]:
    if spec.family_id == "pair_trade_backtest":
        return build_pair_recommendation_candidates(spec)

    available_candidates: list[tuple[str, str]] = []
    for code in collect_stock_candidates(spec):
        for adjust_flag in collect_recommendation_adjust_flags(spec, code):
            available_candidates.append((code, adjust_flag))
    return available_candidates


def evaluate_stock_for_recommendation(
    spec: StrategySpec,
    code: str,
    adjust_flag: str,
) -> dict | None:
    config = dict(spec.config)
    config.update(
        {
            "code": code,
            "adjust_flag": adjust_flag,
            "plot": False,
            "print_log": False,
            "benchmark_code": "",
        }
    )
    try:
        spec.validate_config(config)
        df = (
            load_daily_data(config["code"], config["adjust_flag"])
            if get_required_codes(spec, code) == [code]
            else None
        )
        with redirect_stdout(io.StringIO()):
            summary = spec.run_backtest(config, df)
        return summary
    except Exception:
        return None


def choose_recommended_stock(spec: StrategySpec) -> tuple[str, str] | None:
    candidate_items = build_recommendation_candidates(spec)
    if not candidate_items:
        print("当前没有可用于推荐的本地股票数据，请先同步数据")
        return None

    print()
    print(f"系统正在基于 {spec.display_name} 评估本地股票，请稍等...")
    ranked_results: list[tuple[str, str, dict]] = []
    for code, adjust_flag in candidate_items:
        summary = evaluate_stock_for_recommendation(spec, code, adjust_flag)
        if summary is None or summary.get("annual_return_pct") is None:
            continue
        ranked_results.append((code, adjust_flag, summary))

    if not ranked_results:
        print("系统推荐失败，当前没有可用的回测结果")
        return None

    ranked_results.sort(
        key=lambda item: (
            item[2]["annual_return_pct"],
            item[2]["sharpe_ratio"] if item[2]["sharpe_ratio"] is not None else float("-inf"),
            -(item[2]["max_drawdown_pct"] if item[2]["max_drawdown_pct"] is not None else float("inf")),
        ),
        reverse=True,
    )
    top_results = ranked_results[:5]

    while True:
        print()
        print("系统推荐前5：")
        for index, (code, adjust_flag, summary) in enumerate(top_results, start=1):
            annual_text = f"{summary['annual_return_pct']:.2f}%"
            drawdown_text = (
                f"{summary['max_drawdown_pct']:.2f}%"
                if summary["max_drawdown_pct"] is not None
                else "N/A"
            )
            sharpe_text = (
                f"{summary['sharpe_ratio']:.2f}"
                if summary["sharpe_ratio"] is not None
                else "N/A"
            )
            print(
                f"  {index}. {code}  {get_display_label(spec, code)}  "
                f"[{adjust_flag}]  年化={annual_text}  回撤={drawdown_text}  夏普={sharpe_text}"
            )
        print("  b. 返回上一级")
        print("  q. 退出")

        choice = input("请输入编号: ").strip()
        lower_choice = choice.lower()
        if lower_choice == "b":
            return None
        if lower_choice == "q":
            raise SystemExit(FULL_EXIT_CODE)
        if not choice.isdigit():
            print("输入无效，请输入数字编号")
            continue
        selected_index = int(choice)
        if 1 <= selected_index <= len(top_results):
            selected_code, selected_adjust_flag, _summary = top_results[selected_index - 1]
            return selected_code, selected_adjust_flag
        print("编号超出范围，请重新输入")


def validate_required_data_files(spec: StrategySpec, config: dict) -> None:
    missing_files: list[Path] = []
    for code in get_required_codes(spec, config["code"]):
        csv_path = get_daily_csv_path(code, config["adjust_flag"])
        if csv_path.exists():
            continue
        sync_single_stock_data(code)
        if not csv_path.exists():
            missing_files.append(csv_path)

    if not missing_files:
        return

    missing_text = "\n".join(str(path) for path in missing_files)
    raise FileNotFoundError(
        "缺少 GUI 回测所需日线数据文件，且自动同步失败。请手动执行 ./sync_data.sh 同步数据：\n"
        f"{missing_text}"
    )


def run_single_strategy(
    spec: StrategySpec,
    stock_selection: str | tuple[str, str],
    cash: float,
    *,
    current_position: str = DEFAULT_CURRENT_POSITION,
    preload_df: pd.DataFrame | None = None,
) -> tuple[dict, Path]:
    config = resolve_config(spec, stock_selection, cash, current_position)
    print(f"已选择策略: {spec.display_name} ({spec.strategy_id})")
    print(f"已选择股票: {config['code']} {get_display_label(spec, config['code'])}")
    print(f"初始资金: {config['cash']:.2f}")

    validate_required_data_files(spec, config)
    spec.validate_config(config)
    df = preload_df
    if df is None and get_required_codes(spec, config["code"]) == [config["code"]]:
        df = load_daily_data(config["code"], config["adjust_flag"])
    spec.run_backtest(config, df)
    return config, build_report_path(config)


def run_simple_ma_family(
    selected_spec: StrategySpec,
    stock_selection: str | tuple[str, str],
    cash: float,
    current_position: str = DEFAULT_CURRENT_POSITION,
) -> Path:
    family_specs = list_family_strategy_specs(selected_spec.family_id)
    if not family_specs:
        raise RuntimeError("未找到普通双均线家族策略")

    selected_config = resolve_config(
        selected_spec,
        stock_selection,
        cash,
        current_position,
    )
    selected_code = selected_config["code"]
    selected_label = get_display_label(selected_spec, selected_code)

    df_cache: dict[tuple[str, str], pd.DataFrame] = {}
    version_reports: list[tuple[StrategySpec, Path]] = []
    for index, family_spec in enumerate(family_specs, start=1):
        print()
        print(f"===== 联跑版本 {index}/{len(family_specs)}：{family_spec.display_name} =====")
        preview_config = resolve_config(
            family_spec,
            stock_selection,
            cash,
            current_position,
        )
        df_key = (preview_config["code"], preview_config["adjust_flag"])
        preload_df = None
        if get_required_codes(family_spec, preview_config["code"]) == [preview_config["code"]]:
            if df_key not in df_cache:
                validate_required_data_files(family_spec, preview_config)
                df_cache[df_key] = load_daily_data(
                    preview_config["code"],
                    preview_config["adjust_flag"],
                )
            preload_df = df_cache[df_key]
        config, report_path = run_single_strategy(
            family_spec,
            stock_selection,
            cash,
            current_position=current_position,
            preload_df=preload_df,
        )
        version_reports.append((family_spec, report_path))

    dashboard_path = write_family_dashboard_report(
        family_name=selected_spec.family_display_name,
        code=selected_code,
        stock_label=selected_label,
        cash=cash,
        active_strategy_id=selected_spec.strategy_id,
        version_reports=version_reports,
    )
    print()
    print(f"多版本对比报告: {dashboard_path}")
    return dashboard_path


def main() -> None:
    cli_strategy_id, cli_stock_code = parse_cli_args()
    while True:
        spec = choose_strategy_spec(cli_strategy_id)
        stock_code = normalize_cli_stock_selection(spec, cli_stock_code) or choose_stock_interactively(spec)
        if stock_code == BACK_MENU_VALUE:
            cli_strategy_id = None
            cli_stock_code = None
            continue
        break
    cash = prompt_initial_cash()
    current_position = prompt_current_position()
    if spec.family_id == "simple_ma_backtest":
        report_path = run_simple_ma_family(
            spec,
            stock_code,
            cash,
            current_position,
        )
    else:
        _config, report_path = run_single_strategy(
            spec,
            stock_code,
            cash,
            current_position=current_position,
        )
        print(f"GUI 回测报告: {report_path}")
    try_open_report(report_path)


if __name__ == "__main__":
    main()

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any
from urllib import error, request

import pandas as pd

from analysis.config import load_llm_analysis_settings
from analysis.payload_builder import (
    build_batch_analysis_payload,
    build_pair_analysis_payload,
    build_single_stock_analysis_payload,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LLM_REPORT_DIR = PROJECT_ROOT / "logs" / "llm_analysis"
REQUIRED_OUTPUT_KEYS = {
    "score",
    "conclusion",
    "strengths",
    "risks",
    "regime_fit",
    "next_action",
    "confidence",
}


def maybe_generate_single_stock_analysis(
    config: dict[str, Any],
    summary: dict[str, Any],
    df: pd.DataFrame,
) -> Path | None:
    if not config.get("enable_llm_analysis"):
        return None

    output_path = _build_single_report_path(config)
    title = f"{config.get('strategy_name', '策略')} {config.get('code')} 大模型分析报告"
    try:
        payload = build_single_stock_analysis_payload(config, summary, df)
        settings, result = _request_analysis_result(
            payload=payload,
            task_title="单策略回测分析",
        )
        _write_html_report(
            output_path=output_path,
            title=title,
            settings=settings,
            payload=payload,
            result=result,
        )
        print(f"AI 分析报告: {output_path}")
        return output_path
    except Exception as exc:
        return _handle_analysis_failure(
            output_path=output_path,
            title=title,
            error_message=str(exc),
        )


def maybe_generate_pair_analysis(
    config: dict[str, Any],
    summary: dict[str, Any],
    spread_price_df: pd.DataFrame,
    pair_label: str,
    pair_quality: dict[str, Any] | None,
) -> Path | None:
    if not config.get("enable_llm_analysis"):
        return None

    output_path = _build_single_report_path(config)
    title = f"{pair_label} 大模型分析报告"
    try:
        payload = build_pair_analysis_payload(
            config=config,
            summary=summary,
            spread_price_df=spread_price_df,
            pair_label=pair_label,
            pair_quality=pair_quality,
        )
        settings, result = _request_analysis_result(
            payload=payload,
            task_title="配对交易回测分析",
        )
        _write_html_report(
            output_path=output_path,
            title=title,
            settings=settings,
            payload=payload,
            result=result,
        )
        print(f"AI 分析报告: {output_path}")
        return output_path
    except Exception as exc:
        return _handle_analysis_failure(
            output_path=output_path,
            title=title,
            error_message=str(exc),
        )


def maybe_generate_batch_analysis(
    *,
    strategy_id: str,
    strategy_name: str,
    batch_results: list[dict[str, Any]],
) -> Path | None:
    if not batch_results:
        return None
    if not batch_results[0].get("enable_llm_analysis"):
        return None

    output_path = _build_batch_report_path(strategy_id)
    title = f"{strategy_name} 批量回测大模型分析报告"
    try:
        payload = build_batch_analysis_payload(
            strategy_id=strategy_id,
            strategy_name=strategy_name,
            batch_results=batch_results,
        )
        settings, result = _request_analysis_result(
            payload=payload,
            task_title="批量回测横向分析",
        )
        _write_html_report(
            output_path=output_path,
            title=title,
            settings=settings,
            payload=payload,
            result=result,
        )
        print(f"AI 批量分析报告: {output_path}")
        return output_path
    except Exception as exc:
        return _handle_analysis_failure(
            output_path=output_path,
            title=title,
            error_message=str(exc),
        )


def _request_analysis_result(
    *,
    payload: dict[str, Any],
    task_title: str,
) -> tuple[Any, dict[str, Any]]:
    settings = load_llm_analysis_settings()
    if not settings.enabled:
        raise ValueError("当前未启用大模型分析")

    prompt = _build_user_prompt(task_title, payload)
    request_payload = {
        "model": settings.model,
        "temperature": settings.temperature,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _build_system_prompt()},
            {"role": "user", "content": prompt},
        ],
    }
    body = json.dumps(request_payload).encode("utf-8")
    http_request = request.Request(
        url=f"{settings.base_url}/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.api_key}",
        },
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=settings.timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(_format_http_error_message(exc.code, detail)) from exc
    except error.URLError as exc:
        raise RuntimeError(f"大模型分析请求失败: {exc.reason}") from exc

    content = (
        response_payload.get("choices", [{}])[0]
        .get("message", {})
        .get("content")
    )
    if not content:
        raise RuntimeError("大模型返回内容为空")

    try:
        result = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"大模型返回的内容不是合法 JSON: {content}") from exc

    missing_keys = sorted(REQUIRED_OUTPUT_KEYS - set(result.keys()))
    if missing_keys:
        missing_text = ", ".join(missing_keys)
        raise ValueError(f"大模型分析结果缺少字段: {missing_text}")
    return settings, result


def _build_system_prompt() -> str:
    return (
        "你是资深量化研究员。"
        "你只能基于用户提供的结构化回测数据做分析，不允许编造不存在的行情、财务或新闻信息。"
        "你必须输出 JSON 对象，字段固定为："
        "score, conclusion, strengths, risks, regime_fit, next_action, confidence。"
        "其中 strengths 和 risks 必须是字符串数组，score 和 confidence 是 0 到 100 的整数。"
        "结论必须聚焦策略表现、风险来源、适用行情和下一步研究建议，不能给出直接实盘买卖指令。"
    )


def _build_user_prompt(task_title: str, payload: dict[str, Any]) -> str:
    return (
        f"任务：{task_title}\n"
        "请严格基于下面的 JSON 数据输出分析结果。\n"
        "如果数据不足以支撑强结论，请在 risks 和 next_action 里明确指出。\n"
        "输入 JSON：\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def _format_http_error_message(status_code: int, detail: str) -> str:
    parsed_detail = detail
    try:
        payload = json.loads(detail)
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        error_info = payload.get("error", {})
        message = error_info.get("message")
        code = error_info.get("code")
        if message:
            parsed_detail = str(message)
            if code:
                parsed_detail = f"{parsed_detail} ({code})"

    if status_code == 402:
        return f"大模型分析请求失败: HTTP 402 余额不足，详情: {parsed_detail}"
    return f"大模型分析请求失败: HTTP {status_code} {parsed_detail}"


def _write_html_report(
    *,
    output_path: Path,
    title: str,
    settings: Any,
    payload: dict[str, Any],
    result: dict[str, Any],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    strengths_html = "".join(
        f"<li>{_escape_html(str(item))}</li>" for item in result["strengths"]
    )
    risks_html = "".join(
        f"<li>{_escape_html(str(item))}</li>" for item in result["risks"]
    )
    payload_json = _escape_html(json.dumps(payload, ensure_ascii=False, indent=2))
    result_json = _escape_html(json.dumps(result, ensure_ascii=False, indent=2))
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_escape_html(title)}</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --text: #172033;
      --muted: #667085;
      --primary: #1d4ed8;
      --primary-soft: rgba(29, 78, 216, 0.12);
      --border: #e5e7eb;
      --shadow: 0 16px 40px rgba(15, 23, 42, 0.10);
      --success: #047857;
      --danger: #b42318;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(29, 78, 216, 0.10), transparent 26%),
        radial-gradient(circle at top right, rgba(22, 163, 74, 0.08), transparent 24%),
        var(--bg);
      color: var(--text);
    }}
    .container {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .hero {{
      padding: 28px 30px;
      border-radius: 24px;
      background: linear-gradient(135deg, #163b85, #245cc7 54%, #3b82f6);
      color: #fff;
      box-shadow: var(--shadow);
      margin-bottom: 20px;
    }}
    .hero h1 {{
      margin: 0;
      font-size: 30px;
      line-height: 1.2;
    }}
    .hero-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 14px;
    }}
    .hero-pill {{
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.16);
      font-size: 13px;
      font-weight: 600;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 20px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid rgba(229, 231, 235, 0.9);
      border-radius: 20px;
      box-shadow: var(--shadow);
      padding: 20px;
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .metric-value {{
      font-size: 28px;
      font-weight: 800;
      color: var(--primary);
    }}
    .section-title {{
      margin: 0 0 12px;
      font-size: 18px;
    }}
    .section-body {{
      color: #344054;
      line-height: 1.75;
      white-space: pre-wrap;
    }}
    .list {{
      margin: 0;
      padding-left: 18px;
      line-height: 1.8;
    }}
    .list.is-risk li {{ color: var(--danger); }}
    .list.is-strength li {{ color: var(--success); }}
    details {{
      margin-top: 12px;
      border-top: 1px solid var(--border);
      padding-top: 12px;
    }}
    summary {{
      cursor: pointer;
      color: var(--primary);
      font-weight: 700;
    }}
    pre {{
      margin: 12px 0 0;
      padding: 14px;
      border-radius: 14px;
      background: #0f172a;
      color: #e2e8f0;
      overflow-x: auto;
      font-size: 12px;
      line-height: 1.6;
    }}
    @media (max-width: 768px) {{
      .container {{ padding: 20px 12px 32px; }}
      .hero {{ padding: 22px 20px; }}
      .hero h1 {{ font-size: 24px; }}
      .metric-value {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1>{_escape_html(title)}</h1>
      <div class="hero-meta">
        <span class="hero-pill">生成时间：{_escape_html(generated_at)}</span>
        <span class="hero-pill">模型提供方：{_escape_html(settings.provider)}</span>
        <span class="hero-pill">模型名称：{_escape_html(str(settings.model))}</span>
      </div>
    </section>
    <section class="grid">
      <article class="card">
        <div class="metric-label">综合评分</div>
        <div class="metric-value">{_escape_html(str(result["score"]))}</div>
      </article>
      <article class="card">
        <div class="metric-label">结论置信度</div>
        <div class="metric-value">{_escape_html(str(result["confidence"]))}</div>
      </article>
    </section>
    <section class="card">
      <h2 class="section-title">结论</h2>
      <div class="section-body">{_escape_html(str(result["conclusion"]).strip())}</div>
    </section>
    <section class="grid">
      <article class="card">
        <h2 class="section-title">优势</h2>
        <ul class="list is-strength">{strengths_html}</ul>
      </article>
      <article class="card">
        <h2 class="section-title">风险</h2>
        <ul class="list is-risk">{risks_html}</ul>
      </article>
    </section>
    <section class="card">
      <h2 class="section-title">适用行情</h2>
      <div class="section-body">{_escape_html(str(result["regime_fit"]).strip())}</div>
    </section>
    <section class="card">
      <h2 class="section-title">下一步建议</h2>
      <div class="section-body">{_escape_html(str(result["next_action"]).strip())}</div>
      <details>
        <summary>查看输入摘要 JSON</summary>
        <pre>{payload_json}</pre>
      </details>
      <details>
        <summary>查看原始模型输出 JSON</summary>
        <pre>{result_json}</pre>
      </details>
    </section>
  </div>
</body>
</html>
"""
    output_path.write_text(html_text, encoding="utf-8")


def _handle_analysis_failure(
    *,
    output_path: Path,
    title: str,
    error_message: str,
) -> Path:
    failure_path = output_path.with_suffix(".failed.html")
    failure_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html_text = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_escape_html(title)} 失败报告</title>
  <style>
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      background: #f8fafc;
      color: #1f2937;
    }}
    .container {{
      max-width: 900px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .card {{
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 18px;
      box-shadow: 0 16px 40px rgba(15, 23, 42, 0.08);
      padding: 24px;
    }}
    .badge {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(180, 35, 24, 0.10);
      color: #b42318;
      font-weight: 700;
      font-size: 12px;
      margin-bottom: 12px;
    }}
    h1 {{ margin: 0 0 12px; font-size: 28px; }}
    p {{ line-height: 1.8; color: #475467; }}
    .meta {{
      margin-top: 18px;
      padding: 14px;
      border-radius: 14px;
      background: #fff5f5;
      color: #912018;
      white-space: pre-wrap;
      word-break: break-word;
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="card">
      <div class="badge">AI 分析失败</div>
      <h1>{_escape_html(title)} 失败报告</h1>
      <p>生成时间：{_escape_html(generated_at)}</p>
      <p>本次大模型分析失败，但回测主流程已经完成。你可以补充余额、切换模型提供方，或临时关闭 AI 分析后继续使用回测功能。</p>
      <div class="meta">失败原因：{_escape_html(error_message)}</div>
    </section>
  </div>
</body>
</html>
"""
    failure_path.write_text(html_text, encoding="utf-8")
    print(f"AI 分析失败: {error_message}")
    print(f"AI 失败报告: {failure_path}")
    return failure_path


def _build_single_report_path(config: dict[str, Any]) -> Path:
    return LLM_REPORT_DIR / f"{config['report_name']}-{config['code']}.html"


def _build_batch_report_path(strategy_id: str) -> Path:
    return LLM_REPORT_DIR / f"{strategy_id}-batch.html"


def _escape_html(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

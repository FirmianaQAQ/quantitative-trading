from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


TRUE_VALUES = {"1", "true", "yes", "on"}
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_ENV_FILES = (
    PROJECT_ROOT / ".env.local",
    PROJECT_ROOT / ".env.llm.local",
)
PROVIDER_DEFAULTS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-chat",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-5-mini",
    },
    "custom": {
        "base_url": None,
        "model": None,
    },
}


@dataclass(frozen=True)
class LLMAnalysisSettings:
    enabled: bool
    provider: str
    api_key: str | None
    base_url: str | None
    model: str | None
    timeout_seconds: int
    temperature: float


def _is_true(raw_value: str | None) -> bool:
    if raw_value is None:
        return False
    return raw_value.strip().lower() in TRUE_VALUES


def _load_local_env_files() -> None:
    for env_path in LOCAL_ENV_FILES:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ[key] = value


def _get_provider() -> str:
    provider = os.getenv("QT_LLM_PROVIDER", "deepseek").strip().lower()
    if provider not in PROVIDER_DEFAULTS:
        available = ", ".join(sorted(PROVIDER_DEFAULTS.keys()))
        raise ValueError(f"不支持的 QT_LLM_PROVIDER: {provider}，可选值: {available}")
    return provider


def is_llm_analysis_requested() -> bool:
    _load_local_env_files()
    return _is_true(os.getenv("QT_ENABLE_LLM_ANALYSIS"))


def load_llm_analysis_settings() -> LLMAnalysisSettings:
    _load_local_env_files()
    enabled = is_llm_analysis_requested()
    provider = _get_provider()
    provider_defaults = PROVIDER_DEFAULTS[provider]
    provider_env_prefix = f"QT_LLM_{provider.upper()}_"
    api_key = (
        os.getenv(f"{provider_env_prefix}API_KEY")
        or os.getenv("QT_LLM_API_KEY")
    )
    base_url = os.getenv("QT_LLM_BASE_URL") or provider_defaults["base_url"]
    model = os.getenv("QT_LLM_MODEL") or provider_defaults["model"]
    timeout_seconds = int(os.getenv("QT_LLM_TIMEOUT_SECONDS", "60"))
    temperature = float(os.getenv("QT_LLM_TEMPERATURE", "0.2"))

    if not enabled:
        return LLMAnalysisSettings(
            enabled=False,
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=timeout_seconds,
            temperature=temperature,
        )

    missing_fields: list[str] = []
    if not api_key:
        missing_fields.append(f"{provider_env_prefix}API_KEY 或 QT_LLM_API_KEY")
    if not base_url:
        missing_fields.append("QT_LLM_BASE_URL")
    if not model:
        missing_fields.append("QT_LLM_MODEL")
    if missing_fields:
        missing_text = ", ".join(missing_fields)
        raise ValueError(
            "已启用大模型分析，但缺少必要环境变量: "
            f"{missing_text}"
        )

    return LLMAnalysisSettings(
        enabled=True,
        provider=provider,
        api_key=api_key,
        base_url=base_url.rstrip("/"),
        model=model,
        timeout_seconds=timeout_seconds,
        temperature=temperature,
    )

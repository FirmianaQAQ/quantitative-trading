from __future__ import annotations

import ast
from importlib import import_module
from pathlib import Path
import re
from types import ModuleType
from typing import Any


PATCH_PACKAGE = "backtest.patches"
IGNORED_PATCH_MODULES = {"__init__", "loader"}
# 补丁模块约定以下可选 hook：
# - setup_patch(strategy, context)
# - before_next(strategy, context)
# - after_next(strategy, context)
# - allow_buy(strategy, context) -> None | bool | {"allow": bool, "reason": str}
# - allow_sell(strategy, context) -> None | bool | {"allow": bool, "reason": str}
SUPPORTED_HOOKS = (
    "setup_patch",
    "before_next",
    "after_next",
    "allow_buy",
    "allow_sell",
)


def normalize_requested_patch_names(value: Any) -> list[str]:
    if value is None:
        return []

    raw_names: list[str] = []
    if isinstance(value, str):
        raw_names.extend(
            segment.strip()
            for segment in re.split(r"[\s,，]+", value)
            if segment.strip()
        )
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            raw_names.extend(normalize_requested_patch_names(item))
    else:
        text = str(value).strip()
        if text:
            raw_names.append(text)

    normalized: list[str] = []
    seen: set[str] = set()
    for name in raw_names:
        normalized_name = name.strip().lower().removesuffix(".py")
        if not normalized_name or normalized_name in seen:
            continue
        seen.add(normalized_name)
        normalized.append(normalized_name)
    return normalized


def discover_available_patch_names() -> list[str]:
    patch_dir = Path(__file__).resolve().parent
    names: list[str] = []
    for path in sorted(patch_dir.glob("*.py")):
        if path.stem in IGNORED_PATCH_MODULES or path.stem.startswith("_"):
            continue
        names.append(path.stem)
    return names


def build_patch_analysis_context(config: dict[str, Any] | None) -> dict[str, Any]:
    resolved_config = config or {}
    requested_names = normalize_requested_patch_names(resolved_config.get("patches"))
    available_names = discover_available_patch_names()
    available_catalog = [
        describe_patch_module(name, enabled=name in requested_names)
        for name in available_names
    ]
    return {
        "requested_patches": requested_names,
        "patch_strict": bool(resolved_config.get("patch_strict", False)),
        "active_patch_count": len(requested_names),
        "available_patch_count": len(available_catalog),
        "active_patches": [
            item for item in available_catalog if item["name"] in requested_names
        ],
        "available_patches": available_catalog,
        "missing_requested_patches": [
            name for name in requested_names if name not in set(available_names)
        ],
    }


def describe_patch_module(name: str, *, enabled: bool = False) -> dict[str, Any]:
    patch_path = Path(__file__).resolve().parent / f"{name}.py"
    summary = None
    hooks: list[str] = []
    if patch_path.exists():
        source = patch_path.read_text(encoding="utf-8")
        summary = _extract_patch_summary(source)
        hooks = _extract_supported_hooks(source)
    return {
        "name": name,
        "enabled": bool(enabled),
        "summary": summary or f"{name} 补丁",
        "supported_hooks": hooks,
        "path": str(patch_path),
    }


def _extract_patch_summary(source: str) -> str | None:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        tree = None

    if tree is not None:
        docstring = ast.get_docstring(tree)
        if docstring:
            first_line = next(
                (line.strip() for line in docstring.splitlines() if line.strip()),
                "",
            )
            if first_line:
                return first_line

    comment_lines: list[str] = []
    for raw_line in source.splitlines():
        line = raw_line.strip()
        if not line and not comment_lines:
            continue
        if line.startswith("#"):
            comment_text = line.removeprefix("#").strip()
            if comment_text:
                comment_lines.append(comment_text)
            continue
        break
    if comment_lines:
        return " ".join(comment_lines)
    return None


def _extract_supported_hooks(source: str) -> list[str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    hooks: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in SUPPORTED_HOOKS:
                hooks.append(node.name)
    return hooks


class StrategyPatchManager:
    def __init__(
        self,
        strategy: Any,
        requested_names: Any = None,
        *,
        strict: bool = False,
    ) -> None:
        self.strategy = strategy
        self.strict = bool(strict)
        self.available_names = discover_available_patch_names()
        self.requested_names = normalize_requested_patch_names(requested_names)
        self._modules: list[tuple[str, ModuleType]] = []

        if not self.requested_names:
            return

        self._load_modules()
        self._run_lifecycle_hook("setup_patch")

    @property
    def active_patch_names(self) -> list[str]:
        return [name for name, _ in self._modules]

    def before_next(self, **extra: Any) -> None:
        self._run_lifecycle_hook("before_next", **extra)

    def after_next(self, **extra: Any) -> None:
        self._run_lifecycle_hook("after_next", **extra)

    def allow_buy(self, **extra: Any) -> tuple[bool, str | None]:
        return self._run_decision_hook("allow_buy", action="buy", **extra)

    def allow_sell(self, **extra: Any) -> tuple[bool, str | None]:
        return self._run_decision_hook("allow_sell", action="sell", **extra)

    def _load_modules(self) -> None:
        available_set = set(self.available_names)
        for name in self.requested_names:
            if name not in available_set:
                self._handle_problem(f"未找到补丁模块: {name}")
                continue

            try:
                module = import_module(f"{PATCH_PACKAGE}.{name}")
            except Exception as exc:
                self._handle_problem(f"补丁模块导入失败: {name} | {exc}")
                continue
            if not self._module_has_supported_hook(module):
                self._handle_problem(f"补丁模块未暴露可用 hook: {name}")
                continue

            self._modules.append((name, module))

        if self._modules:
            self._log(
                "补丁加载完成"
                f" | 已启用={','.join(self.active_patch_names)}"
            )

    def _module_has_supported_hook(self, module: ModuleType) -> bool:
        return any(
            callable(getattr(module, hook_name, None))
            for hook_name in SUPPORTED_HOOKS
        )

    def _build_context(self, **extra: Any) -> dict[str, Any]:
        current_date = None
        try:
            if getattr(self.strategy, "datas", None):
                current_date = self.strategy.datas[0].datetime.date(0)
        except Exception:
            current_date = None

        position = getattr(self.strategy, "position", None)
        has_position_method = getattr(self.strategy, "has_effective_position", None)
        has_position = (
            bool(has_position_method())
            if callable(has_position_method)
            else bool(position)
        )
        config = getattr(self.strategy, "param", {})
        context = {
            "config": config,
            "code": config.get("code"),
            "bar_index": len(self.strategy),
            "current_date": current_date,
            "has_position": has_position,
            "position_size": float(getattr(position, "size", 0.0) or 0.0),
            "active_patches": self.active_patch_names,
        }
        context.update(extra)
        return context

    def _run_lifecycle_hook(self, hook_name: str, **extra: Any) -> None:
        if not self._modules:
            return

        base_context = self._build_context(**extra)
        for name, module in self._modules:
            hook = getattr(module, hook_name, None)
            if not callable(hook):
                continue
            try:
                hook(self.strategy, {**base_context, "patch_name": name})
            except Exception as exc:
                self._handle_problem(f"补丁 {name}.{hook_name} 执行失败: {exc}")

    def _run_decision_hook(
        self,
        hook_name: str,
        *,
        action: str,
        **extra: Any,
    ) -> tuple[bool, str | None]:
        if not self._modules:
            return True, None

        base_context = self._build_context(action=action, **extra)
        for name, module in self._modules:
            hook = getattr(module, hook_name, None)
            if not callable(hook):
                continue
            try:
                result = hook(self.strategy, {**base_context, "patch_name": name})
            except Exception as exc:
                self._handle_problem(f"补丁 {name}.{hook_name} 执行失败: {exc}")
                continue

            allowed, reason = self._normalize_hook_result(
                result,
                action=action,
                patch_name=name,
            )
            if not allowed:
                return False, reason

        return True, None

    def _normalize_hook_result(
        self,
        result: Any,
        *,
        action: str,
        patch_name: str,
    ) -> tuple[bool, str | None]:
        if result is None:
            return True, None
        if isinstance(result, bool):
            reason = None if result else f"{patch_name} 拒绝 {action}"
            return result, reason
        if isinstance(result, dict):
            allow = bool(result.get("allow", True))
            reason = result.get("reason")
            if reason is not None:
                reason = str(reason).strip() or None
            if not allow and reason is None:
                reason = f"{patch_name} 拒绝 {action}"
            return allow, reason

        self._handle_problem(
            f"补丁 {patch_name} 返回了不支持的结果类型: {type(result).__name__}"
        )
        return True, None

    def _handle_problem(self, message: str) -> None:
        if self.strict:
            raise RuntimeError(message)
        self._log(f"补丁已跳过 | {message}")

    def _log(self, message: str) -> None:
        logger = getattr(self.strategy, "log", None)
        if callable(logger):
            logger(message)
            return
        print(message)

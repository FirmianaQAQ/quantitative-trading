from typing import Any

def get_nested_value(data: Any, path: list[str], default: Any = None) -> Any:
    """从嵌套的字典或对象中安全地获取值，避免 KeyError 或 AttributeError"""
    current = data
    for key in path:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(key, default)
            continue
        try:
            current = getattr(current, key)
        except (AttributeError, KeyError):
            return default
    return current

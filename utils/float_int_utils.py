from typing import Any
import math

def safe_round(value: Any, digits: int = 2) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return round(value, digits)
    return value

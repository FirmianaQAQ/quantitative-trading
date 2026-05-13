from pathlib import Path


def ensure_dir(dir: Path) -> Path:
    dir.mkdir(parents=True, exist_ok=True)
    return dir

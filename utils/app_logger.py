from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class AppLogger:
    def __init__(
        self,
        name: str,
        log_file_name: str,
        level: int = logging.INFO,
        max_bytes: int = 5 * 1024 * 1024,
        backup_count: int = 5,
    ) -> None:
        self._logger = logging.getLogger(name)
        if self._logger.handlers:
            return

        self._logger.setLevel(level)
        self._logger.propagate = False

        logs_dir = Path(__file__).resolve().parent.parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = logs_dir / log_file_name

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

    @property
    def logger(self) -> logging.Logger:
        return self._logger


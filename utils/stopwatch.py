from __future__ import annotations
import time


class Duration:
    def __init__(self, seconds: float):
        self.seconds = seconds

    def __str__(self):
        if self.seconds < 1:
            return f"{self.seconds * 1000:.2f} ms"
        elif self.seconds < 120:
            return f"{self.seconds:.2f} s"
        elif self.seconds < 3600:
            return f"{self.seconds / 60:.2f} min"
        else:
            return f"{self.seconds / 3600:.2f} h"


class Stopwatch:
    instances = {}

    def __init__(self):
        self.records = []
        self.duration = None

    def mark(self=None, name: str = None):
        now = time.perf_counter()
        if not isinstance(self, Stopwatch):
            # 把函数当静态方法用，自动创建一个 Stopwatch 实例来记录
            name = self or name or "default"
            if name in Stopwatch.instances:
                self = Stopwatch.instances[name]
            else:
                self = Stopwatch()
                Stopwatch.instances[name] = self
        self.duration = Duration(now - (self.records[-1] if self.records else now))
        self.records.append(now)
        return self

    def duration_from_start(self):
        if not isinstance(self, Stopwatch):
            # 把函数当静态方法用，自动创建一个 Stopwatch 实例来记录
            name = self or name or "default"
            if name in Stopwatch.instances:
                self = Stopwatch.instances[name]
            else:
                return Duration(0)
        return Duration(
            (self.records[-1] if len(self.records) > 1 else time.perf_counter())
            - self.records[0]
        )

    def __str__(self):
        return str(self.duration)

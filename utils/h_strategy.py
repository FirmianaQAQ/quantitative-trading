import backtrader as bt


"""
我给 backtrader 封装的策略基类

主要是为了添加一个 log 方法，方便在策略中打印日志。
"""


class HStrategy(bt.Strategy):
    def __init__(self, allow_log: bool = True) -> None:
        self.log_messages: list[str] = []
        self.allow_log = allow_log

    def log(self, text: str) -> None:
        dt = self.datas[0].datetime.date(0)
        message = f"{dt.isoformat()} {text}"
        self.log_messages.append(message)
        if self.allow_log:
            print(message)

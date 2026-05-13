from __future__ import annotations

from typing import Any

import backtrader as bt


DEFAULT_BROKER_COMMISSION = 0.0001
DEFAULT_STAMP_DUTY = 0.0005
DEFAULT_TRANSFER_FEE = 0.00001
DEFAULT_MIN_COMMISSION = 5.0


def get_a_share_cost_config(config: dict[str, Any]) -> dict[str, float]:
    return {
        "commission": float(config.get("commission", DEFAULT_BROKER_COMMISSION)),
        "stamp_duty": float(config.get("stamp_duty", DEFAULT_STAMP_DUTY)),
        "transfer_fee": float(config.get("transfer_fee", DEFAULT_TRANSFER_FEE)),
        "min_commission": float(
            config.get("min_commission", DEFAULT_MIN_COMMISSION)
        ),
    }


def validate_a_share_cost_config(config: dict[str, Any]) -> None:
    cost_config = get_a_share_cost_config(config)
    if cost_config["commission"] < 0:
        raise ValueError("佣金 commission 不能小于 0")
    if cost_config["stamp_duty"] < 0:
        raise ValueError("印花税 stamp_duty 不能小于 0")
    if cost_config["transfer_fee"] < 0:
        raise ValueError("过户费 transfer_fee 不能小于 0")
    if cost_config["min_commission"] < 0:
        raise ValueError("最低佣金 min_commission 不能小于 0")


def estimate_trade_fee(
    size: int,
    price: float,
    *,
    is_sell: bool,
    config: dict[str, Any],
) -> float:
    turnover = abs(size) * float(price)
    if turnover <= 0:
        return 0.0

    cost_config = get_a_share_cost_config(config)
    broker_commission = max(
        turnover * cost_config["commission"],
        cost_config["min_commission"],
    )
    transfer_fee = turnover * cost_config["transfer_fee"]
    stamp_duty = turnover * cost_config["stamp_duty"] if is_sell else 0.0
    return broker_commission + transfer_fee + stamp_duty


def estimate_max_buy_size(
    *,
    available_cash: float,
    price: float,
    lot_size: int,
    cash_usage_ratio: float,
    config: dict[str, Any],
) -> int:
    budget = float(available_cash) * float(cash_usage_ratio)
    if budget <= 0 or price <= 0:
        return 0

    normalized_lot_size = max(int(lot_size), 1)
    raw_size = int(budget / float(price))
    raw_size = (raw_size // normalized_lot_size) * normalized_lot_size

    while raw_size > 0:
        total_cost = raw_size * float(price) + estimate_trade_fee(
            raw_size,
            price,
            is_sell=False,
            config=config,
        )
        if total_cost <= budget + 1e-8:
            return raw_size
        raw_size -= normalized_lot_size

    return 0


class AShareStockCommissionInfo(bt.CommInfoBase):
    params = (
        ("commission", DEFAULT_BROKER_COMMISSION),
        ("stamp_duty", DEFAULT_STAMP_DUTY),
        ("transfer_fee", DEFAULT_TRANSFER_FEE),
        ("min_commission", DEFAULT_MIN_COMMISSION),
        ("stocklike", True),
        ("commtype", bt.CommInfoBase.COMM_PERC),
        ("percabs", True),
    )

    def _getcommission(
        self,
        size: float,
        price: float,
        pseudoexec: bool,
    ) -> float:
        del pseudoexec
        turnover = abs(size) * float(price)
        if turnover <= 0:
            return 0.0

        broker_commission = max(
            turnover * float(self.p.commission),
            float(self.p.min_commission),
        )
        transfer_fee = turnover * float(self.p.transfer_fee)
        stamp_duty = turnover * float(self.p.stamp_duty) if size < 0 else 0.0
        return broker_commission + transfer_fee + stamp_duty

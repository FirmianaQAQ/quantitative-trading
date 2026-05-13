from __future__ import annotations


class StockAccount:
    """用于单只股票回测的账户模型。"""

    def __init__(
        self,
        initial_cash: float = 0.0,
        stock_code: str | None = None,
        allow_negative_cash: bool = False,
    ):
        if initial_cash < 0:
            raise ValueError("initial_cash must be greater than or equal to 0")

        self.stock_code = stock_code
        self.allow_negative_cash = allow_negative_cash
        self.initial_cash = float(initial_cash)
        self._cash = float(initial_cash)
        self._shares = 0.0
        self._cost_basis = 0.0

    def buy(self, price: float, shares: float, fee: float = 0.0) -> None:
        """买入股票，并更新现金、持仓和持仓成本。"""
        normalized_price = self._validate_price(price)
        normalized_shares = self._validate_shares(shares)
        normalized_fee = self._validate_fee(fee)

        total_cost = normalized_price * normalized_shares + normalized_fee
        if not self.allow_negative_cash and total_cost > self._cash:
            raise ValueError("insufficient cash for this buy order")

        self._cash -= total_cost
        self._shares += normalized_shares
        self._cost_basis += total_cost

    def sell(self, price: float, shares: float, fee: float = 0.0) -> None:
        """卖出股票，并按当前平均成本结转持仓成本。"""
        normalized_price = self._validate_price(price)
        normalized_shares = self._validate_shares(shares)
        normalized_fee = self._validate_fee(fee)

        if normalized_shares > self._shares:
            raise ValueError("insufficient shares for this sell order")

        average_cost = self.get_average_cost()
        sale_amount = normalized_price * normalized_shares - normalized_fee

        self._cash += sale_amount
        self._shares -= normalized_shares
        self._cost_basis -= average_cost * normalized_shares

        if self._shares == 0:
            self._cost_basis = 0.0

    def get_cash(self) -> float:
        return self._cash

    def get_shares(self) -> float:
        return self._shares

    def get_average_cost(self) -> float:
        if self._shares == 0:
            return 0.0
        return self._cost_basis / self._shares

    def get_cost_basis(self) -> float:
        return self._cost_basis

    def get_market_value(self, price: float) -> float:
        normalized_price = self._validate_price(price)
        return self._shares * normalized_price

    def get_total_asset(self, price: float) -> float:
        return self._cash + self.get_market_value(price)

    @staticmethod
    def _validate_price(price: float) -> float:
        normalized_price = float(price)
        if normalized_price <= 0:
            raise ValueError("price must be greater than 0")
        return normalized_price

    @staticmethod
    def _validate_shares(shares: float) -> float:
        normalized_shares = float(shares)
        if normalized_shares <= 0:
            raise ValueError("shares must be greater than 0")
        return normalized_shares

    @staticmethod
    def _validate_fee(fee: float) -> float:
        normalized_fee = float(fee)
        if normalized_fee < 0:
            raise ValueError("fee must be greater than or equal to 0")
        return normalized_fee

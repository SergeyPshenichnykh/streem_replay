from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MyOrdersAtPrice:
    my_lay: float = 0.0
    my_back: float = 0.0
    matched: float = 0.0


@dataclass
class OrderModel:
    """
    Placeholder for future maker-order state.

    Keyed by (market_id, selection_id, handicap, price).
    Values are the user's own orders (my_lay/my_back) and matched amount.
    """

    by_key: dict[tuple[str, int, float | None, float], MyOrdersAtPrice] = field(default_factory=dict)

    def get(
        self,
        *,
        market_id: str,
        selection_id: int,
        handicap: float | None,
        price: float,
    ) -> MyOrdersAtPrice:
        return self.by_key.get((market_id, selection_id, handicap, float(price)), MyOrdersAtPrice())


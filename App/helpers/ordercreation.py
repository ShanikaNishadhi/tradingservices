from typing import Optional

from binance.client import Client

from .futureorder import create_market_order


def create_order_if_trend_up(
    last_order_price: float,
    current_price: float,
    order_creation_threshold: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "LONG",
    **order_kwargs,
) -> Optional[dict]:
    """Create a market long order when price increases past the threshold."""
    if current_price <= last_order_price + order_creation_threshold:
        return None

    return create_market_order(
        client=client,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        position_side=position_side,
        **order_kwargs,
    )


def create_short_order_if_trend_down(
    last_order_price: float,
    current_price: float,
    order_creation_threshold: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "SHORT",
    **order_kwargs,
) -> Optional[dict]:
    """Create a market short order when price drops past the threshold."""
    if current_price >= last_order_price - order_creation_threshold:
        return None

    return create_market_order(
        client=client,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        position_side=position_side,
        **order_kwargs,
    )

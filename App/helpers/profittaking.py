from typing import Optional

from binance.client import Client

from .futureorder import create_market_order, create_trailing_stop_order


def close_long_order_if_price_drops(
    order_creation_price: float,
    current_price: float,
    close_threshold: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "LONG",
    **order_kwargs,
) -> Optional[dict]:
    """Close a long futures position when price falls past the threshold."""
    if current_price >= order_creation_price - close_threshold:
        return None

    return create_market_order(
        client=client,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        position_side=position_side,
        **order_kwargs,
    )


def close_short_order_if_price_rises(
    order_creation_price: float,
    current_price: float,
    close_threshold: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "SHORT",
    **order_kwargs,
) -> Optional[dict]:
    """Close a short futures position when price rises past the threshold."""
    if current_price <= order_creation_price + close_threshold:
        return None

    return create_market_order(
        client=client,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        position_side=position_side,
        **order_kwargs,
    )


def take_profit_on_long(
    order_creation_price: float,
    current_price: float,
    profit_threshold: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "LONG",
    **order_kwargs,
) -> Optional[dict]:
    """Close a long position for profit once price exceeds the threshold."""
    if current_price <= order_creation_price + profit_threshold:
        return None

    return create_market_order(
        client=client,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        position_side=position_side,
        **order_kwargs,
    )


def take_profit_on_short(
    order_creation_price: float,
    current_price: float,
    profit_threshold: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "SHORT",
    **order_kwargs,
) -> Optional[dict]:
    """Close a short position for profit once price falls past the threshold."""
    if current_price >= order_creation_price - profit_threshold:
        return None

    return create_market_order(
        client=client,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        position_side=position_side,
        **order_kwargs,
    )


def set_trailing_stop_for_long(
    order_creation_price: float,
    profit_threshold: float,
    drawdown_percentage: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "LONG",
    **order_kwargs,
) -> dict:
    """
    Place a trailing stop for a long position with specified activation price and drawdown.
    activationPrice = order_creation_price + profit_threshold
    callbackRate = drawdown_percentage
    """
    activation_price = order_creation_price + profit_threshold

    return create_trailing_stop_order(
        client=client,
        symbol=symbol,
        side="SELL",
        quantity=quantity,
        callback_rate=drawdown_percentage,
        activation_price=activation_price,
        position_side=position_side,
        **order_kwargs,
    )


def set_trailing_stop_for_short(
    order_creation_price: float,
    profit_threshold: float,
    drawdown_percentage: float,
    *,
    client: Client,
    symbol: str,
    quantity: float,
    position_side: str = "SHORT",
    **order_kwargs,
) -> dict:
    """
    Place a trailing stop for a short position with specified activation price and drawdown.
    activationPrice = order_creation_price - profit_threshold
    callbackRate = drawdown_percentage
    """
    activation_price = order_creation_price - profit_threshold

    return create_trailing_stop_order(
        client=client,
        symbol=symbol,
        side="BUY",
        quantity=quantity,
        callback_rate=drawdown_percentage,
        activation_price=activation_price,
        position_side=position_side,
        **order_kwargs,
    )

from binance.client import Client
from typing import Optional


def create_market_order(client: Client, symbol: str, side: str, quantity: float, position_side: str, **kwargs):
    """
    Create a futures market order
    """
    return client.futures_create_order(
        symbol=symbol,
        side=side,
        type='MARKET',
        quantity=quantity,
        positionSide=position_side,
        **kwargs  # Optional: timeInForce (default: None), reduceOnly (default: False),
                  # newClientOrderId (default: auto-generated), stopPrice, activationPrice,
                  # callbackRate, workingType, priceProtect, etc.
    )


def create_stop_market_order(client: Client, symbol: str, side: str, quantity: float, stop_price: float, position_side: str, **kwargs):
    """
    Create a futures stop market order
    """
    return client.futures_create_order(
        symbol=symbol,
        side=side,
        type='STOP_MARKET',
        quantity=quantity,
        stopPrice=stop_price,
        positionSide=position_side,
        **kwargs  # Optional: timeInForce (default: GTC), reduceOnly (default: False),
                  # newClientOrderId (default: auto-generated), workingType (default: CONTRACT_PRICE),
                  # priceProtect (default: False), closePosition, etc.
    )


def create_limit_order(client: Client, symbol: str, side: str, quantity: float, price: float, position_side: str, **kwargs):
    """
    Create a futures limit order
    """
    return client.futures_create_order(
        symbol=symbol,
        side=side,
        type='LIMIT',
        quantity=quantity,
        price=price,
        positionSide=position_side,
        **kwargs  # Optional: timeInForce (default: GTC), reduceOnly (default: False),
                  # newClientOrderId (default: auto-generated), postOnly, priceProtect, etc.
    )


def create_stop_limit_order(client: Client, symbol: str, side: str, quantity: float, price: float, stop_price: float, position_side: str, **kwargs):
    """
    Create a futures stop limit order
    """
    return client.futures_create_order(
        symbol=symbol,
        side=side,
        type='STOP',
        quantity=quantity,
        price=price,
        stopPrice=stop_price,
        positionSide=position_side,
        **kwargs  # Optional: timeInForce (default: GTC), reduceOnly (default: False),
                  # newClientOrderId (default: auto-generated), workingType (default: CONTRACT_PRICE),
                  # priceProtect (default: False), etc.
    )


def create_trailing_market_order(client: Client, symbol: str, side: str, quantity: float, callback_rate: float, position_side: str, **kwargs):
    """
    Create a futures trailing market order
    """
    return client.futures_create_order(
        symbol=symbol,
        side=side,
        type='TRAILING_STOP_MARKET',
        quantity=quantity,
        callbackRate=callback_rate,
        positionSide=position_side,
        **kwargs  # Optional: reduceOnly (default: False), newClientOrderId (default: auto-generated),
                  # workingType (default: CONTRACT_PRICE), priceProtect (default: False), etc.
    )


def create_trailing_stop_order(client: Client, symbol: str, side: str, quantity: float, callback_rate: float, activation_price: float, position_side: str, **kwargs):
    """
    Create a futures trailing stop order
    """
    return client.futures_create_order(
        symbol=symbol,
        side=side,
        type='TRAILING_STOP_MARKET',
        quantity=quantity,
        callbackRate=callback_rate,
        activationPrice=activation_price,
        positionSide=position_side,
        **kwargs  # Optional: reduceOnly (default: False), newClientOrderId (default: auto-generated),
                  # workingType (default: CONTRACT_PRICE), priceProtect (default: False), etc.
    )


def cancel_order(client: Client, symbol: str, order_id: int = None, orig_client_order_id: str = None):
    """
    Cancel a futures order
    Provide either order_id or orig_client_order_id not both
    """
    return client.futures_cancel_order(
        symbol=symbol,
        orderId=order_id,
        origClientOrderId=orig_client_order_id
    )


def cancel_all_orders(client: Client, symbol: str, position_side: str = 'BOTH'):
    """
    Cancel all open orders for a symbol
    position_side: 'LONG', 'SHORT', or 'BOTH' (default: cancels all sides)
    """
    open_orders = client.futures_get_open_orders(symbol=symbol)

    if position_side == 'BOTH':
        # Cancel all orders regardless of position side
        return [client.futures_cancel_order(symbol=symbol, orderId=order['orderId']) for order in open_orders]
    else:
        # Cancel only orders for the specified position side
        filtered_orders = [order for order in open_orders if order.get('positionSide') == position_side]
        return [client.futures_cancel_order(symbol=symbol, orderId=order['orderId']) for order in filtered_orders]


def close_all_positions(client: Client, symbol: str, position_side: str = 'BOTH'):
    """
    Close all open positions for a symbol at market price
    position_side: 'LONG', 'SHORT', or 'BOTH' (default: closes all sides)
    """
    positions = client.futures_position_information(symbol=symbol)
    results = []

    for position in positions:
        position_amt = float(position['positionAmt'])
        pos_side = position['positionSide']

        # Skip if no position
        if position_amt == 0:
            continue

        # Skip if not the specified side
        if position_side != 'BOTH' and pos_side != position_side:
            continue

        # Determine order side to close position
        if position_amt > 0:
            # Close LONG position with SELL
            side = 'SELL'
        else:
            # Close SHORT position with BUY
            side = 'BUY'

        # Close position at market price
        order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=abs(position_amt),
            positionSide=pos_side
        )
        results.append(order)

    return results


def get_position_info(client: Client, symbol: str):
    """
    Get position information for a specific symbol
    Returns: positionAmt, entryPrice, unRealizedProfit, leverage, marginType,
             isolatedMargin, positionSide, liquidationPrice, markPrice, etc.
    """
    return client.futures_position_information(symbol=symbol)


def get_price(client: Client, symbol: str) -> float:
    """
    Get current price for a specific symbol from futures market
    """
    ticker = client.futures_symbol_ticker(symbol=symbol)
    return float(ticker['price'])


def get_all_prices(client: Client, symbols: Optional[list] = None) -> dict[str, float]:
    """
    Get current prices for multiple symbols from futures market
    """
    all_tickers = client.futures_symbol_ticker()

    if symbols is None:
        # Return all symbols
        return {ticker['symbol']: float(ticker['price']) for ticker in all_tickers}
    else:
        # Return only requested symbols
        ticker_dict = {ticker['symbol']: float(ticker['price']) for ticker in all_tickers}
        return {symbol: ticker_dict.get(symbol) for symbol in symbols if symbol in ticker_dict}


def set_leverage(client: Client, symbol: str, leverage: int):
    """
    Set leverage for a symbol
    """
    return client.futures_change_leverage(symbol=symbol, leverage=leverage)

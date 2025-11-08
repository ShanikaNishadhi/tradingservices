import asyncio
import logging
from datetime import datetime
from binance.client import Client
from App.simpletrendsmemecoins.database import SimpleTrendsDatabase

logger = logging.getLogger(__name__)


async def monitor_all_discrepancies(client: Client, strategies: dict, db: SimpleTrendsDatabase, running_flag: list):
    """Monitor for discrepancies across all symbols every 5 minutes"""
    while running_flag[0]:
        try:
            await asyncio.sleep(300)  # 5 minutes

            # Get all open orders from Binance in one API call
            binance_orders = client.futures_get_open_orders()

            # Get all positions from Binance
            binance_positions = client.futures_position_information()

            # Group Binance orders by symbol
            binance_by_symbol = {}
            for order in binance_orders:
                symbol = order['symbol']
                if symbol not in binance_by_symbol:
                    binance_by_symbol[symbol] = {'all': [], 'stop_loss': [], 'trailing': [], 'positions': []}

                binance_by_symbol[symbol]['all'].append(order)
                if order['type'] == 'STOP_MARKET':
                    binance_by_symbol[symbol]['stop_loss'].append(order)
                elif order['type'] == 'TRAILING_STOP_MARKET':
                    binance_by_symbol[symbol]['trailing'].append(order)

            # Group positions by symbol (only non-zero positions)
            for position in binance_positions:
                symbol = position['symbol']
                position_amt = float(position['positionAmt'])
                if position_amt != 0:
                    if symbol not in binance_by_symbol:
                        binance_by_symbol[symbol] = {'all': [], 'stop_loss': [], 'trailing': [], 'positions': []}
                    binance_by_symbol[symbol]['positions'].append(position)

            # Check each symbol
            for symbol, strategy in strategies.items():
                binance_symbol_data = binance_by_symbol.get(symbol, {'all': [], 'stop_loss': [], 'trailing': [], 'positions': []})
                db_orders = db.get_open_orders(symbol)

                cache_count_long = len(strategy.open_orders_cache['LONG'])
                cache_count_short = len(strategy.open_orders_cache['SHORT'])
                cache_count_total = cache_count_long + cache_count_short
                db_count = len(db_orders)

                binance_all_order_ids = {str(o['orderId']) for o in binance_symbol_data['all']}
                binance_positions = binance_symbol_data['positions']
                binance_stop_loss = binance_symbol_data['stop_loss']
                binance_trailing_stop = binance_symbol_data['trailing']

                # Check 1: Compare position sizes (quantities) per side
                # Calculate DB position sizes per side
                db_long_qty = sum(float(o['quantity']) for o in db_orders if o['side'] == 'LONG')
                db_short_qty = sum(float(o['quantity']) for o in db_orders if o['side'] == 'SHORT')

                # Get Binance position sizes per side
                binance_long_qty = 0
                binance_short_qty = 0
                for pos in binance_positions:
                    pos_amt = float(pos['positionAmt'])
                    pos_side = pos['positionSide']
                    if pos_side == 'LONG':
                        binance_long_qty = pos_amt
                    elif pos_side == 'SHORT':
                        binance_short_qty = abs(pos_amt)  # SHORT positions are negative

                # Compare position sizes
                if abs(db_long_qty - binance_long_qty) > 0.001:  # Allow small floating point difference
                    logger.error(f"{symbol}: DISCREPANCY - DB LONG position: {db_long_qty}, Binance LONG position: {binance_long_qty}")
                if abs(db_short_qty - binance_short_qty) > 0.001:
                    logger.error(f"{symbol}: DISCREPANCY - DB SHORT position: {db_short_qty}, Binance SHORT position: {binance_short_qty}")

                # Check 3: Cache count vs Database count
                if cache_count_total != db_count:
                    logger.error(f"{symbol}: DISCREPANCY - Cache has {cache_count_total} orders (LONG:{cache_count_long}, SHORT:{cache_count_short}) but Database has {db_count}")

                # Check 4: Stop loss and trailing stop counts should match
                stop_loss_count = len(binance_stop_loss)
                trailing_stop_count = len(binance_trailing_stop)
                if stop_loss_count != trailing_stop_count:
                    logger.error(f"{symbol}: DISCREPANCY - Stop loss orders: {stop_loss_count}, Trailing stop orders: {trailing_stop_count} (should be equal)")

                # Check 5: Stop loss/trailing stop IDs in DB exist on Binance
                for db_order in db_orders:
                    stop_loss_id = db_order.get('stop_loss_order_id')
                    trailing_stop_id = db_order.get('trailing_stop_order_id')

                    if stop_loss_id and stop_loss_id not in binance_all_order_ids:
                        logger.error(f"{symbol}: DISCREPANCY - Stop loss order {stop_loss_id} for position {db_order['order_id']} not found on Binance")

                    if trailing_stop_id and trailing_stop_id not in binance_all_order_ids:
                        logger.error(f"{symbol}: DISCREPANCY - Trailing stop order {trailing_stop_id} for position {db_order['order_id']} not found on Binance")

        except Exception as e:
            logger.error(f"Error in discrepancy monitoring: {e}")

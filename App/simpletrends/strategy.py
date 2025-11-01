import asyncio
import logging
import redis
from decimal import Decimal
from typing import Dict
from datetime import datetime
from binance.client import Client

from App.helpers.futureorder import (
    set_leverage,
    create_market_order,
    create_stop_market_order,
    create_trailing_stop_order,
    cancel_order,
    get_price
)
from App.simpletrends.database import SimpleTrendsDatabase

logger = logging.getLogger(__name__)


class SimpleTrendsStrategy:
    """Simple Trends strategy - tracks min/max and creates orders with trailing stops and stop losses"""

    def __init__(self, client: Client, symbol_config: Dict, db: SimpleTrendsDatabase,
                 redis_client: redis.Redis):
        self.client = client
        self.symbol = symbol_config['symbol']
        self.config = symbol_config
        self.db = db
        self.redis_client = redis_client

        # Config parameters
        self.position_size = Decimal(str(symbol_config['position_size']))
        self.order_threshold_percent = Decimal(str(symbol_config['order_threshold_percent']))
        self.profit_threshold_percent = Decimal(str(symbol_config['profit_threshold_percent']))
        self.trailing_stop_callback_rate = Decimal(str(symbol_config['trailing_stop_callback_rate']))
        self.stop_loss_percent = Decimal(str(symbol_config['stop_loss_percent'])) if symbol_config['stop_loss_percent'] is not None else None
        self.leverage = symbol_config['leverage']
        self.price_precision = symbol_config['price_precision']
        self.quantity_precision = symbol_config['quantity_precision']
        self.long_enabled = symbol_config.get('long_enabled', True)
        self.short_enabled = symbol_config.get('short_enabled', True)
        self.long_order_limit = symbol_config.get('long_order_limit', 999)
        self.short_order_limit = symbol_config.get('short_order_limit', 999)
        self.long_margin_limit = symbol_config.get('long_margin_limit')  # None means no limit
        self.short_margin_limit = symbol_config.get('short_margin_limit')  # None means no limit
        self.forward_order_block_percent = Decimal(str(symbol_config.get('forward_order_block_percent', 0)))
        self.backward_order_block_percent = Decimal(str(symbol_config.get('backward_order_block_percent', 0)))

        # Trading state
        self.start_price = None
        self.min_price = None
        self.max_price = None
        self.order_threshold_price = None
        self.profit_threshold_price = None
        self.stop_loss_price = None
        self.forward_order_block_price = None
        self.backward_order_block_price = None
        self.running = False

        # Track pending market orders awaiting fill from User Data Stream
        self.pending_market_orders = {}  # {order_id: {'side': 'LONG', 'created_at': datetime}}

        # Cache open orders to avoid DB calls on every tick
        self.open_orders_cache = {'LONG': [], 'SHORT': []}

        # Track position entry prices (breakeven) from ACCOUNT_UPDATE events
        self.position_entry_price = {'LONG': None, 'SHORT': None}

        # Track last time state was saved to database
        self.last_state_save = datetime.now()

        # Set leverage
        self._set_leverage()

    def _set_leverage(self):
        """Set leverage for symbol"""
        try:
            set_leverage(self.client, self.symbol, self.leverage)
        except Exception as e:
            logger.error(f"{self.symbol}: Error setting leverage: {e}")

    def _format_quantity(self, quantity: Decimal) -> float:
        """Format quantity to correct precision"""
        return float(round(quantity, self.quantity_precision))

    def _format_price(self, price: Decimal) -> float:
        """Format price to correct precision"""
        return float(round(price, self.price_precision))

    def _check_order_block(self, side: str, current_price: Decimal) -> bool:
        """Check if new order is too close to existing orders of same side"""
        if self.forward_order_block_price == 0 and self.backward_order_block_price == 0:
            return True

        # Use cached orders instead of database query
        same_side_orders = self.open_orders_cache[side]

        if not same_side_orders:
            return True

        prices = [Decimal(str(o['entry_price'])) for o in same_side_orders]

        # Find closest order above (forward) and below (backward)
        forward_orders = [p for p in prices if p > current_price]
        backward_orders = [p for p in prices if p < current_price]

        # Check forward block
        if self.forward_order_block_price > 0 and forward_orders:
            closest_forward = min(forward_orders)
            if closest_forward - current_price < self.forward_order_block_price:
                return False

        # Check backward block
        if self.backward_order_block_price > 0 and backward_orders:
            closest_backward = max(backward_orders)
            if current_price - closest_backward < self.backward_order_block_price:
                return False

        return True

    def initialize(self):
        """Initialize strategy state"""

        # Try to load saved state from database
        saved_state = self.db.get_state(self.symbol)

        # Get current price as start price
        current_price = self._get_current_price()
        if current_price <= 0:
            logger.error(f"{self.symbol}: Could not get current price")
            return

        self.start_price = current_price

        # Use saved min/max if available, otherwise use current price
        if saved_state:
            self.min_price = Decimal(str(saved_state['min_price']))
            self.max_price = Decimal(str(saved_state['max_price']))
            logger.info(f"{self.symbol}: Loaded saved state - min={self.min_price}, max={self.max_price}")
        else:
            self.min_price = current_price
            self.max_price = current_price
            logger.info(f"{self.symbol}: No saved state, using current price - {current_price}")

        # Calculate threshold values based on start price
        self._calculate_thresholds()

        # Load open orders from database into cache
        self._load_orders_cache()

        stop_loss_str = f"${self.stop_loss_price:.8f} ({self.stop_loss_percent}%)" if self.stop_loss_price is not None else "DISABLED"
        logger.warning(f"{self.symbol}: INITIALIZED - start_price={self.start_price}, "
                      f"order_threshold=${self.order_threshold_price:.8f} ({self.order_threshold_percent}%), "
                      f"profit_threshold=${self.profit_threshold_price:.8f} ({self.profit_threshold_percent}%), "
                      f"stop_loss={stop_loss_str}, "
                      f"forward_block=${self.forward_order_block_price:.8f} ({self.forward_order_block_percent}%), "
                      f"backward_block=${self.backward_order_block_price:.8f} ({self.backward_order_block_percent}%), "
                      f"trailing_callback={self.trailing_stop_callback_rate}%, "
                      f"leverage={self.leverage}x")

    def _calculate_thresholds(self):
        """Calculate threshold values as price movements (not position values)"""
        # Order threshold: price movement to trigger new orders
        self.order_threshold_price = self.start_price * (self.order_threshold_percent / Decimal('100'))

        # Profit threshold: price movement to activate trailing stop
        self.profit_threshold_price = self.start_price * (self.profit_threshold_percent / Decimal('100'))

        # Stop loss: price movement for stop loss (None if disabled)
        self.stop_loss_price = self.start_price * (self.stop_loss_percent / Decimal('100')) if self.stop_loss_percent is not None else None

        # Order block: prevent orders too close to existing orders
        self.forward_order_block_price = self.start_price * (self.forward_order_block_percent / Decimal('100'))
        self.backward_order_block_price = self.start_price * (self.backward_order_block_percent / Decimal('100'))

    def _load_orders_cache(self):
        """Load open orders from database into cache on startup"""
        open_orders = self.db.get_open_orders(self.symbol)

        # Clear cache first
        self.open_orders_cache = {'LONG': [], 'SHORT': []}

        # Populate cache
        for order in open_orders:
            side = order['side']
            self.open_orders_cache[side].append(order)

        logger.info(f"{self.symbol}: Loaded {len(open_orders)} open orders into cache "
                   f"(LONG: {len(self.open_orders_cache['LONG'])}, SHORT: {len(self.open_orders_cache['SHORT'])})")

    async def run(self):
        """Main strategy loop - polls Redis for price every 1 second"""
        self.running = True
        logger.info(f"{self.symbol}: Starting strategy loop (polling every 1 second)")

        while self.running:
            try:
                await self.check_and_execute()

                # Save state to database every 5 minutes
                now = datetime.now()
                if (now - self.last_state_save).total_seconds() >= 300:  # 5 minutes
                    self.db.save_state(self.symbol, self.min_price, self.max_price)
                    self.last_state_save = now
                    logger.info(f"{self.symbol}: Saved state - min={self.min_price}, max={self.max_price}")

                await asyncio.sleep(1)  # Poll every 1 second
            except Exception as e:
                logger.error(f"{self.symbol}: Error in strategy loop: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """Stop strategy loop"""
        self.running = False

        # Save state one final time before stopping
        if self.min_price and self.max_price:
            self.db.save_state(self.symbol, self.min_price, self.max_price)
            logger.info(f"{self.symbol}: Final state saved - min={self.min_price}, max={self.max_price}")

        logger.info(f"{self.symbol}: Strategy stopped")

    async def check_and_execute(self):
        """Main strategy execution - called every 1 second"""
        try:
            # Get current mark price from Redis
            mark_price = self._get_mark_price()
            if mark_price == 0:
                return

            # Update min/max prices
            self._update_min_max(mark_price)

            # Check for new entry signals
            await self._check_entry_signals(mark_price)

        except Exception as e:
            logger.error(f"{self.symbol}: Error in strategy execution: {e}")

    def _get_mark_price(self) -> Decimal:
        """Get current mark price for symbol from Redis"""
        try:
            key = f"mark_price:{self.symbol}"
            mark_price_str = self.redis_client.get(key)

            if mark_price_str:
                return Decimal(mark_price_str)

            return Decimal('0')
        except Exception as e:
            logger.error(f"{self.symbol}: Error getting mark price from Redis: {e}")
            return Decimal('0')

    def _get_last_trade_price(self) -> Decimal:
        """Get current last trade price for symbol from Redis"""
        try:
            key = f"last_trade_price:{self.symbol}"
            trade_price_str = self.redis_client.get(key)

            if trade_price_str:
                return Decimal(trade_price_str)

            return Decimal('0')
        except Exception as e:
            logger.error(f"{self.symbol}: Error getting last trade price from Redis: {e}")
            return Decimal('0')

    def _update_min_max(self, mark_price: Decimal):
        """Update min and max prices based on current market price"""
        if mark_price < self.min_price:
            self.min_price = mark_price

        if mark_price > self.max_price:
            self.max_price = mark_price

    async def _check_entry_signals(self, mark_price: Decimal):
        """Check for LONG or SHORT entry signals"""
        if not all([self.min_price, self.max_price, self.order_threshold_price]):
            return

        # Get last trade price for order blocking (where order will actually execute)
        last_trade_price = self._get_last_trade_price()
        if last_trade_price == Decimal('0'):
            last_trade_price = mark_price  # Fallback to mark price if trade price unavailable

        # LONG signal: price > min_price + order_threshold
        if self.long_enabled:
            long_trigger = self.min_price + self.order_threshold_price
            if mark_price >= long_trigger:
                # Update min price first (always update even if we don't create order)
                old_min = self.min_price
                self.min_price = mark_price

                # Check margin limit FIRST (if price above margin limit, don't create LONG)
                if self.long_margin_limit is not None and mark_price > Decimal(str(self.long_margin_limit)):
                    pass 
                # Check if order limit reached for LONG
                elif len(self.open_orders_cache['LONG']) >= self.long_order_limit:
                    pass
                # Check order block using last trade price (actual execution price)
                elif self._check_order_block('LONG', last_trade_price):
                    logger.warning(f"{self.symbol}: LONG SIGNAL - mark_price={mark_price}, last_trade={last_trade_price}, "
                                  f"trigger={long_trigger:.8f}, old_min={old_min}, new_min={self.min_price}")
                    await self._open_position('LONG', mark_price)

        # SHORT signal: price < max_price - order_threshold
        if self.short_enabled:
            short_trigger = self.max_price - self.order_threshold_price
            if mark_price <= short_trigger:
                # Update max price first (always update even if we don't create order)
                old_max = self.max_price
                self.max_price = mark_price

                # Check margin limit FIRST (if price below margin limit, don't create SHORT)
                if self.short_margin_limit is not None and mark_price < Decimal(str(self.short_margin_limit)):
                    pass  # Skip order creation but max_price already updated
                # Check if order limit reached for SHORT
                elif len(self.open_orders_cache['SHORT']) >= self.short_order_limit:
                    pass  # Skip order creation
                # Check order block using last trade price (actual execution price)
                elif self._check_order_block('SHORT', last_trade_price):
                    logger.warning(f"{self.symbol}: SHORT SIGNAL - mark_price={mark_price}, last_trade={last_trade_price}, "
                                  f"trigger={short_trigger:.8f}, old_max={old_max}, new_max={self.max_price}")
                    await self._open_position('SHORT', mark_price)

    async def _open_position(self, side: str, mark_price: Decimal):
        """Open LONG or SHORT position - wait for User Data Stream to confirm fill"""
        try:
            order_side = 'BUY' if side == 'LONG' else 'SELL'

            # Create market order
            response = create_market_order(
                client=self.client,
                symbol=self.symbol,
                side=order_side,
                quantity=self._format_quantity(self.position_size),
                position_side=side
            )

            order_id = str(response.get('orderId'))
            logger.warning(f"{self.symbol}: MARKET ORDER CREATED - side={side}, order_id={order_id}")

            # Add to pending - User Data Stream will handle the fill event
            self.pending_market_orders[order_id] = {
                'side': side,
                'created_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING ORDER - side={side}, error={e}")

    async def _create_stop_orders(self, side: str, entry_price: Decimal, quantity: Decimal, db_id: int):
        """Create trailing stop and stop loss orders after position is filled"""
        try:
            # Use fixed threshold values calculated at startup
            # Calculate stop loss price and trailing activation
            if side == 'LONG':
                stop_loss_price = entry_price - self.stop_loss_price
                # For trailing stop: SELL order, activation = entry + profit threshold
                trailing_activation = entry_price + self.profit_threshold_price
                trailing_side = 'SELL'
                stop_side = 'SELL'
            else:  # SHORT
                stop_loss_price = entry_price + self.stop_loss_price
                # For trailing stop: BUY order, activation = entry - profit threshold
                trailing_activation = entry_price - self.profit_threshold_price
                trailing_side = 'BUY'
                stop_side = 'BUY'

            # Create stop loss order
            stop_loss_response = create_stop_market_order(
                client=self.client,
                symbol=self.symbol,
                side=stop_side,
                quantity=self._format_quantity(quantity),
                stop_price=self._format_price(stop_loss_price),
                position_side=side
            )
            stop_loss_order_id = str(stop_loss_response.get('orderId'))

            logger.warning(f"{self.symbol}: STOP LOSS CREATED - side={side}, "
                          f"stop_price={stop_loss_price:.8f}, order_id={stop_loss_order_id}")

            # Create trailing stop order
            trailing_response = create_trailing_stop_order(
                client=self.client,
                symbol=self.symbol,
                side=trailing_side,
                quantity=self._format_quantity(quantity),
                callback_rate=float(self.trailing_stop_callback_rate),
                activation_price=self._format_price(trailing_activation),
                position_side=side
            )
            trailing_order_id = str(trailing_response.get('orderId'))

            logger.warning(f"{self.symbol}: TRAILING STOP CREATED - side={side}, "
                          f"activation={trailing_activation:.8f}, callback={self.trailing_stop_callback_rate}%, "
                          f"order_id={trailing_order_id}")

            # Update database with stop order IDs
            self.db.update_order_stop_orders(
                order_db_id=db_id,
                stop_loss_order_id=stop_loss_order_id,
                trailing_stop_order_id=trailing_order_id
            )

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING STOP ORDERS - error={e}")

    def handle_order_fill(self, order_data: Dict):
        """Handle order fill event from User Data Stream"""
        try:
            order_id = str(order_data.get('i'))  # orderId
            status = order_data.get('X')  # order status
            order_type = order_data.get('o')  # current order type
            original_order_type = order_data.get('ot')  # original order type (use this for stop orders)

            if status != 'FILLED':
                return

            # Handle MARKET order fills - create stop orders
            if order_type == 'MARKET' and order_id in self.pending_market_orders:
                side = self.pending_market_orders[order_id]['side']
                filled_price = Decimal(order_data.get('ap', '0'))  # average fill price
                filled_qty = Decimal(order_data.get('z', '0'))  # cumulative filled quantity

                logger.warning(f"{self.symbol}: MARKET ORDER FILLED - side={side}, order_id={order_id}, "
                             f"price={filled_price}, qty={filled_qty}")

                # Record in database
                db_id = self.db.add_order(
                    symbol=self.symbol,
                    side=side,
                    quantity=filled_qty,
                    entry_price=filled_price,
                    order_id=order_id
                )

                # Add to cache
                order_cache_entry = {
                    'id': db_id,
                    'symbol': self.symbol,
                    'side': side,
                    'order_id': order_id,
                    'quantity': filled_qty,
                    'entry_price': filled_price,
                    'status': 'OPEN'
                }
                self.open_orders_cache[side].append(order_cache_entry)

                # Create stop orders using asyncio (only if stop loss is enabled)
                if self.stop_loss_percent is not None:
                    asyncio.create_task(self._create_stop_orders(side, filled_price, filled_qty, db_id))

                # Remove from pending
                del self.pending_market_orders[order_id]
                return

            # Handle STOP_MARKET and TRAILING_STOP_MARKET fills - close position
            if original_order_type in ['STOP_MARKET', 'TRAILING_STOP_MARKET']:
                # Get order from database
                db_order = self.db.get_order_by_binance_id(self.symbol, order_id)
                if not db_order:
                    return

                side = db_order['side']
                entry_price = Decimal(str(db_order['entry_price']))
                exit_price = Decimal(order_data.get('ap', '0'))  # average fill price

                # Get realized profit from Binance (includes fees)
                pnl = Decimal(order_data.get('rp', '0'))

                close_reason = 'STOP_LOSS' if original_order_type == 'STOP_MARKET' else 'TRAILING_STOP'

                # Get position breakeven price if available
                position_breakeven = self.position_entry_price.get(side)
                breakeven_str = f", breakeven={position_breakeven}" if position_breakeven else ""

                logger.warning(f"{self.symbol}: {close_reason} HIT - side={side}, "
                             f"entry={entry_price}, exit={exit_price}{breakeven_str}, pnl=${pnl:.2f}")

                # Close order in database
                self.db.close_order(
                    order_db_id=db_order['id'],
                    exit_price=exit_price,
                    profit_usdt=pnl,
                    close_reason=close_reason
                )

                # Remove from cache
                self.open_orders_cache[side] = [
                    o for o in self.open_orders_cache[side]
                    if o['id'] != db_order['id']
                ]

                # Cancel the other stop order
                other_order_id = db_order.get('trailing_stop_order_id' if original_order_type == 'STOP_MARKET' else 'stop_loss_order_id')
                if other_order_id:
                    try:
                        cancel_order(self.client, self.symbol, order_id=int(other_order_id))
                        logger.info(f"{self.symbol}: Cancelled other stop order {other_order_id}")
                    except Exception as e:
                        logger.error(f"{self.symbol}: Error cancelling order {other_order_id}: {e}")

        except Exception as e:
            logger.error(f"{self.symbol}: Error handling order fill: {e}")

    def update_position_entry_price(self, position_data: Dict):
        """Update position entry price from ACCOUNT_UPDATE event"""
        try:
            position_side = position_data.get('ps')  # LONG or SHORT
            entry_price = position_data.get('ep')  # Entry price (breakeven)
            position_amt = float(position_data.get('pa', 0))  # Position amount

            # Only update if there's an actual position
            if position_amt != 0 and entry_price:
                self.position_entry_price[position_side] = Decimal(str(entry_price))

        except Exception as e:
            logger.error(f"{self.symbol}: Error updating position entry price: {e}")

    def _get_current_price(self):
        """Get current price from Binance"""
        try:
            price = get_price(self.client, self.symbol)
            return Decimal(str(price))
        except Exception as e:
            logger.error(f"{self.symbol}: Error getting price: {e}")
            return Decimal('0')

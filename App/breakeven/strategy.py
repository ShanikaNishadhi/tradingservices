import asyncio
import logging
import redis
from decimal import Decimal
from typing import Dict, Optional
from datetime import datetime
from binance.client import Client

from App.helpers.futureorder import (
    set_leverage,
    create_market_order,
    create_trailing_stop_order,
    cancel_order,
    get_price,
    get_position_info
)
from App.breakeven.database import BreakevenDatabase

logger = logging.getLogger(__name__)


class BreakevenStrategy:
    """Breakeven strategy - manages LONG and SHORT positions with single TP per direction"""

    def __init__(self, client: Client, symbol_config: Dict, db: BreakevenDatabase,
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
        self.leverage = symbol_config['leverage']
        self.price_precision = symbol_config['price_precision']
        self.quantity_precision = symbol_config['quantity_precision']
        self.long_enabled = symbol_config.get('long_enabled', True)
        self.short_enabled = symbol_config.get('short_enabled', True)
        self.long_order_limit = symbol_config.get('long_order_limit', 999)
        self.short_order_limit = symbol_config.get('short_order_limit', 999)
        self.long_margin_limit = symbol_config.get('long_margin_limit')  # Absolute price or None
        self.short_margin_limit = symbol_config.get('short_margin_limit')  # Absolute price or None
        self.forward_order_block_percent = Decimal(str(symbol_config.get('forward_order_block_percent', 0)))
        self.backward_order_block_percent = Decimal(str(symbol_config.get('backward_order_block_percent', 0)))

        # Trading state
        self.start_price = None
        self.min_price = None
        self.max_price = None
        self.order_threshold_price = None
        self.profit_threshold_price = None
        self.forward_order_block_price = None
        self.backward_order_block_price = None
        self.running = False

        # Position state (LONG and SHORT tracked separately)
        self.position_state = {
            'LONG': {
                'breakeven': None,
                'quantity': Decimal('0'),
                'current_tp_order_id': None,
                'open_order_count': 0,
                'pending_market_orders': {}  # {order_id: created_at}
            },
            'SHORT': {
                'breakeven': None,
                'quantity': Decimal('0'),
                'current_tp_order_id': None,
                'open_order_count': 0,
                'pending_market_orders': {}  # {order_id: created_at}
            }
        }

        # Track last API price call for rate limiting
        self.last_api_price_call = None

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

    def _get_last_trade_price(self) -> Decimal:
        """Get current last trade price from Redis, fallback to API once per minute"""
        try:
            key = f"last_trade_price:{self.symbol}"
            price_str = self.redis_client.get(key)

            if price_str:
                return Decimal(price_str)

            # Redis returned None - log error and fallback to API
            logger.error(f"{self.symbol}: Last trade price not available in Redis, falling back to API")

            # Rate limit: only call API once per minute
            now = datetime.now()
            if self.last_api_price_call is None or (now - self.last_api_price_call).total_seconds() >= 60:
                price = get_price(self.client, self.symbol)
                self.last_api_price_call = now
                return Decimal(str(price))

            return Decimal('0')  # Skip this tick if within rate limit

        except Exception as e:
            logger.error(f"{self.symbol}: Error getting last trade price from Redis: {e}")
            return Decimal('0')

    def _check_order_block(self, side: str, current_price: Decimal) -> bool:
        """Check if new order is too close to existing orders of same side"""
        if self.forward_order_block_price == 0 and self.backward_order_block_price == 0:
            return True

        # Get open orders from database
        open_orders = self.db.get_open_orders(self.symbol, side)

        if not open_orders:
            return True

        prices = [Decimal(str(o['entry_price'])) for o in open_orders]

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
        """Initialize strategy state on startup"""
        # Get current price as start price
        current_price = self._get_current_price()
        if current_price > 0:
            self.start_price = current_price
            self.min_price = current_price
            self.max_price = current_price
        else:
            logger.error(f"{self.symbol}: Could not get current price")
            return

        # Calculate threshold values based on start price
        self._calculate_thresholds()

        # Load existing positions from Binance
        self._load_positions_from_binance()

        logger.warning(f"{self.symbol}: INITIALIZED - start_price={self.start_price}, "
                      f"order_threshold=${self.order_threshold_price:.8f} ({self.order_threshold_percent}%), "
                      f"profit_threshold=${self.profit_threshold_price:.8f} ({self.profit_threshold_percent}%), "
                      f"forward_block=${self.forward_order_block_price:.8f} ({self.forward_order_block_percent}%), "
                      f"backward_block=${self.backward_order_block_price:.8f} ({self.backward_order_block_percent}%), "
                      f"trailing_callback={self.trailing_stop_callback_rate}%, "
                      f"long_margin_limit={self.long_margin_limit}, short_margin_limit={self.short_margin_limit}, "
                      f"leverage={self.leverage}x")

    def _calculate_thresholds(self):
        """Calculate threshold values as price movements"""
        self.order_threshold_price = self.start_price * (self.order_threshold_percent / Decimal('100'))
        self.profit_threshold_price = self.start_price * (self.profit_threshold_percent / Decimal('100'))
        self.forward_order_block_price = self.start_price * (self.forward_order_block_percent / Decimal('100'))
        self.backward_order_block_price = self.start_price * (self.backward_order_block_percent / Decimal('100'))

    def _load_positions_from_binance(self):
        """Load existing positions and TP orders from Binance on restart"""
        try:
            # Get position information
            positions = get_position_info(self.client, self.symbol)

            for position in positions:
                side = position['positionSide']
                position_amt = float(position['positionAmt'])
                entry_price = float(position['entryPrice'])

                if side in ['LONG', 'SHORT'] and position_amt != 0:
                    self.position_state[side]['breakeven'] = Decimal(str(entry_price))
                    self.position_state[side]['quantity'] = Decimal(str(abs(position_amt)))

                    logger.info(f"{self.symbol}: Loaded {side} position from Binance - "
                              f"breakeven={entry_price}, quantity={abs(position_amt)}")

                    # Ensure position exists in database
                    db_position = self.db.get_open_position(self.symbol, side)
                    if not db_position:
                        self.db.create_position(self.symbol, side)

            # Get open orders and check for TP orders
            open_orders = self.client.futures_get_open_orders(symbol=self.symbol)

            for order in open_orders:
                if order['type'] == 'TRAILING_STOP_MARKET':
                    side = order['positionSide']
                    order_id = str(order['orderId'])
                    activation_price = order.get('activatePrice')

                    self.position_state[side]['current_tp_order_id'] = order_id

                    logger.info(f"{self.symbol}: Loaded {side} TP order from Binance - "
                              f"order_id={order_id}, activation={activation_price}")

                    # Update database
                    self.db.update_position_breakeven(
                        self.symbol, side,
                        self.position_state[side]['breakeven'],
                        self.position_state[side]['quantity'],
                        order_id
                    )

        except Exception as e:
            logger.error(f"{self.symbol}: Error loading positions from Binance: {e}")

    def _get_current_price(self) -> Decimal:
        """Get current price from Binance API (used only at startup)"""
        try:
            price = get_price(self.client, self.symbol)
            return Decimal(str(price))
        except Exception as e:
            logger.error(f"{self.symbol}: Error getting price: {e}")
            return Decimal('0')

    async def run(self):
        """Main strategy loop - runs LONG and SHORT strategies concurrently"""
        self.running = True
        logger.info(f"{self.symbol}: Starting strategy loop (LONG and SHORT tasks)")

        try:
            # Run LONG and SHORT strategies concurrently
            await asyncio.gather(
                self._run_long_strategy(),
                self._run_short_strategy()
            )
        except Exception as e:
            logger.error(f"{self.symbol}: Error in strategy loop: {e}")

    async def stop(self):
        """Stop strategy loop"""
        self.running = False
        logger.info(f"{self.symbol}: Strategy stopped")

    async def _run_long_strategy(self):
        """LONG strategy - polls every 1 second"""
        logger.info(f"{self.symbol}: LONG strategy started")

        while self.running:
            try:
                await self._check_long_entry()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"{self.symbol}: Error in LONG strategy: {e}")
                await asyncio.sleep(1)

    async def _run_short_strategy(self):
        """SHORT strategy - polls every 1 second"""
        logger.info(f"{self.symbol}: SHORT strategy started")

        while self.running:
            try:
                await self._check_short_entry()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"{self.symbol}: Error in SHORT strategy: {e}")
                await asyncio.sleep(1)

    async def _check_long_entry(self):
        """Check for LONG entry signals"""
        if not self.long_enabled:
            return

        # Get current price
        current_price = self._get_last_trade_price()
        if current_price == 0:
            return

        # Update min/max prices
        if current_price < self.min_price:
            self.min_price = current_price
        if current_price > self.max_price:
            self.max_price = current_price

        # Check margin limit
        if self.long_margin_limit is not None and current_price > Decimal(str(self.long_margin_limit)):
            return

        # LONG signal: price > min_price + order_threshold
        long_trigger = self.min_price + self.order_threshold_price
        if current_price >= long_trigger:
            # Check order limit
            if self.position_state['LONG']['open_order_count'] >= self.long_order_limit:
                return

            # Check order block
            if self._check_order_block('LONG', current_price):
                old_min = self.min_price
                self.min_price = current_price
                logger.warning(f"{self.symbol}: LONG SIGNAL - price={current_price}, "
                             f"trigger={long_trigger:.8f}, old_min={old_min}, new_min={self.min_price}")
                await self._open_position('LONG', current_price)

    async def _check_short_entry(self):
        """Check for SHORT entry signals"""
        if not self.short_enabled:
            return

        # Get current price
        current_price = self._get_last_trade_price()
        if current_price == 0:
            return

        # Update min/max prices
        if current_price < self.min_price:
            self.min_price = current_price
        if current_price > self.max_price:
            self.max_price = current_price

        # Check margin limit
        if self.short_margin_limit is not None and current_price < Decimal(str(self.short_margin_limit)):
            return

        # SHORT signal: price < max_price - order_threshold
        short_trigger = self.max_price - self.order_threshold_price
        if current_price <= short_trigger:
            # Check order limit
            if self.position_state['SHORT']['open_order_count'] >= self.short_order_limit:
                return

            # Check order block
            if self._check_order_block('SHORT', current_price):
                old_max = self.max_price
                self.max_price = current_price
                logger.warning(f"{self.symbol}: SHORT SIGNAL - price={current_price}, "
                             f"trigger={short_trigger:.8f}, old_max={old_max}, new_max={self.max_price}")
                await self._open_position('SHORT', current_price)

    async def _open_position(self, side: str, current_price: Decimal):
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
            self.position_state[side]['pending_market_orders'][order_id] = datetime.now()

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING ORDER - side={side}, error={e}")

    async def _create_tp_order(self, side: str, breakeven: Decimal, quantity: Decimal):
        """Create TP order for entire position"""
        try:
            # Calculate activation price
            if side == 'LONG':
                activation_price = breakeven + self.profit_threshold_price
                tp_side = 'SELL'
            else:  # SHORT
                activation_price = breakeven - self.profit_threshold_price
                tp_side = 'BUY'

            # Cancel old TP if exists
            old_tp_id = self.position_state[side]['current_tp_order_id']
            if old_tp_id:
                try:
                    cancel_order(self.client, self.symbol, order_id=int(old_tp_id))
                    logger.info(f"{self.symbol}: Cancelled old {side} TP order {old_tp_id}")
                except Exception as e:
                    logger.error(f"{self.symbol}: Error cancelling old TP: {e}")

            # Create new TP
            response = create_trailing_stop_order(
                client=self.client,
                symbol=self.symbol,
                side=tp_side,
                quantity=self._format_quantity(quantity),
                callback_rate=float(self.trailing_stop_callback_rate),
                activation_price=self._format_price(activation_price),
                position_side=side
            )

            tp_order_id = str(response.get('orderId'))
            self.position_state[side]['current_tp_order_id'] = tp_order_id

            logger.warning(f"{self.symbol}: TP ORDER CREATED - side={side}, "
                         f"breakeven={breakeven:.8f}, activation={activation_price:.8f}, "
                         f"callback={self.trailing_stop_callback_rate}%, order_id={tp_order_id}")

            # Update database
            self.db.update_position_breakeven(
                self.symbol, side, breakeven, quantity, tp_order_id
            )

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING TP - side={side}, error={e}")

    def handle_order_fill(self, order_data: Dict):
        """Handle order fill event from User Data Stream"""
        try:
            order_id = str(order_data.get('i'))
            status = order_data.get('X')
            order_type = order_data.get('o')
            original_order_type = order_data.get('ot')

            if status != 'FILLED':
                return

            # Handle MARKET order fills
            if order_type == 'MARKET':
                side = order_data.get('ps')  # Position side

                if side in ['LONG', 'SHORT'] and order_id in self.position_state[side]['pending_market_orders']:
                    filled_price = Decimal(order_data.get('ap', '0'))
                    filled_qty = Decimal(order_data.get('z', '0'))

                    logger.warning(f"{self.symbol}: MARKET ORDER FILLED - side={side}, "
                                 f"order_id={order_id}, price={filled_price}, qty={filled_qty}")

                    # Record in database
                    self.db.add_order(
                        symbol=self.symbol,
                        side=side,
                        quantity=filled_qty,
                        entry_price=filled_price,
                        order_id=order_id
                    )

                    # Increment order count
                    self.position_state[side]['open_order_count'] += 1

                    # Ensure position exists in database
                    db_position = self.db.get_open_position(self.symbol, side)
                    if not db_position:
                        self.db.create_position(self.symbol, side)

                    # Remove from pending
                    del self.position_state[side]['pending_market_orders'][order_id]

                    # Note: TP will be created after ACCOUNT_UPDATE provides breakeven

            # Handle TRAILING_STOP_MARKET fills
            elif original_order_type == 'TRAILING_STOP_MARKET':
                side = order_data.get('ps')  # Position side
                exit_price = Decimal(order_data.get('ap', '0'))
                realized_pnl = Decimal(order_data.get('rp', '0'))

                logger.warning(f"{self.symbol}: TP HIT - side={side}, "
                             f"exit={exit_price}, pnl=${realized_pnl:.2f}")

                # Close all orders for this side in database
                self.db.close_all_orders_for_side(self.symbol, side, exit_price, 'TRAILING_STOP')

                # Close position in database
                self.db.close_position(self.symbol, side, realized_pnl)

                # Reset position state
                self.position_state[side]['breakeven'] = None
                self.position_state[side]['quantity'] = Decimal('0')
                self.position_state[side]['current_tp_order_id'] = None
                self.position_state[side]['open_order_count'] = 0

        except Exception as e:
            logger.error(f"{self.symbol}: Error handling order fill: {e}")

    def update_position_breakeven(self, position_data: Dict):
        """Update position breakeven from ACCOUNT_UPDATE event and create/update TP"""
        try:
            side = position_data.get('ps')  # Position side
            entry_price = position_data.get('ep')  # Entry price (breakeven)
            position_amt = float(position_data.get('pa', 0))  # Position amount

            if side not in ['LONG', 'SHORT']:
                return

            # Only update if there's an actual position
            if position_amt != 0 and entry_price:
                old_breakeven = self.position_state[side]['breakeven']
                new_breakeven = Decimal(str(entry_price))
                new_quantity = Decimal(str(abs(position_amt)))

                self.position_state[side]['breakeven'] = new_breakeven
                self.position_state[side]['quantity'] = new_quantity

                logger.info(f"{self.symbol}: {side} BREAKEVEN UPDATED - "
                          f"old={old_breakeven}, new={new_breakeven}, qty={new_quantity}")

                # Create or update TP order
                asyncio.create_task(self._create_tp_order(side, new_breakeven, new_quantity))

        except Exception as e:
            logger.error(f"{self.symbol}: Error updating position breakeven: {e}")

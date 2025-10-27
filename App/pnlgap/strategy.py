import asyncio
import logging
import redis
from decimal import Decimal
from typing import Dict, Optional
from datetime import datetime, timedelta
from binance.client import Client

from App.helpers.futureorder import (
    set_leverage,
    create_market_order,
    close_all_positions,
    get_position_info,
    get_price
)
from App.pnlgap.database import PnlGridDatabase

logger = logging.getLogger(__name__)


class PnlGridStrategy:
    """Bidirectional PNL grid strategy with async execution"""

    def __init__(self, client: Client, symbol_config: Dict, db: PnlGridDatabase,
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
        self.leverage = symbol_config['leverage']

        # Trading state
        self.reference_price = None
        self.min_price = None
        self.max_price = None
        self.order_threshold_value = None
        self.profit_threshold_value = None
        self.period_id: Optional[int] = None
        self.running = False

        # Cached breakeven prices (to avoid API calls every second)
        self.cached_breakeven = {
            'long_breakeven': Decimal('0'),
            'short_breakeven': Decimal('0'),
            'long_size': Decimal('0'),
            'short_size': Decimal('0'),
            'last_updated': None
        }

        # Set leverage
        self._set_leverage()

    def _set_leverage(self):
        """Set leverage for symbol"""
        try:
            set_leverage(self.client, self.symbol, self.leverage)
            logger.info(f"{self.symbol}: Leverage set to {self.leverage}x")
        except Exception as e:
            logger.error(f"{self.symbol}: Error setting leverage: {e}")

    def initialize(self):
        """Initialize strategy state from Binance (source of truth). Database is for record-keeping only."""
        logger.info(f"{self.symbol}: Initializing from Binance API...")

        # Get open positions from Binance (source of truth)
        open_positions = self._get_open_positions()

        if open_positions:
            # Resume existing positions
            logger.warning(f"{self.symbol}: RESTART DETECTED - Found {len(open_positions)} open positions on Binance")

            # Try to get reference price and min/max from database (for consistency)
            active_period = self.db.get_active_period(self.symbol)

            if active_period and active_period.get('reference_price'):
                # Use reference price from database
                self.reference_price = Decimal(str(active_period['reference_price']))
                self.period_id = active_period['id']

                # Restore min/max from database if available
                if active_period.get('min_price') and active_period.get('max_price'):
                    self.min_price = Decimal(str(active_period['min_price']))
                    self.max_price = Decimal(str(active_period['max_price']))
                    logger.warning(f"{self.symbol}: RESUMING PERIOD {self.period_id} - ref_price={self.reference_price}, min_price={self.min_price}, max_price={self.max_price}")
                else:
                    # Fallback: set to reference if not stored
                    self.min_price = self.reference_price
                    self.max_price = self.reference_price
                    logger.warning(f"{self.symbol}: No min/max in DB, using reference={self.reference_price}")
            else:
                # Fallback: use current price as reference
                self.reference_price = self._get_current_price()
                self.min_price = self.reference_price
                self.max_price = self.reference_price
                logger.warning(f"{self.symbol}: No active period in DB, using current price as reference={self.reference_price}")
                # Start new period in database with min/max
                self.period_id = self.db.start_new_period(self.symbol, self.reference_price, self.min_price, self.max_price)
                logger.warning(f"{self.symbol}: NEW PERIOD CREATED - period_id={self.period_id}, ref_price={self.reference_price}, min_price={self.min_price}, max_price={self.max_price}")

        else:
            # No open positions, start fresh
            current_price = self._get_current_price()
            if current_price > 0:
                self.reference_price = current_price
                self.min_price = current_price
                self.max_price = current_price
                logger.warning(f"{self.symbol}: FRESH START - No open positions on Binance, starting at price={current_price}")
            else:
                logger.error(f"{self.symbol}: Could not get current price")
                return

            # Start new period in database with min/max
            self.period_id = self.db.start_new_period(self.symbol, self.reference_price, self.min_price, self.max_price)
            logger.warning(f"{self.symbol}: NEW PERIOD CREATED - period_id={self.period_id}, ref_price={self.reference_price}, min_price={self.min_price}, max_price={self.max_price}")

        # Calculate threshold values
        self._calculate_thresholds()

        # Populate initial breakeven cache
        self._refresh_breakeven_cache()

        logger.warning(f"{self.symbol}: INITIALIZED - ref_price={self.reference_price}, "
                      f"order_threshold={self.order_threshold_value:.8f} (${self.order_threshold_value:.2f}), "
                      f"profit_threshold={self.profit_threshold_value:.2f} USDT, "
                      f"leverage={self.leverage}x")

    def _calculate_thresholds(self):
        """Calculate absolute threshold values from reference price"""
        self.order_threshold_value = self.reference_price * (self.order_threshold_percent / Decimal('100'))
        position_value_usdt = self.position_size * self.reference_price
        self.profit_threshold_value = position_value_usdt * (self.profit_threshold_percent / Decimal('100'))

    async def run(self):
        """Main strategy loop - polls Redis for price every 1 second"""
        self.running = True
        logger.info(f"{self.symbol}: Starting strategy loop (polling every 1 second)")

        while self.running:
            try:
                await self.check_and_execute()
                await asyncio.sleep(1)  # Poll every 1 second
            except Exception as e:
                logger.error(f"{self.symbol}: Error in strategy loop: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """Stop strategy loop"""
        self.running = False
        logger.info(f"{self.symbol}: Strategy stopped")

    async def check_and_execute(self):
        """Main strategy execution - called every 1 second"""
        try:
            # Get current mark price from Redis
            mark_price = self._get_mark_price()

            if mark_price == 0:
                return

            # Check profit taking first
            await self._check_profit_taking(mark_price)

            # Check for new entries
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

    async def _check_entry_signals(self, mark_price: Decimal):
        """Check for LONG or SHORT entry signals"""
        if not all([self.min_price, self.max_price, self.order_threshold_value]):
            return

        # LONG signal
        long_trigger = self.max_price + self.order_threshold_value
        if mark_price >= long_trigger:
            old_max = self.max_price
            self.max_price = mark_price
            logger.warning(f"{self.symbol}: LONG SIGNAL TRIGGERED - price={mark_price}, trigger={long_trigger:.8f}, old_max={old_max}, new_max={self.max_price}")
            # Update database with new max_price
            self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
            await self._open_position('LONG', mark_price)

        # SHORT signal
        short_trigger = self.min_price - self.order_threshold_value
        if mark_price <= short_trigger:
            old_min = self.min_price
            self.min_price = mark_price
            logger.warning(f"{self.symbol}: SHORT SIGNAL TRIGGERED - price={mark_price}, trigger={short_trigger:.8f}, old_min={old_min}, new_min={self.min_price}")
            # Update database with new min_price
            self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
            await self._open_position('SHORT', mark_price)

    async def _open_position(self, side: str, mark_price: Decimal):
        """Open LONG or SHORT position"""
        try:
            order_side = 'BUY' if side == 'LONG' else 'SELL'

            # Use helper function to create market order
            response = create_market_order(
                client=self.client,
                symbol=self.symbol,
                side=order_side,
                quantity=float(self.position_size),
                position_side=side
            )

            order_id = response.get('orderId')

            # Wait for fill
            await asyncio.sleep(0.3)
            order_status = self.client.futures_get_order(symbol=self.symbol, orderId=order_id)

            if order_status.get('status') == 'FILLED':
                filled_price = Decimal(order_status.get('avgPrice', '0'))
                filled_qty = Decimal(order_status.get('executedQty', '0'))
                position_value = filled_price * filled_qty

                logger.warning(f"{self.symbol}: ORDER CREATED - side={side}, order_id={order_id}, "
                             f"filled_price={filled_price}, qty={filled_qty}, "
                             f"position_value=${position_value:.2f}, period_id={self.period_id}")

                # Record order in database
                self.db.add_order(
                    symbol=self.symbol,
                    side=side,
                    quantity=self.position_size,
                    entry_price=filled_price,
                    order_id=str(order_id),
                    period_id=self.period_id
                )

                # Refresh breakeven cache after position change
                self._refresh_breakeven_cache()

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING ORDER - side={side}, error={e}")

    async def _check_profit_taking(self, mark_price: Decimal):
        """Check if net PNL exceeds profit threshold"""
        try:
            if not self._has_open_positions():
                return

            # Refresh cache every 5 minutes as safety mechanism
            if self.cached_breakeven['last_updated'] is None or \
               datetime.now() - self.cached_breakeven['last_updated'] > timedelta(minutes=5):
                logger.info(f"{self.symbol}: Periodic cache refresh (5 minutes elapsed)")
                self._refresh_breakeven_cache()

            # Use cached breakeven prices and sizes
            long_breakeven = self.cached_breakeven['long_breakeven']
            short_breakeven = self.cached_breakeven['short_breakeven']
            long_size = self.cached_breakeven['long_size']
            short_size = self.cached_breakeven['short_size']

            # Calculate PNL
            long_pnl = Decimal('0')
            short_pnl = Decimal('0')

            if long_size > 0 and long_breakeven > 0:
                long_pnl = (mark_price - long_breakeven) * long_size

            if short_size > 0 and short_breakeven > 0:
                short_pnl = (short_breakeven - mark_price) * short_size

            net_pnl = long_pnl + short_pnl

            # Check if profit target hit
            if net_pnl >= self.profit_threshold_value:
                logger.warning(f"{self.symbol}: PROFIT TARGET HIT! "
                             f"net_pnl=${net_pnl:.2f}, profit_threshold=${self.profit_threshold_value:.2f}, "
                             f"long_pnl=${long_pnl:.2f} (size={long_size}, breakeven={long_breakeven}), "
                             f"short_pnl=${short_pnl:.2f} (size={short_size}, breakeven={short_breakeven}), "
                             f"current_price={mark_price}")
                await self._close_all_positions(net_pnl, mark_price)

        except Exception as e:
            logger.error(f"{self.symbol}: Error checking profit: {e}")

    async def _close_all_positions(self, net_pnl: Decimal, close_price: Decimal):
        """Close all positions and reset"""
        try:
            old_period_id = self.period_id
            logger.warning(f"{self.symbol}: CLOSING ALL POSITIONS - period_id={old_period_id}, "
                         f"profit=${net_pnl:.2f}, close_price={close_price}")

            # Use helper function to close all positions
            close_all_positions(self.client, self.symbol)

            await asyncio.sleep(1)

            # Reset breakeven cache after closing all positions
            self.cached_breakeven = {
                'long_breakeven': Decimal('0'),
                'short_breakeven': Decimal('0'),
                'long_size': Decimal('0'),
                'short_size': Decimal('0'),
                'last_updated': datetime.now()
            }

            # End current period in database
            if self.period_id:
                self.db.end_period(self.period_id, net_pnl)
                self.db.close_all_open_orders(self.symbol, self.period_id)
                logger.warning(f"{self.symbol}: PERIOD {old_period_id} ENDED - total_profit=${net_pnl:.2f}")

            # Reset to new period
            current_price = self._get_current_price()
            old_ref = self.reference_price
            self.reference_price = current_price
            self.min_price = current_price
            self.max_price = current_price
            self._calculate_thresholds()

            # Start new period in database with min/max
            self.period_id = self.db.start_new_period(self.symbol, self.reference_price, self.min_price, self.max_price)

            logger.warning(f"{self.symbol}: NEW PERIOD STARTED - period_id={self.period_id}, "
                         f"new_ref_price={self.reference_price}, old_ref_price={old_ref}, "
                         f"order_threshold={self.order_threshold_value:.8f}, "
                         f"profit_threshold=${self.profit_threshold_value:.2f}")

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CLOSING POSITIONS - error={e}")

    def _get_open_positions(self):
        """Get open positions from Binance"""
        try:
            positions = get_position_info(self.client, self.symbol)
            return [pos for pos in positions if float(pos.get('positionAmt', 0)) != 0]
        except Exception as e:
            logger.error(f"{self.symbol}: Error getting positions: {e}")
            return []

    def _has_open_positions(self):
        """Check if there are open positions"""
        return len(self._get_open_positions()) > 0

    def _refresh_breakeven_cache(self):
        """Refresh cached breakeven prices from Binance API"""
        try:
            positions = get_position_info(self.client, self.symbol)

            # Reset cache values
            self.cached_breakeven['long_breakeven'] = Decimal('0')
            self.cached_breakeven['short_breakeven'] = Decimal('0')
            self.cached_breakeven['long_size'] = Decimal('0')
            self.cached_breakeven['short_size'] = Decimal('0')

            for pos in positions:
                pos_side = pos.get('positionSide')
                pos_amt = Decimal(str(pos.get('positionAmt', 0)))
                breakeven = Decimal(str(pos.get('breakEvenPrice', 0)))

                if pos_side == 'LONG' and pos_amt > 0:
                    self.cached_breakeven['long_breakeven'] = breakeven
                    self.cached_breakeven['long_size'] = pos_amt
                elif pos_side == 'SHORT' and pos_amt < 0:
                    self.cached_breakeven['short_breakeven'] = breakeven
                    self.cached_breakeven['short_size'] = abs(pos_amt)

            self.cached_breakeven['last_updated'] = datetime.now()
            logger.info(f"{self.symbol}: Cache refreshed - LONG: {self.cached_breakeven['long_size']}@{self.cached_breakeven['long_breakeven']}, "
                       f"SHORT: {self.cached_breakeven['short_size']}@{self.cached_breakeven['short_breakeven']}")

        except Exception as e:
            logger.error(f"{self.symbol}: Error refreshing breakeven cache: {e}")

    def _get_current_price(self):
        """Get current price from Binance"""
        try:
            price = get_price(self.client, self.symbol)
            return Decimal(str(price))
        except Exception as e:
            logger.error(f"{self.symbol}: Error getting price: {e}")
            return Decimal('0')

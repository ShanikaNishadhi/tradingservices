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
    create_stop_limit_order,
    create_stop_market_order,
    create_trailing_stop_order,
    cancel_order,
    close_all_positions,
    get_position_info,
    get_price
)
from App.advancedsimpletrends.database import AdvancedSimpleTrendsDatabase

logger = logging.getLogger(__name__)


class AdvancedSimpleTrendsStrategy:
    """AdvancedSimpleTrends strategy - SimpleTrends primary, PNLGap rare (large thresholds)"""

    def __init__(self, client: Client, symbol_config: Dict, db: AdvancedSimpleTrendsDatabase,
                 redis_client: redis.Redis):
        self.client = client
        self.symbol = symbol_config['symbol']
        self.config = symbol_config
        self.db = db
        self.redis_client = redis_client

        # ===== PNLGAP CONFIG (Parent) =====
        self.pnlgap_long_position_size = Decimal(str(symbol_config['pnlgap']['long_position_size']))
        self.pnlgap_short_position_size = Decimal(str(symbol_config['pnlgap']['short_position_size']))
        self.pnlgap_long_first_order_threshold_percent = Decimal(str(symbol_config['pnlgap']['long_first_order_threshold_percent']))
        self.pnlgap_short_first_order_threshold_percent = Decimal(str(symbol_config['pnlgap']['short_first_order_threshold_percent']))
        self.pnlgap_gap_percent = Decimal(str(symbol_config['pnlgap']['pnl_gap_percent']))
        self.pnlgap_long_order_threshold_percent = Decimal(str(symbol_config['pnlgap']['long_order_threshold_percent']))
        self.pnlgap_short_order_threshold_percent = Decimal(str(symbol_config['pnlgap']['short_order_threshold_percent']))
        self.pnlgap_long_profit_threshold_percent = Decimal(str(symbol_config['pnlgap']['long_profit_threshold_percent']))
        self.pnlgap_short_profit_threshold_percent = Decimal(str(symbol_config['pnlgap']['short_profit_threshold_percent']))
        # Stop-loss can be None (disabled), or a percentage value
        stoploss_config = symbol_config['pnlgap'].get('pnl_stoploss_percent')
        self.pnlgap_stoploss_percent = Decimal(str(stoploss_config)) if stoploss_config is not None else None
        # Leverage at symbol level
        self.leverage = symbol_config['leverage']

        # ===== SIMPLETRENDS CONFIG (Child) =====
        st_config = symbol_config['simpletrends']
        self.st_long_position_size = Decimal(str(st_config['long_position_size']))
        self.st_short_position_size = Decimal(str(st_config['short_position_size']))
        self.st_long_order_threshold_percent = Decimal(str(st_config['long_order_threshold_percent']))
        self.st_short_order_threshold_percent = Decimal(str(st_config['short_order_threshold_percent']))
        self.st_long_profit_threshold_percent = Decimal(str(st_config['long_profit_threshold_percent']))
        self.st_short_profit_threshold_percent = Decimal(str(st_config['short_profit_threshold_percent']))
        self.st_trailing_stop_callback_rate = Decimal(str(st_config['trailing_stop_callback_rate']))
        self.st_stop_loss_percent = Decimal(str(st_config['stop_loss_percent'])) if st_config.get('stop_loss_percent') is not None else None
        self.st_forward_order_block_percent = Decimal(str(st_config.get('forward_order_block_percent', 0)))
        self.st_backward_order_block_percent = Decimal(str(st_config.get('backward_order_block_percent', 0)))
        self.st_long_order_limit = st_config.get('long_order_limit', 999)
        self.st_short_order_limit = st_config.get('short_order_limit', 999)
        self.st_enable_pnl_barrier = st_config.get('enable_pnl_barrier', False)  # Default False
        self.price_precision = symbol_config['price_precision']
        self.quantity_precision = symbol_config['quantity_precision']

        # ===== PNLGAP STATE (Parent) =====
        self.reference_price = None
        self.min_price = None  # PNLGap boundary
        self.max_price = None  # PNLGap boundary
        self.pnlgap_long_first_order_threshold_value = None
        self.pnlgap_short_first_order_threshold_value = None
        self.pnlgap_long_order_threshold_value = None
        self.pnlgap_short_order_threshold_value = None
        self.pnlgap_long_profit_threshold_value = None  # Calculated from percent
        self.pnlgap_short_profit_threshold_value = None  # Calculated from percent
        self.pnlgap_stoploss_threshold_value = None  # Fixed threshold for period
        self.pnlgap_gap_abs_value = None  # Gap between opposing zones (calculated from percent)
        self.period_id: Optional[int] = None
        self.pnlgap_first_long_created = False  # Track if first LONG order has been created
        self.pnlgap_first_short_created = False  # Track if first SHORT order has been created

        # ===== SIMPLETRENDS STATE (Child) =====
        self.st_min_price = None  # SimpleTrends local min
        self.st_max_price = None  # SimpleTrends local max
        self.st_long_order_threshold_value = None
        self.st_short_order_threshold_value = None
        self.st_long_profit_threshold_value = None
        self.st_short_profit_threshold_value = None
        self.st_long_stop_loss_value = None
        self.st_short_stop_loss_value = None
        self.st_forward_order_block_value = None
        self.st_backward_order_block_value = None


        # SimpleTrends order cache
        self.st_open_orders_cache = {'LONG': [], 'SHORT': []}
        self.st_pending_market_orders = {}  # {order_id: {'side': 'LONG', 'created_at': datetime}}

        # PNLGap stop-loss tracking
        self.pnlgap_pending_stoploss_orders = {}  # {order_id: {'side': 'LONG/SHORT', 'db_id': int, 'created_at': datetime}}

        # Track last time st state was saved
        self.last_st_state_save = datetime.now()

        # Cached breakeven prices (shared by both strategies)
        self.cached_breakeven = {
            'long_breakeven': Decimal('0'),
            'short_breakeven': Decimal('0'),
            'long_size': Decimal('0'),
            'short_size': Decimal('0'),
            'last_updated': None
        }

        # Cached entry prices (for SimpleTrends zone logic)
        self.long_entry_price = Decimal('0')
        self.short_entry_price = Decimal('0')

        # Track realized PnL from ACCOUNT_UPDATE (for final period profit logging)
        self.realized_pnl = {
            'long': Decimal('0'),
            'short': Decimal('0'),
            'last_updated': None
        }

        # Track close orders when period is closing
        self.closing_period = False
        self.close_orders = {
            'long_close_price': None,
            'short_close_price': None,
            'long_close_qty': None,
            'short_close_qty': None
        }

        self.running = False

        # Set leverage
        self._set_leverage()

    def _set_leverage(self):
        """Set leverage for symbol"""
        try:
            set_leverage(self.client, self.symbol, self.leverage)
            logger.info(f"{self.symbol}: Leverage set to {self.leverage}x")
        except Exception as e:
            logger.error(f"{self.symbol}: Error setting leverage: {e}")

    def _format_quantity(self, quantity: Decimal) -> float:
        """Format quantity to correct precision"""
        return float(round(quantity, self.quantity_precision))

    def _format_price(self, price: Decimal) -> float:
        """Format price to correct precision"""
        return float(round(price, self.price_precision))

    def initialize(self):
        """Initialize strategy state from Binance (source of truth)"""
        logger.info(f"{self.symbol}: Initializing from Binance API...")

        # Get open positions from Binance
        open_positions = self._get_open_positions()

        if open_positions:
            # Resume existing positions
            logger.warning(f"{self.symbol}: RESTART DETECTED - Found {len(open_positions)} open positions on Binance")

            # Try to get reference price and min/max from database
            active_period = self.db.get_active_period(self.symbol)

            if active_period and active_period.get('reference_price'):
                # Use reference price from database
                self.reference_price = Decimal(str(active_period['reference_price']))
                self.period_id = active_period['id']

                # Restore min/max from database
                if active_period.get('min_price') and active_period.get('max_price'):
                    self.min_price = Decimal(str(active_period['min_price']))
                    self.max_price = Decimal(str(active_period['max_price']))
                    logger.warning(f"{self.symbol}: RESUMING PERIOD {self.period_id} - ref_price={self.reference_price}, min={self.min_price}, max={self.max_price}")
                else:
                    self.min_price = self.reference_price
                    self.max_price = self.reference_price
                    logger.warning(f"{self.symbol}: No min/max in DB, using reference={self.reference_price}")
            else:
                # Fallback: use current price as reference
                self.reference_price = self._get_current_price()
                self.min_price = self.reference_price
                self.max_price = self.reference_price
                logger.warning(f"{self.symbol}: No active period in DB, using current price as reference={self.reference_price}")
                # Start new period in database
                self.period_id = self.db.start_new_period(self.symbol, self.reference_price, self.min_price, self.max_price)
                logger.warning(f"{self.symbol}: NEW PERIOD CREATED - period_id={self.period_id}")

        else:
            # No open positions, start fresh
            current_price = self._get_current_price()
            if current_price > 0:
                self.reference_price = current_price
                self.min_price = current_price
                self.max_price = current_price
                logger.warning(f"{self.symbol}: FRESH START - No open positions, starting at price={current_price}")
            else:
                logger.error(f"{self.symbol}: Could not get current price")
                return

            # Start new period in database
            self.period_id = self.db.start_new_period(self.symbol, self.reference_price, self.min_price, self.max_price)
            logger.warning(f"{self.symbol}: NEW PERIOD CREATED - period_id={self.period_id}")

        # Calculate pnlgap thresholds
        self._calculate_pnlgap_thresholds()

        # Initialize SimpleTrends state
        self._initialize_simpletrends()

        # Populate initial breakeven cache
        self._refresh_breakeven_cache()

        # Detect if first orders have been created by checking for open orders
        # This fixes the bug where max_price < reference_price after a drop,
        # causing the system to incorrectly think no LONG zone exists
        if self.period_id:
            open_pnlgap_orders = self.db.get_open_pnlgap_orders(self.symbol, self.period_id)
            has_open_longs = any(order['side'] == 'LONG' for order in open_pnlgap_orders)
            has_open_shorts = any(order['side'] == 'SHORT' for order in open_pnlgap_orders)

            # If we have open orders, the zone definitely exists
            if has_open_longs:
                self.pnlgap_first_long_created = True
            elif self.max_price > self.reference_price:
                # Fallback: max moved up from reference (for fresh periods)
                self.pnlgap_first_long_created = True

            if has_open_shorts:
                self.pnlgap_first_short_created = True
            elif self.min_price < self.reference_price:
                # Fallback: min moved down from reference (for fresh periods)
                self.pnlgap_first_short_created = True
        else:
            # No period yet (shouldn't happen, but fallback to old logic)
            if self.max_price > self.reference_price:
                self.pnlgap_first_long_created = True
            if self.min_price < self.reference_price:
                self.pnlgap_first_short_created = True

        # Sync stop-loss tracking from database (after restart)
        if self.period_id and self.pnlgap_stoploss_percent is not None:
            self._sync_pnl_stoploss_tracking()

        stoploss_str = f"{self.pnlgap_stoploss_percent}%" if self.pnlgap_stoploss_percent is not None else "DISABLED"
        logger.warning(f"{self.symbol}: INITIALIZED - ref={self.reference_price}, "
                      f"first_long_created={self.pnlgap_first_long_created}, first_short_created={self.pnlgap_first_short_created}, "
                      f"pnlgap_long_threshold={self.pnlgap_long_order_threshold_value:.8f} ({self.pnlgap_long_order_threshold_percent}%), "
                      f"pnlgap_short_threshold={self.pnlgap_short_order_threshold_value:.8f} ({self.pnlgap_short_order_threshold_percent}%), "
                      f"long_profit_threshold=${self.pnlgap_long_profit_threshold_value:.2f}, "
                      f"short_profit_threshold=${self.pnlgap_short_profit_threshold_value:.2f}, "
                      f"pnl_stoploss={stoploss_str}, "
                      f"leverage={self.leverage}x")

    def _calculate_pnlgap_thresholds(self):
        """Calculate pnlgap threshold values from reference price"""
        # First order thresholds
        self.pnlgap_long_first_order_threshold_value = self.reference_price * (self.pnlgap_long_first_order_threshold_percent / Decimal('100'))
        self.pnlgap_short_first_order_threshold_value = self.reference_price * (self.pnlgap_short_first_order_threshold_percent / Decimal('100'))

        # Subsequent order thresholds
        self.pnlgap_long_order_threshold_value = self.reference_price * (self.pnlgap_long_order_threshold_percent / Decimal('100'))
        self.pnlgap_short_order_threshold_value = self.reference_price * (self.pnlgap_short_order_threshold_percent / Decimal('100'))

        # Calculate separate profit threshold values
        long_position_value_usdt = self.pnlgap_long_position_size * self.reference_price
        short_position_value_usdt = self.pnlgap_short_position_size * self.reference_price
        self.pnlgap_long_profit_threshold_value = long_position_value_usdt * (self.pnlgap_long_profit_threshold_percent / Decimal('100'))
        self.pnlgap_short_profit_threshold_value = short_position_value_usdt * (self.pnlgap_short_profit_threshold_percent / Decimal('100'))

        # Calculate stop-loss threshold (fixed for period) - only if stop-loss is enabled
        if self.pnlgap_stoploss_percent is not None:
            self.pnlgap_stoploss_threshold_value = self.reference_price * (self.pnlgap_stoploss_percent / Decimal('100'))
        else:
            self.pnlgap_stoploss_threshold_value = None

        # Calculate PNL gap absolute value (fixed for period)
        self.pnlgap_gap_abs_value = self.reference_price * (self.pnlgap_gap_percent / Decimal('100'))

    def _initialize_simpletrends(self):
        """Initialize SimpleTrends state"""
        # Try to load saved st state from database
        saved_st_state = self.db.get_st_state(self.symbol)

        # Get current price
        current_price = self._get_current_price()
        if current_price <= 0:
            logger.error(f"{self.symbol}: Could not get current price for ST init")
            return

        # Use saved st min/max if available, otherwise use current price
        if saved_st_state:
            self.st_min_price = Decimal(str(saved_st_state['min_price']))
            self.st_max_price = Decimal(str(saved_st_state['max_price']))
            logger.info(f"{self.symbol}: ST - Loaded saved state - min={self.st_min_price}, max={self.st_max_price}")
        else:
            self.st_min_price = current_price
            self.st_max_price = current_price
            logger.info(f"{self.symbol}: ST - No saved state, using current price - {current_price}")

        # Calculate st threshold values based on reference price
        self._calculate_st_thresholds()

        # Load open st orders from database into cache
        self._load_st_orders_cache()

    def _calculate_st_thresholds(self):
        """Calculate SimpleTrends threshold values"""
        # Order thresholds
        self.st_long_order_threshold_value = self.reference_price * (self.st_long_order_threshold_percent / Decimal('100'))
        self.st_short_order_threshold_value = self.reference_price * (self.st_short_order_threshold_percent / Decimal('100'))

        # Profit thresholds
        self.st_long_profit_threshold_value = self.reference_price * (self.st_long_profit_threshold_percent / Decimal('100'))
        self.st_short_profit_threshold_value = self.reference_price * (self.st_short_profit_threshold_percent / Decimal('100'))

        # Stop loss (None if disabled)
        if self.st_stop_loss_percent is not None:
            self.st_long_stop_loss_value = self.reference_price * (self.st_stop_loss_percent / Decimal('100'))
            self.st_short_stop_loss_value = self.reference_price * (self.st_stop_loss_percent / Decimal('100'))

        # Order blocking
        self.st_forward_order_block_value = self.reference_price * (self.st_forward_order_block_percent / Decimal('100'))
        self.st_backward_order_block_value = self.reference_price * (self.st_backward_order_block_percent / Decimal('100'))

    def _load_st_orders_cache(self):
        """Load open simpletrends orders from database into cache"""
        open_orders = self.db.get_st_open_orders(self.symbol)

        # Clear cache first
        self.st_open_orders_cache = {'LONG': [], 'SHORT': []}

        # Populate cache
        for order in open_orders:
            side = order['side']
            self.st_open_orders_cache[side].append(order)

        logger.info(f"{self.symbol}: ST - Loaded {len(open_orders)} open orders into cache "
                   f"(LONG: {len(self.st_open_orders_cache['LONG'])}, SHORT: {len(self.st_open_orders_cache['SHORT'])})")

    async def run(self):
        """Main strategy loop - polls Redis for price every 1 second"""
        self.running = True
        logger.info(f"{self.symbol}: Starting strategy loop (polling every 1 second)")

        while self.running:
            try:
                await self.check_and_execute()

                # Save st state to database every 5 minutes
                now = datetime.now()
                if (now - self.last_st_state_save).total_seconds() >= 300:  # 5 minutes
                    if self.st_min_price and self.st_max_price:
                        self.db.save_st_state(self.symbol, self.st_min_price, self.st_max_price)
                        self.last_st_state_save = now
                        logger.info(f"{self.symbol}: ST - Saved state - min={self.st_min_price}, max={self.st_max_price}")

                await asyncio.sleep(1)  # Poll every 1 second
            except Exception as e:
                logger.error(f"{self.symbol}: Error in strategy loop: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """Stop strategy loop"""
        self.running = False

        # Save st state one final time
        if self.st_min_price and self.st_max_price:
            self.db.save_st_state(self.symbol, self.st_min_price, self.st_max_price)
            logger.info(f"{self.symbol}: ST - Final state saved")

        logger.info(f"{self.symbol}: Strategy stopped")

    async def check_and_execute(self):
        """Main strategy execution - called every 1 second"""
        try:
            # Get current mark price from Redis
            mark_price = self._get_mark_price()
            if mark_price == 0:
                return

            # 1. Check period close FIRST (pnlgap profit taking)
            await self._check_period_close(mark_price)

            # 2. Check pnlgap entry signals
            await self._check_pnlgap_entries(mark_price)

            # 3. Update simpletrends min/max
            self._update_st_min_max(mark_price)

            # 4. Check simpletrends entry signals
            await self._check_st_entries(mark_price)

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

    # ===== PNLGAP METHODS (Parent) =====

    async def _check_period_close(self, mark_price: Decimal):
        """Check if period should close (breakeven + profit threshold met)"""
        try:
            # Check if there are open positions using cache (avoids API call)
            if self.cached_breakeven['long_size'] == 0 and self.cached_breakeven['short_size'] == 0:
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

            # Get last trade price for PNL calculation
            last_trade_price = self._get_last_trade_price()
            if last_trade_price == Decimal('0'):
                last_trade_price = mark_price

            # Calculate PNL using last trade price
            long_pnl = Decimal('0')
            short_pnl = Decimal('0')

            if long_size > 0 and long_breakeven > 0:
                long_pnl = (last_trade_price - long_breakeven) * long_size

            if short_size > 0 and short_breakeven > 0:
                short_pnl = (short_breakeven - last_trade_price) * short_size

            net_pnl = long_pnl + short_pnl

            # Determine which side is winning and use its profit threshold
            if long_pnl > short_pnl:
                # LONG is winning, use LONG profit threshold
                profit_threshold = self.pnlgap_long_profit_threshold_value
                winning_side = "LONG"
            else:
                # SHORT is winning, use SHORT profit threshold
                profit_threshold = self.pnlgap_short_profit_threshold_value
                winning_side = "SHORT"

            # Check if profit target hit (safety: skip if threshold not initialized)
            if profit_threshold is not None and net_pnl >= profit_threshold:
                logger.warning(f"{self.symbol}: PROFIT TARGET HIT! "
                             f"net_pnl=${net_pnl:.2f}, threshold=${profit_threshold:.2f} ({winning_side} winning), "
                             f"long_pnl=${long_pnl:.2f} (size={long_size}, breakeven={long_breakeven}), "
                             f"short_pnl=${short_pnl:.2f} (size={short_size}, breakeven={short_breakeven}), "
                             f"last_trade_price={last_trade_price}, mark_price={mark_price}")
                await self._close_period(net_pnl, last_trade_price)

        except Exception as e:
            logger.error(f"{self.symbol}: Error checking period close: {e}")

    async def _check_pnlgap_entries(self, mark_price: Decimal):
        """Check for pnlgap LONG or SHORT entry signals with dynamic gap logic"""
        if not all([self.min_price, self.max_price, self.pnlgap_long_order_threshold_value]):
            return

        # ===== LONG ENTRY LOGIC =====
        if not self.pnlgap_first_long_created:
            # CASE 1: First LONG (no LONG zone exists yet)
            if not self.pnlgap_first_short_created:
                # Neither zone exists - use initial large threshold
                long_trigger = self.reference_price + self.pnlgap_long_first_order_threshold_value
                trigger_type = "initial"
            else:
                # SHORT zone exists - create LONG zone close to it using gap
                long_trigger = self.min_price + self.pnlgap_gap_abs_value
                trigger_type = "gap-activated"

            if mark_price >= long_trigger:
                # Create first LONG
                old_max = self.max_price
                self.max_price = mark_price
                logger.warning(f"{self.symbol}: PNLGAP FIRST LONG ({trigger_type}) - "
                             f"price={mark_price}, trigger={long_trigger:.8f}, old_max={old_max}, new_max={self.max_price}, "
                             f"gap=${self.pnlgap_gap_abs_value:.8f}")
                self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
                await self._open_pnlgap_position('LONG', mark_price)
                self.pnlgap_first_long_created = True
        else:
            # CASE 2: Subsequent LONGs (pyramid) - normal logic
            long_trigger = self.max_price + self.pnlgap_long_order_threshold_value
            if mark_price >= long_trigger:
                old_max = self.max_price
                self.max_price = mark_price
                logger.warning(f"{self.symbol}: PNLGAP LONG (pyramid) - "
                             f"price={mark_price}, trigger={long_trigger:.8f}, old_max={old_max}, new_max={self.max_price}")
                self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
                await self._open_pnlgap_position('LONG', mark_price)

        # ===== SHORT ENTRY LOGIC =====
        if not self.pnlgap_first_short_created:
            # CASE 1: First SHORT (no SHORT zone exists yet)
            if not self.pnlgap_first_long_created:
                # Neither zone exists - use initial large threshold
                short_trigger = self.reference_price - self.pnlgap_short_first_order_threshold_value
                trigger_type = "initial"
            else:
                # LONG zone exists - create SHORT zone close to it using gap
                short_trigger = self.max_price - self.pnlgap_gap_abs_value
                trigger_type = "gap-activated"

            if mark_price <= short_trigger:
                # Create first SHORT
                old_min = self.min_price
                self.min_price = mark_price
                logger.warning(f"{self.symbol}: PNLGAP FIRST SHORT ({trigger_type}) - "
                             f"price={mark_price}, trigger={short_trigger:.8f}, old_min={old_min}, new_min={self.min_price}, "
                             f"gap=${self.pnlgap_gap_abs_value:.8f}")
                self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
                await self._open_pnlgap_position('SHORT', mark_price)
                self.pnlgap_first_short_created = True
        else:
            # CASE 2: Subsequent SHORTs (pyramid) - normal logic
            short_trigger = self.min_price - self.pnlgap_short_order_threshold_value
            if mark_price <= short_trigger:
                old_min = self.min_price
                self.min_price = mark_price
                logger.warning(f"{self.symbol}: PNLGAP SHORT (pyramid) - "
                             f"price={mark_price}, trigger={short_trigger:.8f}, old_min={old_min}, new_min={self.min_price}")
                self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
                await self._open_pnlgap_position('SHORT', mark_price)

    async def _open_pnlgap_position(self, side: str, mark_price: Decimal):
        """Open pnlgap LONG or SHORT position"""
        try:
            order_side = 'BUY' if side == 'LONG' else 'SELL'
            position_size = self.pnlgap_long_position_size if side == 'LONG' else self.pnlgap_short_position_size

            # Create market order
            response = create_market_order(
                client=self.client,
                symbol=self.symbol,
                side=order_side,
                quantity=float(position_size),
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

                logger.warning(f"{self.symbol}: PNLGAP ORDER CREATED - side={side}, order_id={order_id}, "
                             f"filled_price={filled_price}, qty={filled_qty}, "
                             f"position_value=${position_value:.2f}, period_id={self.period_id}")

                # Record order in database
                db_id = self.db.add_pnlgap_order(
                    symbol=self.symbol,
                    side=side,
                    quantity=position_size,
                    entry_price=filled_price,
                    order_id=str(order_id),
                    period_id=self.period_id
                )

                # Create stop-loss order if configured (but NOT for first LONG or first SHORT)
                is_first_long = (side == 'LONG' and not self.pnlgap_first_long_created)
                is_first_short = (side == 'SHORT' and not self.pnlgap_first_short_created)

                if self.pnlgap_stoploss_percent is not None and not (is_first_long or is_first_short):
                    await self._create_pnl_stoploss_order(side, filled_price, filled_qty, db_id, str(order_id))
                elif is_first_long or is_first_short:
                    logger.warning(f"{self.symbol}: SKIPPING STOP-LOSS for FIRST {side} - This is an anchor position")
                elif self.pnlgap_stoploss_percent is None:
                    logger.info(f"{self.symbol}: PNL stop-loss DISABLED - No stop-loss order created")

                # Refresh breakeven cache after position change
                self._refresh_breakeven_cache()

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING PNLGAP ORDER - side={side}, error={e}")

    async def _create_pnl_stoploss_order(self, side: str, entry_price: Decimal, quantity: Decimal, db_id: int, entry_order_id: str):
        """Create stop-market order for PNL position"""
        try:
            # Calculate stop-loss price
            if side == 'LONG':
                stoploss_price = entry_price - self.pnlgap_stoploss_threshold_value
                order_side = 'SELL'
            else:
                stoploss_price = entry_price + self.pnlgap_stoploss_threshold_value
                order_side = 'BUY'

            # Create stop-market order (trigger at stoploss_price, execute at market)
            response = create_stop_market_order(
                client=self.client,
                symbol=self.symbol,
                side=order_side,
                quantity=self._format_quantity(quantity),
                stop_price=self._format_price(stoploss_price),  # Trigger price
                position_side=side
            )

            stoploss_order_id = str(response.get('orderId'))
            logger.warning(f"{self.symbol}: PNL STOP-LOSS ORDER CREATED - "
                         f"side={side}, stoploss_order_id={stoploss_order_id}, "
                         f"entry={entry_price}, stoploss_price={stoploss_price}, "
                         f"threshold={self.pnlgap_stoploss_threshold_value}, "
                         f"entry_order={entry_order_id}")

            # Save stop-loss order ID to database
            self.db.update_pnlgap_order_stoploss(db_id, stoploss_order_id)

            # Track pending stop-loss order
            self.pnlgap_pending_stoploss_orders[stoploss_order_id] = {
                'side': side,
                'db_id': db_id,
                'entry_order_id': entry_order_id,
                'entry_price': entry_price,
                'stoploss_price': stoploss_price,
                'created_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING PNL STOP-LOSS ORDER - side={side}, error={e}")

    def _sync_pnl_stoploss_tracking(self):
        """Sync stop-loss tracking dictionary from database (after restart)"""
        try:
            # Get all open PNL orders from database
            open_pnl_orders = self.db.get_open_pnlgap_orders(self.symbol, self.period_id)

            if not open_pnl_orders:
                logger.info(f"{self.symbol}: No open PNL orders to sync stop-loss tracking")
                return

            synced_count = 0
            for order in open_pnl_orders:
                stoploss_order_id = order.get('stoploss_order_id')
                if stoploss_order_id:
                    # Restore tracking
                    self.pnlgap_pending_stoploss_orders[stoploss_order_id] = {
                        'side': order['side'],
                        'db_id': order['id'],
                        'entry_order_id': str(order['order_id']),
                        'entry_price': Decimal(str(order['entry_price'])),
                        'stoploss_price': None,  # We don't store this, but it's not critical
                        'created_at': datetime.now()
                    }
                    synced_count += 1

            if synced_count > 0:
                logger.warning(f"{self.symbol}: SYNCED {synced_count} PNL stop-loss orders from database")

        except Exception as e:
            logger.error(f"{self.symbol}: Error syncing PNL stop-loss tracking: {e}")

    def _cancel_all_pending_stop_orders(self):
        """Cancel all pending TRAILING_STOP_MARKET, STOP_MARKET, and STOP orders from Binance"""
        try:
            open_orders = self.client.futures_get_open_orders(symbol=self.symbol)
            canceled_count = 0

            for order in open_orders:
                order_type = order.get('type')
                order_id = order.get('orderId')

                # Cancel trailing stops, stop market orders, and stop-limit orders (PNL stop-loss)
                if order_type in ['TRAILING_STOP_MARKET', 'STOP_MARKET', 'STOP']:
                    try:
                        cancel_order(self.client, self.symbol, order_id)
                        canceled_count += 1
                        logger.info(f"{self.symbol}: Canceled {order_type} order {order_id}")
                    except Exception as e:
                        logger.warning(f"{self.symbol}: Error canceling order {order_id}: {e}")

            if canceled_count > 0:
                logger.warning(f"{self.symbol}: Canceled {canceled_count} pending stop orders")

            # Clear PNL stop-loss tracking
            self.pnlgap_pending_stoploss_orders.clear()

            return canceled_count

        except Exception as e:
            logger.error(f"{self.symbol}: Error getting/canceling open orders: {e}")
            return 0

    async def _close_period(self, net_pnl: Decimal, close_price: Decimal):
        """Close period - close all positions (pnlgap + simpletrends) and reset"""
        try:
            old_period_id = self.period_id
            logger.warning(f"{self.symbol}: CLOSING PERIOD - period_id={old_period_id}, "
                         f"calculated_profit=${net_pnl:.2f}, estimated_close_price={close_price}")

            # Reset tracking before closing positions
            self.closing_period = True
            self.realized_pnl = {
                'long': Decimal('0'),
                'short': Decimal('0'),
                'last_updated': None
            }
            self.close_orders = {
                'long_close_price': None,
                'short_close_price': None,
                'long_close_qty': None,
                'short_close_qty': None
            }

            # Determine which side is losing (close it first to minimize slippage)
            long_breakeven = self.cached_breakeven['long_breakeven']
            short_breakeven = self.cached_breakeven['short_breakeven']
            long_size = self.cached_breakeven['long_size']
            short_size = self.cached_breakeven['short_size']

            long_pnl = (close_price - long_breakeven) * long_size if long_size > 0 else Decimal('0')
            short_pnl = (short_breakeven - close_price) * short_size if short_size > 0 else Decimal('0')

            # Close losing side first, then winning side
            if long_pnl < short_pnl:
                # LONG is losing, close LONG first
                logger.warning(f"{self.symbol}: Closing LONG first (losing side), then SHORT")
                if long_size > 0:
                    close_all_positions(self.client, self.symbol, 'LONG')
                    await asyncio.sleep(0.2)
                if short_size > 0:
                    close_all_positions(self.client, self.symbol, 'SHORT')
            else:
                # SHORT is losing, close SHORT first
                logger.warning(f"{self.symbol}: Closing SHORT first (losing side), then LONG")
                if short_size > 0:
                    close_all_positions(self.client, self.symbol, 'SHORT')
                    await asyncio.sleep(0.2)
                if long_size > 0:
                    close_all_positions(self.client, self.symbol, 'LONG')

            # Cancel any remaining trailing stop orders (cleanup)
            self._cancel_all_pending_stop_orders()

            # Wait for ORDER_TRADE_UPDATE and ACCOUNT_UPDATE events
            await asyncio.sleep(2)

            # Get actual realized PnL from ACCOUNT_UPDATE
            actual_pnl = self.realized_pnl['long'] + self.realized_pnl['short']

            # Log close details
            logger.warning(f"{self.symbol}: POSITION CLOSES - "
                         f"LONG: price={self.close_orders['long_close_price']}, qty={self.close_orders['long_close_qty']}, "
                         f"SHORT: price={self.close_orders['short_close_price']}, qty={self.close_orders['short_close_qty']}")

            logger.warning(f"{self.symbol}: REALIZED PNL - "
                         f"long=${self.realized_pnl['long']:.2f}, "
                         f"short=${self.realized_pnl['short']:.2f}, "
                         f"total=${actual_pnl:.2f} (calculated estimate was ${net_pnl:.2f})")

            # Reset closing flag
            self.closing_period = False

            # Reset breakeven cache
            self.cached_breakeven = {
                'long_breakeven': Decimal('0'),
                'short_breakeven': Decimal('0'),
                'long_size': Decimal('0'),
                'short_size': Decimal('0'),
                'last_updated': datetime.now()
            }

            # End current period in database using actual realized PnL
            if self.period_id:
                self.db.end_period(self.period_id, actual_pnl)
                self.db.close_all_pnlgap_orders(self.symbol, self.period_id)
                self.db.close_all_st_orders(self.symbol, self.period_id)
                logger.warning(f"{self.symbol}: PERIOD {old_period_id} ENDED - total_profit=${actual_pnl:.2f}")

            # Check if we should continue periods
            continue_periods = self.config.get('continue_periods', True)
            # Handle case-insensitive boolean (False, false, FALSE, "false", etc.)
            if isinstance(continue_periods, str):
                continue_periods = continue_periods.lower() not in ['false', '0', 'no']

            if not continue_periods:
                logger.warning(f"{self.symbol}: STOPPING STRATEGY - continue_periods is False. No new periods will be created.")
                self.running = False
                return

            # Clear simpletrends cache for new period
            self.st_open_orders_cache = {'LONG': [], 'SHORT': []}
            self.st_pending_market_orders = {}

            # Reset to new period
            current_price = self._get_current_price()
            old_ref = self.reference_price
            self.reference_price = current_price
            self.min_price = current_price
            self.max_price = current_price
            self.st_min_price = current_price
            self.st_max_price = current_price

            # Reset first order flags for new period
            self.pnlgap_first_long_created = False
            self.pnlgap_first_short_created = False

            # Recalculate thresholds
            self._calculate_pnlgap_thresholds()
            self._calculate_st_thresholds()

            # Start new period in database
            self.period_id = self.db.start_new_period(self.symbol, self.reference_price, self.min_price, self.max_price)

            logger.warning(f"{self.symbol}: NEW PERIOD STARTED - period_id={self.period_id}, "
                         f"new_ref={self.reference_price}, old_ref={old_ref}")

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CLOSING PERIOD - error={e}")

    # ===== SIMPLETRENDS METHODS (Child) =====

    def _update_st_min_max(self, mark_price: Decimal):
        """Update simpletrends min and max prices"""
        if self.st_min_price is None or self.st_max_price is None:
            return

        if mark_price < self.st_min_price:
            self.st_min_price = mark_price

        if mark_price > self.st_max_price:
            self.st_max_price = mark_price

    async def _check_st_entries(self, mark_price: Decimal):
        """Check for SimpleTrends entry signals with optional PNL barrier logic"""
        if not all([self.st_min_price, self.st_max_price, self.st_long_order_threshold_value]):
            return

        # Get last trade price for order blocking
        last_trade_price = self._get_last_trade_price()
        if last_trade_price == Decimal('0'):
            last_trade_price = mark_price

        # LONG signal: Check barrier first (if enabled), then normal trigger
        if self.st_enable_pnl_barrier:
            # Barrier: No PNL LONG exists OR price < PNL LONG entry
            long_barrier_ok = (not self.pnlgap_first_long_created) or (mark_price < self.long_entry_price)
        else:
            # Barrier disabled - always allow
            long_barrier_ok = True

        if long_barrier_ok:
            long_trigger = self.st_min_price + self.st_long_order_threshold_value
            if mark_price >= long_trigger:
                # Check order limit
                if len(self.st_open_orders_cache['LONG']) < self.st_long_order_limit:
                    # Check order block
                    if self._check_st_order_block('LONG', last_trade_price):
                        old_min = self.st_min_price
                        self.st_min_price = mark_price
                        if self.st_enable_pnl_barrier:
                            barrier_str = f"below barrier {self.long_entry_price}" if self.long_entry_price > 0 else "no barrier"
                        else:
                            barrier_str = "barrier disabled"
                        logger.warning(f"{self.symbol}: ST LONG SIGNAL - mark_price={mark_price}, last_trade={last_trade_price}, "
                                      f"trigger={long_trigger:.8f}, old_min={old_min}, new_min={self.st_min_price}, {barrier_str}")
                        await self._open_st_position('LONG', mark_price)

        # SHORT signal: Check barrier first (if enabled), then normal trigger
        if self.st_enable_pnl_barrier:
            # Barrier: No PNL SHORT exists OR price > PNL SHORT entry
            short_barrier_ok = (not self.pnlgap_first_short_created) or (mark_price > self.short_entry_price)
        else:
            # Barrier disabled - always allow
            short_barrier_ok = True

        if short_barrier_ok:
            short_trigger = self.st_max_price - self.st_short_order_threshold_value
            if mark_price <= short_trigger:
                # Check order limit
                if len(self.st_open_orders_cache['SHORT']) < self.st_short_order_limit:
                    # Check order block
                    if self._check_st_order_block('SHORT', last_trade_price):
                        old_max = self.st_max_price
                        self.st_max_price = mark_price
                        if self.st_enable_pnl_barrier:
                            barrier_str = f"above barrier {self.short_entry_price}" if self.short_entry_price > 0 else "no barrier"
                        else:
                            barrier_str = "barrier disabled"
                        logger.warning(f"{self.symbol}: ST SHORT SIGNAL - mark_price={mark_price}, last_trade={last_trade_price}, "
                                      f"trigger={short_trigger:.8f}, old_max={old_max}, new_max={self.st_max_price}, {barrier_str}")
                        await self._open_st_position('SHORT', mark_price)

    def _check_st_order_block(self, side: str, current_price: Decimal) -> bool:
        """Check if new ST order is too close to existing ST orders of same side"""
        if self.st_forward_order_block_value == 0 and self.st_backward_order_block_value == 0:
            return True

        same_side_orders = self.st_open_orders_cache[side]

        if not same_side_orders:
            return True

        prices = [Decimal(str(o['entry_price'])) for o in same_side_orders]

        # Find closest order above (forward) and below (backward)
        forward_orders = [p for p in prices if p >= current_price]
        backward_orders = [p for p in prices if p <= current_price]

        # Check forward block
        if self.st_forward_order_block_value > 0 and forward_orders:
            closest_forward = min(forward_orders)
            if closest_forward - current_price < self.st_forward_order_block_value:
                return False

        # Check backward block
        if self.st_backward_order_block_value > 0 and backward_orders:
            closest_backward = max(backward_orders)
            if current_price - closest_backward < self.st_backward_order_block_value:
                return False

        return True

    async def _open_st_position(self, side: str, mark_price: Decimal):
        """Open SimpleTrends LONG or SHORT position - wait for User Data Stream to confirm fill"""
        try:
            order_side = 'BUY' if side == 'LONG' else 'SELL'
            position_size = self.st_long_position_size if side == 'LONG' else self.st_short_position_size

            # Create market order
            response = create_market_order(
                client=self.client,
                symbol=self.symbol,
                side=order_side,
                quantity=self._format_quantity(position_size),
                position_side=side
            )

            order_id = str(response.get('orderId'))
            logger.warning(f"{self.symbol}: ST MARKET ORDER CREATED - side={side}, order_id={order_id}")

            # Add to pending - User Data Stream will handle the fill event
            self.st_pending_market_orders[order_id] = {
                'side': side,
                'created_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING ST ORDER - side={side}, error={e}")

    async def _create_st_stop_orders(self, side: str, entry_price: Decimal, quantity: Decimal, db_id: int):
        """Create trailing stop and optionally stop loss orders for ST position"""
        try:
            # Determine order parameters based on position direction
            if side == 'LONG':
                # For trailing stop: SELL order, activation = entry + profit threshold
                trailing_activation = entry_price + self.st_long_profit_threshold_value
                trailing_side = 'SELL'
                stop_side = 'SELL'
                stop_loss_price = entry_price - self.st_long_stop_loss_value if self.st_long_stop_loss_value else None
            else:  # SHORT
                # For trailing stop: BUY order, activation = entry - profit threshold
                trailing_activation = entry_price - self.st_short_profit_threshold_value
                trailing_side = 'BUY'
                stop_side = 'BUY'
                stop_loss_price = entry_price + self.st_short_stop_loss_value if self.st_short_stop_loss_value else None

            stop_loss_order_id = None

            # Create stop loss order only if enabled
            if stop_loss_price is not None:
                stop_loss_response = create_stop_market_order(
                    client=self.client,
                    symbol=self.symbol,
                    side=stop_side,
                    quantity=self._format_quantity(quantity),
                    stop_price=self._format_price(stop_loss_price),
                    position_side=side
                )
                stop_loss_order_id = str(stop_loss_response.get('orderId'))

                logger.warning(f"{self.symbol}: ST STOP LOSS CREATED - side={side}, "
                              f"stop_price={stop_loss_price:.8f}, order_id={stop_loss_order_id}")

            # Always create trailing stop order
            trailing_response = create_trailing_stop_order(
                client=self.client,
                symbol=self.symbol,
                side=trailing_side,
                quantity=self._format_quantity(quantity),
                callback_rate=float(self.st_trailing_stop_callback_rate),
                activation_price=self._format_price(trailing_activation),
                position_side=side
            )
            trailing_order_id = str(trailing_response.get('orderId'))

            logger.warning(f"{self.symbol}: ST TRAILING STOP CREATED - side={side}, "
                          f"activation={trailing_activation:.8f}, callback={self.st_trailing_stop_callback_rate}%, "
                          f"order_id={trailing_order_id}")

            # Update database with stop order IDs
            self.db.update_st_order_stop_orders(
                order_db_id=db_id,
                stop_loss_order_id=stop_loss_order_id,
                trailing_stop_order_id=trailing_order_id
            )

        except Exception as e:
            logger.error(f"{self.symbol}: ERROR CREATING ST STOP ORDERS - error={e}")

    def handle_order_fill(self, order_data: Dict):
        """Handle order fill event from User Data Stream (for SimpleTrends and close orders)"""
        try:
            order_id = str(order_data.get('i'))  # orderId
            status = order_data.get('X')  # order status
            order_type = order_data.get('o')  # current order type
            original_order_type = order_data.get('ot')  # original order type
            position_side = order_data.get('ps')  # LONG or SHORT
            side_type = order_data.get('S')  # BUY or SELL

            if status != 'FILLED':
                return

            # Handle close orders when period is closing
            if self.closing_period and order_type == 'MARKET':
                filled_price = Decimal(order_data.get('ap', '0'))  # average fill price
                filled_qty = Decimal(order_data.get('z', '0'))  # cumulative filled quantity

                if position_side == 'LONG' and side_type == 'SELL':
                    self.close_orders['long_close_price'] = filled_price
                    self.close_orders['long_close_qty'] = filled_qty
                    logger.warning(f"{self.symbol}: LONG POSITION CLOSED - order_id={order_id}, "
                                 f"close_price={filled_price}, qty={filled_qty}")
                elif position_side == 'SHORT' and side_type == 'BUY':
                    self.close_orders['short_close_price'] = filled_price
                    self.close_orders['short_close_qty'] = filled_qty
                    logger.warning(f"{self.symbol}: SHORT POSITION CLOSED - order_id={order_id}, "
                                 f"close_price={filled_price}, qty={filled_qty}")
                return

            # Handle ST MARKET order fills - create stop orders
            if order_type == 'MARKET' and order_id in self.st_pending_market_orders:
                side = self.st_pending_market_orders[order_id]['side']
                filled_price = Decimal(order_data.get('ap', '0'))  # average fill price
                filled_qty = Decimal(order_data.get('z', '0'))  # cumulative filled quantity

                logger.warning(f"{self.symbol}: ST MARKET ORDER FILLED - side={side}, order_id={order_id}, "
                             f"price={filled_price}, qty={filled_qty}")

                # Record in database
                db_id = self.db.add_st_order(
                    symbol=self.symbol,
                    side=side,
                    quantity=filled_qty,
                    entry_price=filled_price,
                    order_id=order_id,
                    period_id=self.period_id
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
                self.st_open_orders_cache[side].append(order_cache_entry)

                # Create stop orders
                asyncio.create_task(self._create_st_stop_orders(side, filled_price, filled_qty, db_id))

                # Remove from pending
                del self.st_pending_market_orders[order_id]

                # Refresh breakeven cache
                self._refresh_breakeven_cache()
                return

            # Handle PNL stop-loss fills - close position and revert min/max
            if order_id in self.pnlgap_pending_stoploss_orders:
                sl_info = self.pnlgap_pending_stoploss_orders[order_id]
                side = sl_info['side']
                db_id = sl_info['db_id']
                entry_price = sl_info['entry_price']

                filled_price = Decimal(order_data.get('ap', '0'))  # average fill price
                pnl = Decimal(order_data.get('rp', '0'))  # realized profit

                logger.warning(f"{self.symbol}: PNL STOP-LOSS FILLED - side={side}, "
                             f"entry={entry_price}, exit={filled_price}, pnl=${pnl:.2f}, "
                             f"entry_order={sl_info['entry_order_id']}")

                # Close order in database
                self.db.close_pnlgap_order(
                    order_db_id=db_id,
                    exit_price=filled_price,
                    profit_usdt=pnl
                )

                # Revert min/max to second furthest order value
                second_furthest_price = self.db.get_second_furthest_pnlgap_order_price(
                    self.symbol, self.period_id, side
                )

                if second_furthest_price:
                    if side == 'LONG':
                        old_max = self.max_price
                        self.max_price = second_furthest_price
                        logger.warning(f"{self.symbol}: REVERTED MAX after stop-loss - "
                                     f"old_max={old_max}, new_max={self.max_price}")
                    else:
                        old_min = self.min_price
                        self.min_price = second_furthest_price
                        logger.warning(f"{self.symbol}: REVERTED MIN after stop-loss - "
                                     f"old_min={old_min}, new_min={self.min_price}")

                    # Update database with reverted min/max
                    self.db.update_period_prices(self.period_id, self.min_price, self.max_price)
                else:
                    logger.warning(f"{self.symbol}: No second furthest {side} order found, keeping current min/max")

                # Remove from pending
                del self.pnlgap_pending_stoploss_orders[order_id]

                # Refresh breakeven cache
                self._refresh_breakeven_cache()
                return

            # Handle ST STOP_MARKET and TRAILING_STOP_MARKET fills - close position
            if original_order_type in ['STOP_MARKET', 'TRAILING_STOP_MARKET']:
                # Get order from database
                db_order = self.db.get_st_order_by_binance_id(self.symbol, order_id)
                if not db_order:
                    return

                side = db_order['side']
                entry_price = Decimal(str(db_order['entry_price']))
                exit_price = Decimal(order_data.get('ap', '0'))  # average fill price

                # Get realized profit from Binance
                pnl = Decimal(order_data.get('rp', '0'))

                close_reason = 'STOP_LOSS' if original_order_type == 'STOP_MARKET' else 'TRAILING_STOP'

                logger.warning(f"{self.symbol}: ST {close_reason} HIT - side={side}, "
                             f"entry={entry_price}, exit={exit_price}, pnl=${pnl:.2f}")

                # Close order in database
                self.db.close_st_order(
                    order_db_id=db_order['id'],
                    exit_price=exit_price,
                    profit_usdt=pnl,
                    close_reason=close_reason
                )

                # Remove from cache
                self.st_open_orders_cache[side] = [
                    o for o in self.st_open_orders_cache[side]
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

                # Refresh breakeven cache
                self._refresh_breakeven_cache()

        except Exception as e:
            logger.error(f"{self.symbol}: Error handling order fill: {e}")

    def update_position_entry_price(self, position_data: Dict):
        """Update position entry price and realized PnL from ACCOUNT_UPDATE event"""
        try:
            position_side = position_data.get('ps')  # LONG or SHORT
            entry_price = position_data.get('ep')  # Entry price (weighted average of fills)
            breakeven_price = position_data.get('bep')  # Break even price (entry + fees + funding)
            position_amt = float(position_data.get('pa', 0))  # Position amount
            cumulative_realized = position_data.get('cr')  # Cumulative realized PnL

            # Update cached breakeven if position exists (use bep for PnL calculations)
            if position_amt != 0 and breakeven_price:
                if position_side == 'LONG':
                    self.cached_breakeven['long_breakeven'] = Decimal(str(breakeven_price))
                    self.cached_breakeven['long_size'] = Decimal(str(abs(position_amt)))
                elif position_side == 'SHORT':
                    self.cached_breakeven['short_breakeven'] = Decimal(str(breakeven_price))
                    self.cached_breakeven['short_size'] = Decimal(str(abs(position_amt)))

                self.cached_breakeven['last_updated'] = datetime.now()

            # Update entry prices for SimpleTrends zone logic
            if entry_price:
                if position_side == 'LONG':
                    self.long_entry_price = Decimal(str(entry_price))
                elif position_side == 'SHORT':
                    self.short_entry_price = Decimal(str(entry_price))

            # Update realized PnL (always, even if position is closed)
            if cumulative_realized is not None:
                if position_side == 'LONG':
                    self.realized_pnl['long'] = Decimal(str(cumulative_realized))
                elif position_side == 'SHORT':
                    self.realized_pnl['short'] = Decimal(str(cumulative_realized))

                self.realized_pnl['last_updated'] = datetime.now()

        except Exception as e:
            logger.error(f"{self.symbol}: Error updating position entry price: {e}")

    # ===== SHARED METHODS =====

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
                entry_price = Decimal(str(pos.get('entryPrice', 0)))

                if pos_side == 'LONG' and pos_amt > 0:
                    self.cached_breakeven['long_breakeven'] = breakeven
                    self.cached_breakeven['long_size'] = pos_amt
                    self.long_entry_price = entry_price
                elif pos_side == 'SHORT' and pos_amt < 0:
                    self.cached_breakeven['short_breakeven'] = breakeven
                    self.cached_breakeven['short_size'] = abs(pos_amt)
                    self.short_entry_price = entry_price

            self.cached_breakeven['last_updated'] = datetime.now()
            logger.info(f"{self.symbol}: Cache refreshed - LONG: {self.cached_breakeven['long_size']}@{self.cached_breakeven['long_breakeven']} (entry={self.long_entry_price}), "
                       f"SHORT: {self.cached_breakeven['short_size']}@{self.cached_breakeven['short_breakeven']} (entry={self.short_entry_price})")

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

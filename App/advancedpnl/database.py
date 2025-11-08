import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class AdvancedPnlDatabase:
    """Database manager for Advanced PNL strategy - combines PNLGap and SimpleTrends"""

    def __init__(self):
        self.conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'timescaledb'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'tradingplatform'),
            'user': os.getenv('POSTGRES_USER', 'classic'),
            'password': os.getenv('POSTGRES_PASSWORD', 'Postgresql@2025')
        }
        self._init_database()

    def _init_database(self):
        """Initialize database tables"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        # PNLGap-style orders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS advancedpnl_pnlgap_orders (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                side VARCHAR(10) NOT NULL,
                order_id VARCHAR(50),
                quantity DECIMAL NOT NULL,
                entry_price DECIMAL NOT NULL,
                exit_price DECIMAL,
                status VARCHAR(20) NOT NULL,
                opened_at TIMESTAMPTZ DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                profit_usdt DECIMAL,
                period_id INTEGER
            )
        """)

        # SimpleTrends-style orders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS advancedpnl_simpletrends_orders (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                side VARCHAR(10) NOT NULL,
                order_id VARCHAR(50),
                quantity DECIMAL NOT NULL,
                entry_price DECIMAL NOT NULL,
                exit_price DECIMAL,
                status VARCHAR(20) NOT NULL,
                opened_at TIMESTAMPTZ DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                profit_usdt DECIMAL,
                stop_loss_order_id VARCHAR(50),
                trailing_stop_order_id VARCHAR(50),
                close_reason VARCHAR(50),
                period_id INTEGER
            )
        """)

        # Periods table (matches pnlgap schema + adds simpletrends counts)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS advancedpnl_periods (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                reference_price DECIMAL NOT NULL,
                min_price DECIMAL,
                max_price DECIMAL,
                started_at TIMESTAMPTZ DEFAULT NOW(),
                ended_at TIMESTAMPTZ,
                total_profit_usdt DECIMAL,
                long_count INTEGER DEFAULT 0,
                short_count INTEGER DEFAULT 0,
                st_long_count INTEGER DEFAULT 0,
                st_short_count INTEGER DEFAULT 0,
                status VARCHAR(20) NOT NULL
            )
        """)

        # SimpleTrends state table (for st_min/st_max persistence)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS advancedpnl_st_state (
                symbol VARCHAR(20) PRIMARY KEY,
                min_price DECIMAL NOT NULL,
                max_price DECIMAL NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_advpnl_pnlgap_orders_symbol_status
            ON advancedpnl_pnlgap_orders(symbol, status)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_advpnl_st_orders_symbol_status
            ON advancedpnl_simpletrends_orders(symbol, status)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_advpnl_periods_symbol
            ON advancedpnl_periods(symbol, status)
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.conn_params['host']}")

    # ===== PERIOD METHODS =====

    def start_new_period(self, symbol: str, reference_price: Decimal,
                         min_price: Optional[Decimal] = None, max_price: Optional[Decimal] = None) -> int:
        """Start a new trading period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        min_val = float(min_price if min_price else reference_price)
        max_val = float(max_price if max_price else reference_price)

        cursor.execute("""
            INSERT INTO advancedpnl_periods
            (symbol, reference_price, min_price, max_price, status)
            VALUES (%s, %s, %s, %s, 'ACTIVE')
            RETURNING id
        """, (symbol, float(reference_price), min_val, max_val))

        period_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        logger.info(f"Started period {period_id} for {symbol} @ {reference_price}")
        return period_id

    def end_period(self, period_id: int, total_profit_usdt: Decimal):
        """End a trading period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE advancedpnl_periods
            SET status = 'CLOSED',
                ended_at = NOW(),
                total_profit_usdt = %s
            WHERE id = %s
        """, (float(total_profit_usdt), period_id))

        conn.commit()
        conn.close()

        logger.info(f"Ended period {period_id}, profit: ${total_profit_usdt:.2f}")

    def get_active_period(self, symbol: str) -> Optional[Dict]:
        """Get active trading period for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM advancedpnl_periods
            WHERE symbol = %s AND status = 'ACTIVE'
            ORDER BY id DESC
            LIMIT 1
        """, (symbol,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def update_period_prices(self, period_id: int, min_price: Decimal, max_price: Decimal):
        """Update min and max prices for a period (pnlgap boundaries)"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE advancedpnl_periods
            SET min_price = %s, max_price = %s
            WHERE id = %s
        """, (float(min_price), float(max_price), period_id))

        conn.commit()
        conn.close()

    # ===== PNLGAP ORDER METHODS =====

    def add_pnlgap_order(self, symbol: str, side: str, quantity: Decimal, entry_price: Decimal,
                         order_id: Optional[str] = None, period_id: Optional[int] = None) -> int:
        """Add pnlgap-style order to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO advancedpnl_pnlgap_orders
            (symbol, side, order_id, quantity, entry_price, status, period_id)
            VALUES (%s, %s, %s, %s, %s, 'OPEN', %s)
            RETURNING id
        """, (symbol, side, order_id, float(quantity), float(entry_price), period_id))

        order_db_id = cursor.fetchone()[0]

        # Update period counts
        if period_id:
            if side == 'LONG':
                cursor.execute("""
                    UPDATE advancedpnl_periods
                    SET long_count = long_count + 1
                    WHERE id = %s
                """, (period_id,))
            else:
                cursor.execute("""
                    UPDATE advancedpnl_periods
                    SET short_count = short_count + 1
                    WHERE id = %s
                """, (period_id,))

        conn.commit()
        conn.close()

        logger.info(f"Recorded PNLGAP {side} order for {symbol}: ID={order_db_id}, Price={entry_price}")
        return order_db_id

    def close_all_pnlgap_orders(self, symbol: str, period_id: int):
        """Mark all open pnlgap orders as closed for a period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE advancedpnl_pnlgap_orders
            SET status = 'CLOSED',
                closed_at = NOW()
            WHERE symbol = %s AND period_id = %s AND status = 'OPEN'
        """, (symbol, period_id))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Closed {rows_updated} pnlgap orders for period {period_id}")

    # ===== SIMPLETRENDS ORDER METHODS =====

    def add_st_order(self, symbol: str, side: str, quantity: Decimal, entry_price: Decimal,
                     order_id: Optional[str] = None, period_id: Optional[int] = None) -> int:
        """Add simpletrends-style order to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO advancedpnl_simpletrends_orders
            (symbol, side, order_id, quantity, entry_price, status, period_id)
            VALUES (%s, %s, %s, %s, %s, 'OPEN', %s)
            RETURNING id
        """, (symbol, side, order_id, float(quantity), float(entry_price), period_id))

        order_db_id = cursor.fetchone()[0]

        # Update period simpletrends counts
        if period_id:
            if side == 'LONG':
                cursor.execute("""
                    UPDATE advancedpnl_periods
                    SET st_long_count = st_long_count + 1
                    WHERE id = %s
                """, (period_id,))
            else:
                cursor.execute("""
                    UPDATE advancedpnl_periods
                    SET st_short_count = st_short_count + 1
                    WHERE id = %s
                """, (period_id,))

        conn.commit()
        conn.close()

        logger.info(f"Recorded ST {side} order for {symbol}: ID={order_db_id}, Price={entry_price}")
        return order_db_id

    def update_st_order_stop_orders(self, order_db_id: int, stop_loss_order_id: Optional[str] = None,
                                     trailing_stop_order_id: Optional[str] = None):
        """Update simpletrends order with stop loss and trailing stop order IDs"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE advancedpnl_simpletrends_orders
            SET stop_loss_order_id = %s,
                trailing_stop_order_id = %s
            WHERE id = %s
        """, (stop_loss_order_id, trailing_stop_order_id, order_db_id))

        conn.commit()
        conn.close()

    def close_st_order(self, order_db_id: int, exit_price: Decimal, profit_usdt: Decimal, close_reason: str):
        """Close a simpletrends order"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE advancedpnl_simpletrends_orders
            SET status = 'CLOSED',
                exit_price = %s,
                profit_usdt = %s,
                close_reason = %s,
                closed_at = NOW()
            WHERE id = %s
        """, (float(exit_price), float(profit_usdt), close_reason, order_db_id))

        conn.commit()
        conn.close()

        logger.info(f"Closed ST order {order_db_id}: exit_price={exit_price}, profit=${profit_usdt:.2f}, reason={close_reason}")

    def get_st_open_orders(self, symbol: str) -> List[Dict]:
        """Get all open simpletrends orders for a symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM advancedpnl_simpletrends_orders
            WHERE symbol = %s AND status = 'OPEN'
            ORDER BY opened_at
        """, (symbol,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_st_order_by_binance_id(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Get simpletrends order by Binance order ID"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM advancedpnl_simpletrends_orders
            WHERE symbol = %s
            AND (order_id = %s OR stop_loss_order_id = %s OR trailing_stop_order_id = %s)
            LIMIT 1
        """, (symbol, order_id, order_id, order_id))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def close_all_st_orders(self, symbol: str, period_id: int):
        """Mark all open simpletrends orders as closed for a period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE advancedpnl_simpletrends_orders
            SET status = 'CLOSED',
                close_reason = 'PERIOD_CLOSE',
                closed_at = NOW()
            WHERE symbol = %s AND period_id = %s AND status = 'OPEN'
        """, (symbol, period_id))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Closed {rows_updated} simpletrends orders for period {period_id}")

    # ===== SIMPLETRENDS STATE METHODS =====

    def save_st_state(self, symbol: str, min_price: Decimal, max_price: Decimal):
        """Save simpletrends state (min/max prices) to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO advancedpnl_st_state (symbol, min_price, max_price, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (symbol)
            DO UPDATE SET
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                updated_at = NOW()
        """, (symbol, float(min_price), float(max_price)))

        conn.commit()
        conn.close()

    def get_st_state(self, symbol: str) -> Optional[Dict]:
        """Get saved simpletrends state for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM advancedpnl_st_state
            WHERE symbol = %s
        """, (symbol,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

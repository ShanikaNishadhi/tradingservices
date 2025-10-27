import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class PnlGridDatabase:
    """Database manager for PNL Grid strategy - records orders and periods"""

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

        # Orders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pnl_grid_orders (
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

        # Trading periods table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pnl_grid_periods (
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
                status VARCHAR(20) NOT NULL
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_symbol_status
            ON pnl_grid_orders(symbol, status)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_periods_symbol
            ON pnl_grid_periods(symbol, status)
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.conn_params['host']}")

    def start_new_period(self, symbol: str, reference_price: Decimal,
                         min_price: Optional[Decimal] = None, max_price: Optional[Decimal] = None) -> int:
        """Start a new trading period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        # Use reference_price as default for min/max if not provided
        min_val = float(min_price if min_price else reference_price)
        max_val = float(max_price if max_price else reference_price)

        cursor.execute("""
            INSERT INTO pnl_grid_periods
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
            UPDATE pnl_grid_periods
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
            SELECT * FROM pnl_grid_periods
            WHERE symbol = %s AND status = 'ACTIVE'
            ORDER BY id DESC
            LIMIT 1
        """, (symbol,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def add_order(self, symbol: str, side: str, quantity: Decimal, entry_price: Decimal,
                  order_id: Optional[str] = None, period_id: Optional[int] = None) -> int:
        """Add order to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO pnl_grid_orders
            (symbol, side, order_id, quantity, entry_price, status, period_id)
            VALUES (%s, %s, %s, %s, %s, 'OPEN', %s)
            RETURNING id
        """, (symbol, side, order_id, float(quantity), float(entry_price), period_id))

        order_db_id = cursor.fetchone()[0]

        # Update period counts
        if period_id:
            if side == 'LONG':
                cursor.execute("""
                    UPDATE pnl_grid_periods
                    SET long_count = long_count + 1
                    WHERE id = %s
                """, (period_id,))
            else:
                cursor.execute("""
                    UPDATE pnl_grid_periods
                    SET short_count = short_count + 1
                    WHERE id = %s
                """, (period_id,))

        conn.commit()
        conn.close()

        logger.info(f"Recorded {side} order for {symbol}: ID={order_db_id}, Price={entry_price}")
        return order_db_id

    def close_all_open_orders(self, symbol: str, period_id: int):
        """Mark all open orders as closed for a period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE pnl_grid_orders
            SET status = 'CLOSED',
                closed_at = NOW()
            WHERE symbol = %s AND period_id = %s AND status = 'OPEN'
        """, (symbol, period_id))

        rows_updated = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Closed {rows_updated} orders in database for period {period_id}")

    def get_order_history(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get order history for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM pnl_grid_orders
            WHERE symbol = %s
            ORDER BY opened_at DESC
            LIMIT %s
        """, (symbol, limit))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_period_history(self, symbol: str, limit: int = 10) -> List[Dict]:
        """Get period history for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM pnl_grid_periods
            WHERE symbol = %s
            ORDER BY started_at DESC
            LIMIT %s
        """, (symbol, limit))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def update_period_prices(self, period_id: int, min_price: Decimal, max_price: Decimal):
        """Update min and max prices for a period"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE pnl_grid_periods
            SET min_price = %s, max_price = %s
            WHERE id = %s
        """, (float(min_price), float(max_price), period_id))

        conn.commit()
        conn.close()

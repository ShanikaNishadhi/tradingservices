import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class SimpleTrendsDatabase:
    """Database manager for Simple Trends strategy - records orders continuously"""

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

        # Orders table - no period concept, continuous trading
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS simple_trends_orders (
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
                close_reason VARCHAR(50)
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_st_orders_symbol_status
            ON simple_trends_orders(symbol, status)
        """)

        # Strategy state table - stores min/max prices per symbol
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS simple_trends_state (
                symbol VARCHAR(20) PRIMARY KEY,
                min_price DECIMAL NOT NULL,
                max_price DECIMAL NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.conn_params['host']}")

    def add_order(self, symbol: str, side: str, quantity: Decimal, entry_price: Decimal,
                  order_id: Optional[str] = None) -> int:
        """Add order to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO simple_trends_orders
            (symbol, side, order_id, quantity, entry_price, status)
            VALUES (%s, %s, %s, %s, %s, 'OPEN')
            RETURNING id
        """, (symbol, side, order_id, float(quantity), float(entry_price)))

        order_db_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        logger.info(f"Recorded {side} order for {symbol}: ID={order_db_id}, Price={entry_price}")
        return order_db_id

    def update_order_stop_orders(self, order_db_id: int, stop_loss_order_id: Optional[str] = None,
                                  trailing_stop_order_id: Optional[str] = None):
        """Update order with stop loss and trailing stop order IDs"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE simple_trends_orders
            SET stop_loss_order_id = %s,
                trailing_stop_order_id = %s
            WHERE id = %s
        """, (stop_loss_order_id, trailing_stop_order_id, order_db_id))

        conn.commit()
        conn.close()

    def close_order(self, order_db_id: int, exit_price: Decimal, profit_usdt: Decimal, close_reason: str):
        """Close an order"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE simple_trends_orders
            SET status = 'CLOSED',
                exit_price = %s,
                profit_usdt = %s,
                close_reason = %s,
                closed_at = NOW()
            WHERE id = %s
        """, (float(exit_price), float(profit_usdt), close_reason, order_db_id))

        conn.commit()
        conn.close()

        logger.info(f"Closed order {order_db_id}: exit_price={exit_price}, profit=${profit_usdt:.2f}, reason={close_reason}")

    def get_open_orders(self, symbol: str) -> List[Dict]:
        """Get all open orders for a symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM simple_trends_orders
            WHERE symbol = %s AND status = 'OPEN'
            ORDER BY opened_at
        """, (symbol,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_order_history(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get order history for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM simple_trends_orders
            WHERE symbol = %s
            ORDER BY opened_at DESC
            LIMIT %s
        """, (symbol, limit))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_order_by_binance_id(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Get order by Binance order ID - searches market, stop loss, and trailing stop IDs"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM simple_trends_orders
            WHERE symbol = %s
            AND (order_id = %s OR stop_loss_order_id = %s OR trailing_stop_order_id = %s)
            LIMIT 1
        """, (symbol, order_id, order_id, order_id))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def save_state(self, symbol: str, min_price: Decimal, max_price: Decimal):
        """Save strategy state (min/max prices) to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO simple_trends_state (symbol, min_price, max_price, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (symbol)
            DO UPDATE SET
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                updated_at = NOW()
        """, (symbol, float(min_price), float(max_price)))

        conn.commit()
        conn.close()

    def get_state(self, symbol: str) -> Optional[Dict]:
        """Get saved strategy state for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM simple_trends_state
            WHERE symbol = %s
        """, (symbol,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

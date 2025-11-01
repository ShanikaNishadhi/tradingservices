import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime
import os

logger = logging.getLogger(__name__)


class BreakevenDatabase:
    """Database manager for Breakeven strategy - tracks positions and individual orders"""

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

        # Positions table - tracks overall LONG/SHORT positions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS breakeven_positions (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                side VARCHAR(10) NOT NULL,
                breakeven_price DECIMAL,
                position_quantity DECIMAL,
                current_tp_order_id VARCHAR(50),
                status VARCHAR(20) NOT NULL,
                opened_at TIMESTAMPTZ DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                total_profit_usdt DECIMAL,
                UNIQUE(symbol, side, status)
            )
        """)

        # Orders table - tracks individual orders
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS breakeven_orders (
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
                close_reason VARCHAR(50)
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_breakeven_positions_symbol_side_status
            ON breakeven_positions(symbol, side, status)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_breakeven_orders_symbol_status
            ON breakeven_orders(symbol, status)
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.conn_params['host']}")

    # ==================== POSITION METHODS ====================

    def get_open_position(self, symbol: str, side: str) -> Optional[Dict]:
        """Get open position for symbol and side"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM breakeven_positions
            WHERE symbol = %s AND side = %s AND status = 'OPEN'
            LIMIT 1
        """, (symbol, side))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def create_position(self, symbol: str, side: str) -> int:
        """Create new position"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO breakeven_positions
            (symbol, side, status)
            VALUES (%s, %s, 'OPEN')
            RETURNING id
        """, (symbol, side))

        position_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        logger.info(f"Created {side} position for {symbol}: ID={position_id}")
        return position_id

    def update_position_breakeven(self, symbol: str, side: str, breakeven_price: Decimal,
                                   position_quantity: Decimal, tp_order_id: Optional[str] = None):
        """Update position breakeven price and TP order ID"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        if tp_order_id:
            cursor.execute("""
                UPDATE breakeven_positions
                SET breakeven_price = %s,
                    position_quantity = %s,
                    current_tp_order_id = %s
                WHERE symbol = %s AND side = %s AND status = 'OPEN'
            """, (float(breakeven_price), float(position_quantity), tp_order_id, symbol, side))
        else:
            cursor.execute("""
                UPDATE breakeven_positions
                SET breakeven_price = %s,
                    position_quantity = %s
                WHERE symbol = %s AND side = %s AND status = 'OPEN'
            """, (float(breakeven_price), float(position_quantity), symbol, side))

        conn.commit()
        conn.close()

    def close_position(self, symbol: str, side: str, total_profit: Decimal):
        """Close position"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE breakeven_positions
            SET status = 'CLOSED',
                total_profit_usdt = %s,
                closed_at = NOW()
            WHERE symbol = %s AND side = %s AND status = 'OPEN'
        """, (float(total_profit), symbol, side))

        conn.commit()
        conn.close()

        logger.info(f"Closed {side} position for {symbol}: total_profit=${total_profit:.2f}")

    # ==================== ORDER METHODS ====================

    def add_order(self, symbol: str, side: str, quantity: Decimal, entry_price: Decimal,
                  order_id: Optional[str] = None) -> int:
        """Add order to database"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO breakeven_orders
            (symbol, side, order_id, quantity, entry_price, status)
            VALUES (%s, %s, %s, %s, %s, 'OPEN')
            RETURNING id
        """, (symbol, side, order_id, float(quantity), float(entry_price)))

        order_db_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        logger.info(f"Recorded {side} order for {symbol}: ID={order_db_id}, Price={entry_price}")
        return order_db_id

    def close_order(self, order_db_id: int, exit_price: Decimal, profit_usdt: Decimal, close_reason: str):
        """Close an order"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE breakeven_orders
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

    def close_all_orders_for_side(self, symbol: str, side: str, exit_price: Decimal, close_reason: str):
        """Close all open orders for a symbol and side"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE breakeven_orders
            SET status = 'CLOSED',
                exit_price = %s,
                close_reason = %s,
                closed_at = NOW()
            WHERE symbol = %s AND side = %s AND status = 'OPEN'
        """, (float(exit_price), close_reason, symbol, side))

        rows_affected = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Closed {rows_affected} {side} orders for {symbol} at {exit_price}")
        return rows_affected

    def get_open_orders(self, symbol: str, side: str) -> List[Dict]:
        """Get all open orders for a symbol and side"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM breakeven_orders
            WHERE symbol = %s AND side = %s AND status = 'OPEN'
            ORDER BY opened_at
        """, (symbol, side))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def get_order_by_id(self, order_db_id: int) -> Optional[Dict]:
        """Get order by database ID"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM breakeven_orders
            WHERE id = %s
            LIMIT 1
        """, (order_db_id,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def get_order_history(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get order history for symbol"""
        conn = psycopg2.connect(**self.conn_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT * FROM breakeven_orders
            WHERE symbol = %s
            ORDER BY opened_at DESC
            LIMIT %s
        """, (symbol, limit))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

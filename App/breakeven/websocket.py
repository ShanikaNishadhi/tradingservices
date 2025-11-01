import asyncio
import logging
import json
import os
import redis
import websockets
from typing import List

logger = logging.getLogger(__name__)


class LastTradeWebSocket:
    """WebSocket client that streams last trade prices to Redis"""

    def __init__(self, symbols: List[str], redis_host: str = 'localhost',
                 redis_port: int = 6379, redis_db: int = 5):
        self.symbols = [s.lower() for s in symbols]
        redis_password = os.getenv('REDIS_PASSWORD')
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True, password=redis_password)
        self.running = False

        # Build WebSocket URL for trade streams
        # Format: wss://fstream.binance.com/stream?streams=symbol1@trade/symbol2@trade
        streams = '/'.join([f"{symbol}@trade" for symbol in self.symbols])
        self.ws_url = f"wss://fstream.binance.com/stream?streams={streams}"

        logger.info(f"Last Trade WebSocket initialized for {len(symbols)} symbols")
        logger.info(f"WebSocket URL: {self.ws_url}")

    async def start(self):
        """Start WebSocket connection with auto-reconnect"""
        self.running = True
        logger.info("Starting Last Trade WebSocket...")

        while self.running:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    logger.warning("Last Trade WebSocket CONNECTED")

                    while self.running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=30)
                            await self._handle_message(message)
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            try:
                                pong = await websocket.ping()
                                await asyncio.wait_for(pong, timeout=10)
                            except Exception as e:
                                logger.error(f"Ping failed: {e}")
                                break
                        except Exception as e:
                            logger.error(f"Error receiving message: {e}")
                            break

            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")

            if self.running:
                logger.warning("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        logger.info("Last Trade WebSocket stopped")

    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)

            # WebSocket format: {"stream": "symbol@trade", "data": {...}}
            if 'data' in data:
                trade_data = data['data']
                symbol = trade_data.get('s')  # Symbol (e.g., "AVAXUSDT")
                price = trade_data.get('p')  # Last trade price

                if symbol and price:
                    # Store in Redis
                    key = f"last_trade_price:{symbol}"
                    self.redis_client.set(key, price)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

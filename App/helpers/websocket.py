import asyncio
import websockets
import json
import logging
import redis
from decimal import Decimal
from typing import Dict

logger = logging.getLogger(__name__)


class MarkPriceWebSocket:
    """WebSocket listener for Binance Futures mark prices - stores in Redis"""

    def __init__(self, symbols: list[str], redis_host: str = "localhost",
                 redis_port: int = 6379, redis_db: int = 3, redis_password: str = None):
        self.symbols = symbols
        self.ws_url = "wss://stream.binancefuture.com/ws"
        self.running = False
        self.websocket = None
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, password=redis_password, decode_responses=True)

    async def start(self):
        """Start WebSocket listener"""
        self.running = True
        logger.info(f"Starting mark price WebSocket for {len(self.symbols)} symbols")

        while self.running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self):
        """Stop WebSocket listener"""
        self.running = False
        if self.websocket:
            await self.websocket.close()

    async def _connect_and_listen(self):
        """Connect and process mark price messages"""
        streams = [f"{symbol.lower()}@markPrice@1s" for symbol in self.symbols]
        url = f"{self.ws_url}/{'/'.join(streams)}"

        logger.info(f"Connecting to WebSocket: {url}")

        async with websockets.connect(url) as ws:
            self.websocket = ws
            logger.info("Mark price WebSocket connected")

            async for message in ws:
                if not self.running:
                    break

                try:
                    data = json.loads(message)
                    await self._process_mark_price(data)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

    async def _process_mark_price(self, data: Dict):
        """Process incoming mark price and store in Redis"""
        try:
            symbol = data.get('s')
            mark_price = data.get('p')
            event_time = data.get('E')

            if not all([symbol, mark_price, event_time]):
                return

            # Store latest mark price in Redis with 10 second expiry
            key = f"mark_price:{symbol}"
            self.redis_client.setex(key, 10, str(mark_price))

        except Exception as e:
            logger.error(f"Error storing mark price: {e}")

    def get_mark_price(self, symbol: str) -> Decimal:
        """Get current mark price for symbol from Redis"""
        try:
            key = f"mark_price:{symbol}"
            mark_price_str = self.redis_client.get(key)

            if mark_price_str:
                return Decimal(mark_price_str)

            return Decimal('0')
        except Exception as e:
            logger.error(f"Error getting mark price from Redis: {e}")
            return Decimal('0')

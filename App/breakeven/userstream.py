import asyncio
import websockets
import json
import logging
from binance.client import Client
from typing import Dict

logger = logging.getLogger(__name__)


class UserDataStream:
    """Binance Futures User Data Stream - listens for order execution and position events"""

    def __init__(self, client: Client, strategies: Dict):
        self.client = client
        self.strategies = strategies  # {symbol: strategy_instance}
        self.listen_key = None
        self.running = False
        self.websocket = None

    async def start(self):
        """Start User Data Stream"""
        self.running = True

        while self.running:
            try:
                # Get listen key
                self.listen_key = self._get_listen_key()

                # Start keepalive task
                keepalive_task = asyncio.create_task(self._keepalive())

                # Connect and listen
                await self._connect_and_listen()

            except Exception as e:
                logger.error(f"User Data Stream error: {e}")
                if self.running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self):
        """Stop User Data Stream"""
        self.running = False
        if self.websocket:
            await self.websocket.close()
        if self.listen_key:
            try:
                self.client.futures_stream_close(listenKey=self.listen_key)
            except:
                pass

    def _get_listen_key(self) -> str:
        """Get listen key for User Data Stream"""
        try:
            response = self.client.futures_stream_get_listen_key()
            # Response can be either a dict or string depending on binance library version
            listen_key = response['listenKey'] if isinstance(response, dict) else response
            logger.info(f"Got listen key: {listen_key[:10]}...")
            return listen_key
        except Exception as e:
            logger.error(f"Error getting listen key: {e}")
            raise

    async def _keepalive(self):
        """Keep listen key alive - must be called every 30 minutes"""
        while self.running:
            await asyncio.sleep(30 * 60)  # 30 minutes
            try:
                if self.listen_key:
                    self.client.futures_stream_keepalive(listenKey=self.listen_key)
                    logger.info("Listen key keepalive sent")
            except Exception as e:
                logger.error(f"Keepalive error: {e}")

    async def _connect_and_listen(self):
        """Connect to User Data Stream and process events"""
        url = f"wss://stream.binancefuture.com/ws/{self.listen_key}"
        logger.info(f"Connecting to User Data Stream")

        async with websockets.connect(url) as ws:
            self.websocket = ws
            logger.info("User Data Stream connected")

            async for message in ws:
                if not self.running:
                    break

                try:
                    data = json.loads(message)
                    await self._process_event(data)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

    async def _process_event(self, data: Dict):
        """Process User Data Stream events"""
        try:
            event_type = data.get('e')

            if event_type == 'ORDER_TRADE_UPDATE':
                order_data = data.get('o', {})
                symbol = order_data.get('s')

                # Route to appropriate strategy
                if symbol in self.strategies:
                    self.strategies[symbol].handle_order_fill(order_data)

            elif event_type == 'ACCOUNT_UPDATE':
                # Handle position updates to get breakeven prices
                account_data = data.get('a', {})
                positions = account_data.get('P', [])

                for position in positions:
                    symbol = position.get('s')
                    if symbol in self.strategies:
                        self.strategies[symbol].update_position_breakeven(position)

        except Exception as e:
            logger.error(f"Error processing event: {e}")

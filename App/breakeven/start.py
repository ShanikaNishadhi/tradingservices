import asyncio
import logging
import os
import redis
from binance.client import Client
from dotenv import load_dotenv
from pathlib import Path
from logging.handlers import RotatingFileHandler

from App.breakeven.websocket import LastTradeWebSocket
from App.breakeven.userstream import UserDataStream
from App.breakeven.tradingpairs import trading_pairs
from App.breakeven.strategy import BreakevenStrategy
from App.breakeven.database import BreakevenDatabase

# Load environment
load_dotenv()

# Configure logging with file handler
log_file = Path(__file__).parent / 'breakeven.log'
file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Get root logger and add handlers
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)


async def main():
    """Main async entry point for Breakeven strategy"""

    # Initialize Binance client
    api_key = os.getenv('BINANCE_TESTNET_API_KEY')
    api_secret = os.getenv('BINANCE_TESTNET_SECRET_KEY')

    if not api_key or not api_secret:
        raise ValueError("BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_SECRET_KEY required")

    client = Client(api_key, api_secret, testnet=True)
    logger.info("Binance client initialized (TESTNET)")

    # Get enabled symbols
    enabled_symbols = [s for s in trading_pairs['symbols'] if s.get('enabled')]
    symbol_list = [s['symbol'] for s in enabled_symbols]

    logger.info(f"Enabled symbols: {symbol_list}")

    # Initialize Redis client (use 'redis' hostname in Docker, 'localhost' otherwise)
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_password = os.getenv('REDIS_PASSWORD')
    redis_client = redis.Redis(host=redis_host, port=6379, db=5, decode_responses=True, password=redis_password)
    logger.info(f"Redis client initialized (host={redis_host}, db=5)")

    # Initialize database
    db = BreakevenDatabase()

    # Initialize strategies
    strategies = {}
    for symbol_config in enabled_symbols:
        strategy = BreakevenStrategy(client, symbol_config, db, redis_client)
        strategy.initialize()
        strategies[symbol_config['symbol']] = strategy

    # Initialize WebSocket (writes to Redis)
    ws = LastTradeWebSocket(symbol_list, redis_host=redis_host, redis_port=6379, redis_db=5)

    # Initialize User Data Stream (listens for order fills and position updates)
    user_stream = UserDataStream(client, strategies)

    logger.info("=" * 60)
    logger.info("BREAKEVEN STRATEGY STARTING")
    logger.info("=" * 60)
    logger.info(f"Symbols: {len(symbol_list)}")
    logger.info("Price updates: Every 1 second (from WebSocket to Redis)")
    logger.info("Strategy checks: Every 1 second (polling Redis)")
    logger.info("  - LONG and SHORT run as separate async tasks")
    logger.info("Order fills: Real-time via User Data Stream")
    logger.info("TP Management: One TP per position direction (LONG/SHORT)")
    logger.info("=" * 60)

    # Create tasks for all services
    tasks = [
        asyncio.create_task(ws.start()),
        asyncio.create_task(user_stream.start())
    ]

    # Strategy tasks (one per symbol)
    for strategy in strategies.values():
        tasks.append(asyncio.create_task(strategy.run()))

    try:
        # Run all tasks concurrently
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Shutting down...")

        # Stop all strategies
        for strategy in strategies.values():
            await strategy.stop()

        # Stop WebSocket and User Data Stream
        await ws.stop()
        await user_stream.stop()

        logger.info("All services stopped")


if __name__ == '__main__':
    asyncio.run(main())

import asyncio
import logging
import os
import redis
from binance.client import Client
from dotenv import load_dotenv
from pathlib import Path
from logging.handlers import RotatingFileHandler

from App.advancedsimpletrends.websocket import MarkPriceWebSocket
from App.advancedsimpletrends.userstream import UserDataStream
from App.advancedsimpletrends.tradingpairs import trading_pairs
from App.advancedsimpletrends.strategy import AdvancedSimpleTrendsStrategy
from App.advancedsimpletrends.database import AdvancedSimpleTrendsDatabase

# Load environment
load_dotenv()

# Configure logging with separate files for INFO and WARNING+ levels
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Main log file - WARNING and above only (signals, errors, important events)
main_log_file = Path(__file__).parent / 'advancedsimpletrends.log'
main_file_handler = RotatingFileHandler(main_log_file, maxBytes=10*1024*1024, backupCount=5)
main_file_handler.setLevel(logging.WARNING)
main_file_handler.setFormatter(formatter)

# Info log file - INFO level only (periodic refreshes, cache updates, keepalives)
info_log_file = Path(__file__).parent / 'advancedsimpletrends_info.log'
info_file_handler = RotatingFileHandler(info_log_file, maxBytes=10*1024*1024, backupCount=5)
info_file_handler.setLevel(logging.INFO)
info_file_handler.addFilter(lambda record: record.levelno == logging.INFO)  # Only INFO, not WARNING+
info_file_handler.setFormatter(formatter)

# Console handler - WARNING and above
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)

# Get root logger and add handlers
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(main_file_handler)  # WARNING+
root_logger.addHandler(info_file_handler)  # INFO only
root_logger.addHandler(console_handler)    # WARNING+

logger = logging.getLogger(__name__)


async def main():
    """Main async entry point for AdvancedSimpleTrends strategy"""

    # Initialize Binance client
    api_key = os.getenv('BINANCE_SUB_API_KEY')
    api_secret = os.getenv('BINANCE_SUB_SECRET_KEY')

    if not api_key or not api_secret:
        raise ValueError("BINANCE_SUB_API_KEY and BINANCE_SUB_SECRET_KEY required")

    client = Client(api_key, api_secret, testnet=False)
    logger.info("Binance client initialized (SUBACCOUNT - MAINNET)")

    # Get enabled symbols
    enabled_symbols = [s for s in trading_pairs['symbols'] if s.get('enabled')]
    symbol_list = [s['symbol'] for s in enabled_symbols]

    logger.info(f"Enabled symbols: {symbol_list}")

    # Initialize Redis client (use 'redis' hostname in Docker, 'localhost' otherwise)
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_password = os.getenv('REDIS_PASSWORD')
    redis_client = redis.Redis(host=redis_host, port=6379, db=6, password=redis_password, decode_responses=True)
    logger.info(f"Redis client initialized (host={redis_host}, db=6 - ADVANCEDSIMPLETRENDS)")

    # Initialize database
    db = AdvancedSimpleTrendsDatabase()

    # Initialize strategies
    strategies = {}
    for symbol_config in enabled_symbols:
        strategy = AdvancedSimpleTrendsStrategy(client, symbol_config, db, redis_client)
        strategy.initialize()
        strategies[symbol_config['symbol']] = strategy

    # Initialize WebSocket (writes to Redis)
    ws = MarkPriceWebSocket(symbol_list, redis_host=redis_host, redis_port=6379, redis_db=6, redis_password=redis_password)

    # Initialize User Data Stream (listens for order fills)
    user_stream = UserDataStream(client, strategies)

    logger.info("=" * 60)
    logger.info("ADVANCED SIMPLETRENDS STRATEGY STARTING")
    logger.info("=" * 60)
    logger.info(f"Symbols: {len(symbol_list)}")
    logger.info("Architecture: PNLGap (rare, large thresholds) + SimpleTrends (primary)")
    logger.info("Price updates: Every 1 second (from WebSocket to Redis)")
    logger.info("Strategy checks: Every 1 second (polling Redis)")
    logger.info("Order fills: Real-time via User Data Stream")
    logger.info("=" * 60)

    # Create tasks for all services
    tasks = [
        asyncio.create_task(ws.start()),
        asyncio.create_task(user_stream.start()),
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

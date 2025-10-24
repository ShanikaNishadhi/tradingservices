import os
import requests
import logging
import psycopg2
from datetime import datetime

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_NEW_COIN_LISTING_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_NEW_COIN_LISTING_CHAT_ID')

def save_to_database(coin, market, trading_start, source, url):
    """Save new coin listing to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO new_coin_listings (coin, market, trading_start, source, url, reported_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (coin, market, trading_start, source, url, datetime.now()))

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Saved {coin} listing to database")
        return True

    except Exception as e:
        logger.error(f"Failed to save to database: {e}")
        return False

def send_telegram_message(coin, market, trading_start, url, source='Binance'):
    """Send formatted message to Telegram and save to database"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not configured")
        return False

    message = f"""
New {source} Listing Alert

Coin: {coin}
Market: {market}
Trading Start: {trading_start}

Details: {url}
"""

    try:
        # Send Telegram notification
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message.strip()
            },
            timeout=5
        )
        response.raise_for_status()
        logger.info(f"Sent Telegram notification for {coin}")

        # Save to database
        save_to_database(coin, market, trading_start, source, url)

        return True

    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
        return False

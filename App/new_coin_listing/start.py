import os
import time
import asyncio
import logging
from datetime import datetime
from scraper import fetch_latest_listing
from parser import parse_announcement
from notifier import send_telegram_message

log_dir = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'new_coin_listing.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CHECK_INTERVAL = 2 * 60 * 60
last_id = None

async def check_new_listings():
    global last_id
    logger.info("Checking for new listings...")

    listing = await fetch_latest_listing()
    if not listing:
        logger.warning("No listing fetched")
        return

    # On first run or if new listing found
    if not last_id or listing['id'] != last_id:
        logger.info(f"New listing found: {listing['url']}")

        logger.info("Parsing announcement with ChatGPT...")
        details = parse_announcement(listing['content'])
        if details:
            logger.info(f"Sending Telegram notification for {details['coin']}")
            success = send_telegram_message(
                details['coin'],
                details['market'],
                details['trading_start'],
                listing['url']
            )
            if success:
                logger.info("Notification sent successfully")
                last_id = listing['id']
            else:
                logger.error("Failed to send notification - will retry on next cycle")
        else:
            logger.error("Failed to parse announcement details - will retry on next cycle")
    else:
        logger.info("No new listings found")

async def main():
    logger.info("New Coin Listing Monitor started")

    while True:
        try:
            await check_new_listings()
        except Exception as e:
            logger.error(f"Error in check cycle: {e}")

        logger.info(f"Sleeping for {CHECK_INTERVAL/3600} hours...")
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())

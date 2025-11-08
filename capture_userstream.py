#!/usr/bin/env python3
"""
Standalone script to capture Binance Futures User Data Stream events
Saves all ACCOUNT_UPDATE events to JSON file for analysis
"""

import asyncio
import websockets
import json
import os
from datetime import datetime
from binance.client import Client
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Output file
OUTPUT_FILE = "userstream_captures.jsonl"

class UserStreamCapture:
    def __init__(self):
        api_key = os.getenv('BINANCE_SUB_API_KEY')
        api_secret = os.getenv('BINANCE_SUB_SECRET_KEY')

        if not api_key or not api_secret:
            raise ValueError("BINANCE_SUB_API_KEY and BINANCE_SUB_SECRET_KEY required in .env")

        self.client = Client(api_key, api_secret, testnet=False)
        self.listen_key = None
        self.running = False

    def get_listen_key(self):
        """Get listen key for User Data Stream"""
        response = self.client.futures_stream_get_listen_key()
        listen_key = response['listenKey'] if isinstance(response, dict) else response
        print(f"✓ Got listen key: {listen_key[:10]}...")
        return listen_key

    async def keepalive(self):
        """Keep listen key alive - every 30 minutes"""
        while self.running:
            await asyncio.sleep(30 * 60)  # 30 minutes
            try:
                self.client.futures_stream_keepalive(listenKey=self.listen_key)
                print(f"[{datetime.now()}] Listen key keepalive sent")
            except Exception as e:
                print(f"Keepalive error: {e}")

    async def capture(self):
        """Capture User Data Stream events"""
        self.running = True
        self.listen_key = self.get_listen_key()

        # Start keepalive task
        keepalive_task = asyncio.create_task(self.keepalive())

        url = f"wss://fstream.binance.com/ws/{self.listen_key}"
        print(f"✓ Connecting to User Data Stream...")
        print(f"✓ Writing events to: {OUTPUT_FILE}")
        print(f"✓ Press Ctrl+C to stop\n")
        print("=" * 80)

        try:
            async with websockets.connect(url) as ws:
                print(f"✓ Connected! Listening for events...\n")

                async for message in ws:
                    if not self.running:
                        break

                    try:
                        data = json.loads(message)
                        event_type = data.get('e')

                        # Timestamp the capture
                        capture_time = datetime.now().isoformat()

                        # Save ALL events to file
                        with open(OUTPUT_FILE, 'a') as f:
                            capture_record = {
                                'captured_at': capture_time,
                                'event': data
                            }
                            f.write(json.dumps(capture_record) + '\n')

                        # Print summary to console
                        if event_type == 'ACCOUNT_UPDATE':
                            print(f"[{capture_time}] ACCOUNT_UPDATE")

                            # Show positions
                            positions = data.get('a', {}).get('P', [])
                            for pos in positions:
                                symbol = pos.get('s')
                                pa = pos.get('pa')
                                ep = pos.get('ep')
                                bep = pos.get('bep')  # ← THIS IS WHAT WE'RE LOOKING FOR!

                                if float(pa) != 0:
                                    print(f"  {symbol}: pa={pa}, ep={ep}, bep={bep}")

                            print(f"  → Saved to {OUTPUT_FILE}")
                            print()

                        elif event_type == 'ORDER_TRADE_UPDATE':
                            order = data.get('o', {})
                            symbol = order.get('s')
                            side = order.get('S')
                            status = order.get('X')
                            avg_price = order.get('ap')

                            print(f"[{capture_time}] ORDER_TRADE_UPDATE")
                            print(f"  {symbol} {side} {status} @ {avg_price}")
                            print(f"  → Saved to {OUTPUT_FILE}")
                            print()

                        else:
                            # Other events
                            print(f"[{capture_time}] {event_type}")

                    except Exception as e:
                        print(f"Error processing message: {e}")

        except KeyboardInterrupt:
            print("\n\n✓ Stopping...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.running = False
            try:
                self.client.futures_stream_close(listenKey=self.listen_key)
            except:
                pass
            print("✓ Disconnected")

async def main():
    print("=" * 80)
    print("BINANCE FUTURES USER DATA STREAM CAPTURE")
    print("=" * 80)
    print()

    capture = UserStreamCapture()
    await capture.capture()

if __name__ == '__main__':
    asyncio.run(main())

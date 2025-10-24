import os
import json
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

def parse_announcement(content):
    """Extract coin, market type, and trading start time using ChatGPT"""
    try:
        text = content[:3000] if content else ""

        prompt = f"""Extract the following from this Binance announcement:
1. Coin symbol (e.g., BTC, ETH, ASTER)
2. Market type details:
   - If Spot: just say "Spot"
   - If Alpha: just say "Alpha"
   - If Futures: specify "USD-M Perpetual", "USD-M Quarterly", "COIN-M Perpetual", or "COIN-M Quarterly"
3. Trading start date and time

Content: {text}

Respond ONLY with JSON format:
{{"coin": "SYMBOL", "market": "TYPE", "trading_start": "YYYY-MM-DD HH:MM UTC"}}"""

        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENAI_API_KEY}'},
            json={
                'model': 'gpt-4o-mini',
                'messages': [{'role': 'user', 'content': prompt}],
                'temperature': 0
            },
            timeout=30
        )

        data = response.json()
        if 'error' in data:
            logger.error(f"OpenAI API error: {data['error']}")
            return None

        result = data['choices'][0]['message']['content']

        # Clean JSON if wrapped in markdown
        if result.startswith('```'):
            result = result.split('```')[1]
            if result.startswith('json'):
                result = result[4:]

        return json.loads(result.strip())

    except Exception as e:
        logger.error(f"Error parsing announcement: {e}")
        return None

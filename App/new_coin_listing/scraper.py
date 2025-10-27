import logging
import asyncio
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

async def fetch_latest_listing():
    """Go to listing page, click first article, and return content with metadata"""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process'
                ]
            )

            context = await browser.new_context(
                proxy={
                    'server': 'http://142.111.67.146:5611',
                    'username': 'makkrbjv',
                    'password': '164txeju4p4h'
                },
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )

            page = await context.new_page()

            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = {runtime: {}};
            """)

            await page.goto('https://www.binance.com/en/support/announcement/list/48', wait_until='domcontentloaded')
            await asyncio.sleep(5)

            await page.evaluate('''() => {
                const text = document.body.innerText;
                const match = text.match(/Binance.*?(?=\\d{4}-\\d{2}-\\d{2})/);
                if (match) {
                    const elements = Array.from(document.querySelectorAll('a, div, span'));
                    for (const el of elements) {
                        if (el.innerText && el.innerText.includes(match[0].trim())) {
                            if (el.tagName === 'A' || el.closest('a')) {
                                const link = el.tagName === 'A' ? el : el.closest('a');
                                link.click();
                                break;
                            }
                        }
                    }
                }
            }''')

            await asyncio.sleep(5)

            url = page.url
            content = await page.evaluate('() => document.body.innerText')

            # Extract ID from URL
            article_id = url.split('/')[-1] if '/' in url else None

            await browser.close()

            return {
                'id': article_id,
                'url': url,
                'content': content
            }

    except Exception as e:
        logger.error(f"Error fetching listing: {e}")
        return None
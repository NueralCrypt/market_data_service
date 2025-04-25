#!/usr/bin/env python3

import logging
import aiohttp
import pandas as pd
import asyncio
import pika
from prometheus_client import start_http_server, Counter

# Prometheus metrics
MARKET_DATA_FETCHED = Counter('market_data_fetched', 'Total market data fetched')

class MarketDataService:
    def __init__(self):
        self.base_url = "https://api.gmx.io"
        self.logger = logging.getLogger(__name__)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='market_data')

    async def fetch_data(self, symbol: str, timeframe: str):
        """Fetch OHLCV data from GMX."""
        try:
            async with aiohttp.ClientSession() as session:
                # Fetch OHLCV data
                url = f"{self.base_url}/candles?symbol={symbol}&timeframe={timeframe}"
                async with session.get(url) as response:
                    data = await response.json()
                    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    return df
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_http_server(8000)  # Expose metrics on port 8000
    service = MarketDataService()
    asyncio.run(service.fetch_data("BTCUSD", "1m"))
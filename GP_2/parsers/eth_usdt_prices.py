"""
Ответ бинанса приходит в следующем формате (взято из доков):
[
    [
        1499040000000,         // Kline open time — время начала дня, нам надо
        "0.01634790",          // Open price  — цена, нам надо
        "0.80000000",          // High price
        "0.01575800",          // Low price
        "0.01577100",          // Close price
        "148976.11427815",     // Volume
        1499644799999,         // Kline Close time
        "2434.19055334",       // Quote asset volume
        308,                   // Number of trades
        "1756.87402397",       // Taker buy base asset volume
        "28.46694368",         // Taker buy quote asset volume
        "0"                    // Unused field, ignore.
    ]
]
"""
import csv
import logging
import time
from config import *
import httpx
from datetime import datetime


logging.basicConfig(
    filename=LOGS_DIR / "eth_usdt_prices.log",
    format="[%(levelname)-5s/%(asctime)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATASET_FILE = DATASETS_DIR / "ETHUSDT_1d.csv"
BASE_URL = "https://api.binance.com"

def fetch_klines(
    session: httpx.Client,
    start_time: int,
    limit: int = 1000
) -> list[list[int | str]]:
    params = {
        "symbol": "ETHUSDT",
        "interval": "1d",
        "startTime": start_time,
        "limit": limit
    }
    url = "/api/v3/klines"
    for attempt in range(3):
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == 2:
                logger.error("Не удалось получить историю цены", exc_info=True)
                raise e
            logger.warning(f"Ошибка запроса BINANCE #{attempt + 1}: {e}")


def save_price_history():
    with open(DATASET_FILE, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(("open_time_ms", "open_price"))

    session = httpx.Client(
        base_url=BASE_URL,
        headers={"X-MBX-APIKEY": BINANCE_API_KEY}
    )
    start_time = int(datetime.strptime("01.01.2020", "%d.%m.%Y").timestamp() * 1000)
    while True:
        klines = fetch_klines(session, start_time)
        if not klines:
            return
        with open(DATASET_FILE, 'a') as f:
            writer = csv.writer(f)
            writer.writerows(map(lambda l: l[:2], klines))

        start_time = klines[-1][0] + 1
        last_saved_date = datetime.fromtimestamp(start_time // 1000).strftime("%d.%m.%Y")
        logger.info(f"Успешно сохранено {len(klines)} строк. Последняя сохраненная дата: {last_saved_date}")
        

if __name__ == "__main__":
    save_price_history()
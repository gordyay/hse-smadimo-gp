"""
Формат ответа (из доков):
{
  "success": true,
  "result": [
    {
      "unix_timestamp": 1663789595,
      "slot": 151721528,
      "tx_signature": "2D9VPjN93j7YTx13oHN9sL2RDHbytLMfNmxTCBc8a57faomn4e2iF2QyUU1DLfdA9FYEJq1SzXmaC8p9FntLckUL",
      "domain_name": "meggadao",
      "domain_key": "FoidaZVWPYNCgRkthdJqnSQ82x7SLkSpBAypR7RVtFNU",
      "domain_auction_key": "HmGENkrhkA7ekmj9kKni4CJLJyifjzohPAV1wWTWuQFX",
      "domain_token_mint": "ExankJNcWwJoS4ZYe5Xuw8r7ioqAcg5XkbzWT6NJhsiA",
      "price": 48.769577,
      "quote_mint": "EchesyfXePKdLtoiZSL8pBe8Myagyy8ZRqsACNCFGnvp",
      "usd_price": 20.184021
    }
  ]
}
"""

import csv
import logging
import time
import httpx
from config import *


logging.basicConfig(
    filename=LOGS_DIR / "sol_registrations.log",
    format="[%(levelname)-5s/%(asctime)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATASET_FILE = DATASETS_DIR / "sol_domains_registrations.csv"
BASE_URL = "https://sns-api.bonfida.com"
CSV_FIELDS = ["unix_timestamp", "tx_signature", "domain_name", "domain_key", "usd_price", "bidder_key"]
RESULTS_LIMIT = 500

def fetch_registrations(
    session: httpx.Client,
    start_time: int,
    end_time: int,
) -> list[dict[str, str | int]]:
    params = {
        "start_time": start_time,
        "end_time": end_time,
        "limit": RESULTS_LIMIT
    }
    url = "/sales/registrations"
    for attempt in range(5):
        try:
            response = session.get(url, params=params)
            if response.status_code == 429:
                retry_after = response.headers.get("retry-after")
                logging.warning(f"429, повторная отправка запроса через: {retry_after} с.")
                time.sleep(int(retry_after))
                continue

            res_json: dict = response.json()
            if not res_json.get("success"):
                time.sleep(2 ** attempt)
                continue
            
            return res_json["result"]

        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == 4:
                logger.error(f"Не удалось получить историю регистраций", exc_info=True)
                raise e
            logger.warning(f"Ошибка запроса SNS BONFIDA API #{attempt + 1}: {e}")


def save_registrations_history():
    session = httpx.Client(base_url=BASE_URL)
    
    # Проблема: результаты выдаются не понятно вообще в каком порядке, надо смотерть все возможные time windows бинпоиском
    stack = [(int(time.time()) - 365 * 24 * 3600 * 10, int(time.time()))]
    results = {}
    i = 0
    while stack:
        start_time, end_time = stack.pop(0)
        registrations = fetch_registrations(session, start_time, end_time)
        if not registrations:
            continue
        for registration in registrations:
            results[registration["tx_signature"] + registration["domain_name"]] = registration
        
        n_registrations = len(registrations)
        if n_registrations == RESULTS_LIMIT:  # в данном time window могут быть пропущенные элементы
            mid = (start_time + end_time) // 2
            stack.extend([(start_time, mid), (mid, end_time)])
        
        if i % 1000 == 0:
            with open(DATASET_FILE, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(sorted(results.values(), key=lambda l: l["unix_timestamp"]))
        i += 1
        if i % 10 == 0:
            logger.info(f"[Итерация #{i}] Успешно получено {n_registrations} строк. Всего элементов: {len(results)}")
            time.sleep(0.1)

    with open(DATASET_FILE, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(sorted(results.values(), key=lambda l: l["unix_timestamp"]))


if __name__ == "__main__":
    save_registrations_history()

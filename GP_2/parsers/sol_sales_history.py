"""
Формат ответа (из доков):
{
  "data": [
    {
      "operation": 1,
      "unix_timestamp": 1622592000,
      "tx_signature": "27EjmB4NdsRKMNkeYeF4rva...",
      "domain_key": "4cQ3zUeardJweGTnk...",
      "pre_tx_owner": "5fEPywJMxeP2HBo7JyBUv1G...",
      "post_tx_owner": "CUcYT9ZoBXET88o...",
      "transaction_type": 1,
      "usd_price": 152,
      "price": 1,
      "quote_mint": "So11111111111111111111111111111111111111112"
    }
  ],
  "last_token": "1622592000:abcdef1234567890:domain1"  // timestamp:tx_signature:domain_key
}

enum Operation {
  Create, // registration (0)
  Transfer, // sale or transfer (1)  — нам надо вот это
  Update, // update data in a name record (2)
  Delete, // delete or burn a domain (3)
  Realloc, // change the domain storage size (4)
}
"""

import csv
import logging
import time
import httpx
from config import *


logging.basicConfig(
    filename=LOGS_DIR / "sol_sales.log",
    format="[%(levelname)-5s/%(asctime)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATASET_FILE = DATASETS_DIR / "sol_domains_sales.csv"
BASE_URL = "https://sns-api.bonfida.com"
CSV_FIELDS = ["unix_timestamp", "tx_signature", "domain_key", "usd_price", "bidder_key"]
RESULTS_LIMIT = 200

def fetch_history(
    session: httpx.Client,
    last_token: str = None,
) -> tuple[list[dict[str, str | int]], str]:
    params = {
        "limit": RESULTS_LIMIT,
        "order_by": "TimeDesc"
    }
    if last_token:
        params["last_token"] = last_token,

    url = "/v2/domains/history"
    for attempt in range(5):
        try:
            response = session.get(url, params=params)
            if response.status_code == 429:
                retry_after = response.headers.get("retry-after")
                logging.warning(f"429, повторная отправка запроса через: {retry_after} с.")
                time.sleep(int(retry_after))
                continue
            
            res_json = response.json()
            return res_json["data"], res_json["last_token"]

        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == 4:
                logger.error(f"Не удалось получить историю регистраций", exc_info=True)
                raise e
            logger.warning(f"Ошибка запроса SNS BONFIDA API #{attempt + 1}: {e}")


def save_sales_history(last_token: str = None):
    session = httpx.Client(base_url=BASE_URL)
    
    i = 0
    total_results = 0
    res = []
    while True:
        transactions, last_token = fetch_history(session, last_token)
        if not transactions:
            break
        
        is_purchase = lambda l: l["operation"] == 1 and l.get("usd_price") and l.get("pre_tx_owner") != l.get("post_tx_owner")
        transactions = list(filter(is_purchase, transactions))
        res.extend(transactions)
        
        i += 1
        if i % 100 == 0:
            with open(DATASET_FILE, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
                writer.writerows(res)
                res = []
        total_results += len(transactions)
        if i % 10 == 0:
            logger.info(f"[Итерация #{i}] Успешно получено {len(transactions)} строк. Всего элементов: {total_results}")
            time.sleep(0.1)
    
    if res:
        with open(DATASET_FILE, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
            writer.writerows(res)
    

if __name__ == "__main__":
    last_token = None
    if os.path.exists(DATASET_FILE):
        with open(DATASET_FILE, 'r') as f:
            r = csv.DictReader(f, CSV_FIELDS)
            r = list(r)
            if r:
                last_line = r[-1]
                last_token = f"{last_line['unix_timestamp']}:{last_line['tx_signature']}:{last_line['domain_key']}"
    if not last_token:
        with open(DATASET_FILE, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction='ignore')
            writer.writeheader()
    save_sales_history(last_token)

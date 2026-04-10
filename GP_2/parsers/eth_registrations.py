"""
Формат ответа TheGraph ENS Subgraph:
{
  "data": {
    "registrations": [
      {
        "id": "0x...",
        "registrationDate": "1622592000",
        "cost": "5000000000000000",  // wei (eth * 1e18)
        "registrant": { "id": "0x..." },
        "domain": {
          "id": "0x...",
          "name": "example.eth",
          "labelName": "example",
          "labelhash": "0x..."
        }
      }
    ]
  }
}
"""

import csv
import logging
import time
import httpx
from decimal import Decimal
from config import *


logging.basicConfig(
    filename=LOGS_DIR / "eth_registrations.log",
    format="[%(levelname)-5s/%(asctime)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATASET_FILE = DATASETS_DIR / "eth_domains_registrations.csv"
CHECKPOINT_FILE = DATASETS_DIR / "eth_domains_registrations_checkpoint.txt"
BASE_URL = "https://api.thegraph.com"
CSV_FIELDS = ["registration_date", "domain_name", "price", "namehash", "registrant_address", "labelhash_hex"]
RESULTS_LIMIT = 1000

QUERY_TEMPLATE = """{
  registrations(
    first: %d,
    orderBy: id,
    orderDirection: asc,
    where: { id_gt: "%s" }
  ) {
    id
    registrationDate
    cost
    registrant { id }
    domain {
      id
      name
      labelName
      labelhash
    }
  }
}"""


def fetch_registrations(
    session: httpx.Client,
    last_id: str,
) -> list[dict]:
    url = "/subgraphs/name/ensdomains/ens"
    query = QUERY_TEMPLATE % (RESULTS_LIMIT, last_id)

    for attempt in range(5):
        try:
            response = session.post(url, json={"query": query})
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data["data"]["registrations"]
        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == 4:
                logger.error("Не удалось получить регистрации", exc_info=True)
                raise e
            logger.warning(f"Ошибка запроса TheGraph #{attempt + 1}: {e}")


def save_registrations_history(last_id: str = "0x0"):
    session = httpx.Client(base_url=BASE_URL)

    i = 0
    total_results = 0
    res = []

    while True:
        registrations = fetch_registrations(session, last_id)
        if not registrations:
            break

        for r in registrations:
            if not r["domain"].get("labelName"):  # домены типа [000000000000000000000000000000000000000000000000000000000000d1ce].eth
                continue
            res.append({
                "registration_date": int(r["registrationDate"]),
                "domain_name": r["domain"]["name"],
                "price": round(float(Decimal(r["cost"]) / Decimal(10**18)), 18) if r["cost"] else None,
                "namehash": r["domain"]["id"],
                "registrant_address": r["registrant"]["id"],
                "labelhash_hex": r["domain"]["labelhash"],
            })

        last_id = registrations[-1]["id"]
        total_results += len(registrations)
        i += 1

        if i % 50 == 0 and res:
            with open(DATASET_FILE, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writerows(res)
            res = []
            with open(CHECKPOINT_FILE, 'w') as f:
                f.write(last_id)

        if i % 10 == 0:
            logger.info(f"[Итерация #{i}] Получено {len(registrations)} строк. Всего: {total_results}")

        if len(registrations) < RESULTS_LIMIT:
            break
        time.sleep(2)  # у этого эндпоинта достаточно жесткие рейт лимиты

    if res:
        with open(DATASET_FILE, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerows(res)
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(last_id)

    logger.info(f"Выполнение скрипта завершено. Всего элементов: {total_results}")


if __name__ == "__main__":
    last_id = None
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            last_id = f.read().strip()
            logging.info(f"Скрипт начал выполняться с last_id = {last_id}")

    if not last_id:
        last_id = "0x0"
        with open(DATASET_FILE, 'w') as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
    
    save_registrations_history(last_id)

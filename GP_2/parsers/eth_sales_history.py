"""
Формат ответа execution/{id}/results:
{
  "result": {
    "rows": [
      {
        "unix_timestamp": 1682439683,
        "tx_hash": "0x...",
        "namehash": "0x...",
        "price_usd": 18185.1
      }
    ]
  },
  "next_offset": 1000
}
"""

import csv
import logging
import time
import httpx

from config import *


logging.basicConfig(
    filename=LOGS_DIR / "eth_sales_dune.log",
    format="[%(levelname)-5s/%(asctime)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL = "https://api.dune.com"
RESULTS_LIMIT = 1000

QUERY_TEMPLATE = """
WITH matched_sales AS (
    SELECT
        CAST(to_unixtime(t.block_time) AS BIGINT) AS unix_timestamp,
        lower(concat('0x', to_hex(t.tx_hash))) AS tx_hash,
        lower(concat(
            '0x',
            lpad(to_hex(varbinary_ltrim(CAST(x.token_id AS varbinary))), 64, '0')
        )) AS {hash_type},
        CAST(t.amount_usd AS DOUBLE) AS price_usd,
        count(*) OVER (PARTITION BY t.unique_trade_id) AS transfer_matches
    FROM nft.trades t
    JOIN nft.transfers x
        ON t.blockchain = x.blockchain
       AND t.tx_hash = x.tx_hash
       AND t.nft_contract_address = x.contract_address
       AND t.token_standard = x.token_standard
       AND t.seller = x."from"
       AND t.buyer = x."to"
    WHERE t.blockchain = 'ethereum'
      AND t.evt_type = 'Trade'
      AND t.number_of_items = UINT256 '1'
      AND t.nft_contract_address = {contract_address}
)
SELECT unix_timestamp, tx_hash, {hash_type}, price_usd
FROM matched_sales
WHERE transfer_matches = 1
ORDER BY unix_timestamp ASC
"""


def dune_request(
    session: httpx.Client,
    method: str,
    url: str,
    **kwargs,
) -> dict:
    for attempt in range(5):
        try:
            response = session.request(method, url, **kwargs)
            if response.status_code == 429:
                retry_after = 2 ** (attempt + 3)
                logger.warning(f"429, повторная отправка запроса через: {retry_after} с.")
                time.sleep(retry_after)
                continue
            if response.status_code == 402:
                raise RuntimeError("HTTP 402. Скорее всего, некорректный API-ключ")

            response.raise_for_status()
            return response.json()

        except httpx.RemoteProtocolError as e:
            logger.warning(f"Dune разорвал соединение (попытка #{attempt + 1}): {e}")         
            session._transport.close()

        except Exception as e:
            time.sleep(2 ** attempt)
            if attempt == 4:
                logger.error("Не удалось получить ответ от Dune", exc_info=True)
                raise e
            logger.warning(f"Ошибка запроса Dune #{attempt + 1}: {e}")


def execute_query(
    session: httpx.Client,
    query: str
) -> str:
    response = dune_request(
        session,
        "POST",
        "/api/v1/sql/execute",
        json={"sql": query, "performance": "medium"},
    )
    execution_id = response.get("execution_id")
    if not execution_id:
        raise RuntimeError(f"В ответе Dune нет execution_id: {response}")
    logger.info(f"Отправлен SQL запрос с execution_id {execution_id}")
    return execution_id


def wait_execution(
    session: httpx.Client,
    execution_id: str,
) -> None:
    while True:
        response = dune_request(
            session,
            "GET",
            f"/api/v1/execution/{execution_id}/status",
        )
        state = response.get("state")
        if state == "QUERY_STATE_COMPLETED":
            logger.info(f"SQL запрос с execution_id {execution_id} выполнен")
            return
        if state == "QUERY_STATE_COMPLETED_PARTIAL":
            continue
        if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELED", "QUERY_STATE_EXPIRED"):
            raise RuntimeError(f"Выполнение SQL завершилось со статусом {state}: {response}")
        time.sleep(3)


def fetch_sales_page(
    session: httpx.Client,
    execution_id: str,
    offset: int,
) -> tuple[list[dict], int | None]:
    response = dune_request(
        session,
        "GET",
        f"/api/v1/execution/{execution_id}/results",
        params={
            "limit": RESULTS_LIMIT,
            "offset": offset,
        }
    )
    return response["result"]["rows"], response.get("next_offset")


def save_sales_history(sales_type: str) -> None:
    session = httpx.Client(base_url=BASE_URL, timeout=60, headers={"X-Dune-Api-Key": DUNE_API_KEY})
    if sales_type == "base":
        hash_type = "labelhash"
        contract_address = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"
    else:
        contract_address = "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401"
        hash_type = "namehash"
    output_file = DATASETS_DIR / f"eth_domains_sales_{sales_type}.csv"
    csv_fields = ["unix_timestamp", "tx_hash", hash_type, "price_usd"]

    query = QUERY_TEMPLATE.format(contract_address=contract_address, hash_type=hash_type)
    execution_id = execute_query(session, query)
    wait_execution(session, execution_id)

    with open(output_file, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()

    offset = 0
    i = 0
    total_results = 0
    res = []

    while True:
        rows, next_offset = fetch_sales_page(session, execution_id, offset)
        if not rows:
            break

        res.extend(rows)
        total_results += len(rows)
        i += 1

        if i % 10 == 0:
            with open(output_file, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=csv_fields)
                writer.writerows(res)
            res = []

        if i % 10 == 0:
            logger.info(f"[{sales_type} Итерация #{i}]. Получено {len(rows)} строк. Всего: {total_results}")

        if len(rows) < RESULTS_LIMIT or not next_offset:
            break
        offset = next_offset
        time.sleep(5)

    if res:
        with open(output_file, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields)
            writer.writerows(res)

    logger.info(f"[{sales_type}] Выполнение скрипта завершено. Всего элементов: {total_results}")
    session.close()


if __name__ == "__main__":
    if not DUNE_API_KEY:
        raise RuntimeError("Необходим API ключ Dune (иначе будет 402 payment required)")

    
    for sales_type in ("base", "wrapped"):
        save_sales_history(sales_type)

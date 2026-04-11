import csv
import logging
import re
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from config import *


logging.basicConfig(
    filename=LOGS_DIR / "ton_history.log",
    format="[%(levelname)-5s/%(asctime)s] %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATASET_FILE = DATASETS_DIR / "ton_domains_sales.csv"
PAGE_URL = "https://webdom.market/analytics/history"
CSV_FIELDS = ["tx_time", "domain_name", "price_ton", "sale_type"]


def make_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1500,900")
    # options.add_argument("--headless=new")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def setup_filters(driver):
    # Выбираем зону .ton
    driver.execute_script("arguments[0].click();", driver.find_element(
        By.XPATH, "//label[.//span[normalize-space(.)='.ton']]"
    ))
    time.sleep(0.5)

    # Выключаем фильтр Only webdom, если включён
    switch = driver.find_element(By.CSS_SELECTOR, "input[role='switch']")
    if driver.execute_script("return arguments[0].checked;", switch):
        label = driver.find_element(By.CSS_SELECTOR, f"label[for='{switch.get_attribute('id')}']")
        driver.execute_script("arguments[0].click();", label)
        time.sleep(0.5)

    # Очищаем фильтры событий, оставляем только покупки
    clear_buttons = driver.find_elements(
        By.XPATH,
        "//button[normalize-space(.)='Clear' or normalize-space(.)='Очистить']",
    )
    if clear_buttons:
        driver.execute_script("arguments[0].click();", clear_buttons[0])
        time.sleep(0.5)

    for value in ("primary_purchase", "secondary_purchase"):
        cb = driver.find_element(By.CSS_SELECTOR, f"input[type='checkbox'][value='{value}']")
        if not driver.execute_script("return arguments[0].checked;", cb):
            label = driver.find_element(By.CSS_SELECTOR, f"label[for='{cb.get_attribute('id')}']")
            driver.execute_script("arguments[0].click();", label)
            time.sleep(0.5)

    time.sleep(3)


def parse_row(cells):
    links = cells[1].find_elements(By.CSS_SELECTOR, "a[href*='/domain/']")
    if not links:
        return None
    href = links[0].get_attribute("href") or ""
    if "/domain/" not in href:
        return None
    domain = href.split("/domain/")[-1].strip("/")
    if not domain.lower().endswith(".ton"):
        return None

    price_text = cells[2].text.replace("\xa0", " ").strip()
    if "TON" not in price_text.upper():
        return None
    m = re.search(r"[\d\s.,]+", price_text)
    if not m:
        return None
    price = float(m.group(0).replace(" ", ""))

    # primary если в from есть ton dns
    sale_type = "primary" if "ton dns" in {cells[3].text} else "secondary"
    tx_time = int(datetime.strptime(cells[5].text.strip(), "%d.%m.%Y, %H:%M").timestamp())

    return {
        "tx_time": tx_time,
        "domain_name": domain,
        "price_ton": price,
        "sale_type": sale_type,
    }


def save_history():
    driver = make_driver()
    try:
        driver.get(PAGE_URL)
        time.sleep(7)
        setup_filters(driver)

        table = driver.find_element(By.CSS_SELECTOR, "table")
        viewport = table.find_element(
            By.XPATH, "./ancestor::div[contains(@class, 'mantine-ScrollArea-viewport')][1]"
        )

        seen = set()
        records = []
        row_count = 0

        i = 0
        while True:
            i += 1
            all_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            row_count = len(all_rows)
            rows = all_rows[row_count:]
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 6:
                    continue
                record = parse_row(cells)
                if not record:
                    continue
                key = (record["tx_time"], record["domain_name"], record["price_ton"], record["sale_type"])
                if key not in seen:
                    seen.add(key)
                    records.append(record)

            if i % 5 == 0:
                logger.info(f"[Итерация #{i}] Строк в таблице доменов: {len(rows)}")

            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", viewport)
            
            for _ in range(3):
                time.sleep(2)
                new_row_count = len(driver.find_elements(By.CSS_SELECTOR, "table tbody tr"))
                if new_row_count >= row_count:
                    break
            if new_row_count <= row_count:
                logger.info(f"Таблица перестала подгружать строки на итерации {i}")
                break 

        with open(DATASET_FILE, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(records)

        logger.info(f"Сохранено {len(records)} записей в {DATASET_FILE}")
    except Exception as e:
        logger.error("Chromedriver error", exc_info=True)
    
    driver.quit()


if __name__ == "__main__":
    save_history()

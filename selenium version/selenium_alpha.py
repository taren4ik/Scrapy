import datetime
import os
import re
import random
import time

import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine
from user_agents import USER_AGENTS

load_dotenv()

host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
schema_name = os.getenv("DB_SCHEMA")
table_name = os.getenv("DB_TABLE_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASS")


POST_TYPE = (#'legkovye',
             #'kommercheskij',
             #'gruzovye',
             #'pricepy',
             #'spectech',
             'oborudovanie'
             )


URL = f"https://alfaleasing.ru/rasprodazha-avto-s-probegom/"


def timer_wrapper(func):
    """
    Декоратор-таймер.
    :param func:
    :return:
    """

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        difference_time = end_time - start_time
        print(
            f"Функция {func.__name__} выполнилась за "
            f"{difference_time:.4f} секунд.")
        return result
    return wrapper


def write_profiles_to_csv(df, category,  flag=False):
    """
    Запись информации в файл из DataFrame.
    :param df, flag:
    :return:
    """
    path = datetime.date.today().__str__().replace("-", "_")
    filename = f"profiles_farpost_{path}_{category}.csv"
    df.to_csv(
        f"{filename}", mode="a", sep=";", header=flag, index=False,
        encoding="utf-16"
    )
    return filename


def extract_post(soup, **kwargs):
    """
    Extract all posts from page.
    :return: posts
    """
    posts = []
    for class_name, type_element in kwargs.items():
        result = [
            type_element
            for type_element in soup.find_all(
                type_element,
                class_=class_name,
            )
        ]
        posts.append(result)
    return posts


def parse_specs(soup):
    specs = {}

    items = soup.select('[data-test-id="specifications-item"]')

    for item in items:
        name_el = item.select_one('[data-test-id="specifications-item-name"]')
        value_el = item.select_one('[data-test-id="specifications-item-value"]')

        if name_el and value_el:
            name = name_el.text.strip()
            value = value_el.text.strip()
            specs[name] = value

    return specs


def parse_calculator(soup):
    data = {}

    # --- цена ---
    price_el = soup.select_one('[itemprop="price"]')
    data["price"] = int(price_el["content"]) if price_el else None

    # --- блоки калькулятора ---
    def extract_by_testid(test_id):
        el = soup.select_one(f'[data-test-id="{test_id}"]')
        return el.text.strip() if el else None

    # платеж
    payment = soup.select_one('[data-test-id="priceInfoBlock-item-payment-amount"]')
    data["monthly_payment"] = int(re.sub(r'\D', '', payment.text)) if payment else None


    total = soup.select_one('[data-test-id="priceInfoBlock-item-other-amount"]')
    data["lease_total"] = int(re.sub(r'\D', '', total.text)) if total else None

    # скидка / экономия
    savings_blocks = soup.select('[data-test-id="priceInfoBlock-item-other-amount"]')
    if len(savings_blocks) > 1:
        data["savings"] = int(re.sub(r'\D', '', savings_blocks[1].text))
    else:
        data["savings"] = None

    return data


def parse_images(soup):

    images = list(set(
        img.get('itemid')
        for img in soup.select('img[data-carimg="true"]')
        if img.get('itemid')
    ))

    return {
        'main_image': images[0] if images else None,
        'images': '|'.join(images)
    }

@timer_wrapper
def scrape_all_profiles(start_url,category, page):
    """
    Извлекает основную информацию на все объявления
    :return:
    """
    current_url = start_url + category
    chrome_options = webdriver.ChromeOptions()

    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-browser-side-navigation")
    chrome_options.add_argument("--disable-gpu")

    user_agents = USER_AGENTS

    while current_url:
        if page == 181:
            return True
        if page == 1 or page % 50 == 0: # нужно корректировать по фирму
            chrome_options.add_argument(
                f"user-agent={random.choice(user_agents)}"
            )

            driver = webdriver.Chrome(options=chrome_options)
        else:
            driver.execute_script("window.open('', '_blank');")
            # Переключение на новую вкладку (где 1 - вторая вкладка)
            driver.switch_to.window(driver.window_handles[1])
        driver.implicitly_wait(10)
        driver.get(current_url)
        time.sleep(random.uniform(7, 11))
        response = driver.page_source
        soup = BeautifulSoup(response, "html.parser")
        print(f'Страница: {page}')

        cards = soup.find_all(attrs={"data-test-id": "used-offer-card"})



        data = []

        for card in cards:
            url = card.get('href')
            if not url:
                continue

            driver.get(url)
            time.sleep(random.uniform(3, 6))

            soup_car = BeautifulSoup(driver.page_source, "html.parser")

            specs = parse_specs(soup_car)
            calc = parse_calculator(soup_car)
            images = parse_images(soup_car)

            row = {
                "url": url,
                **specs,
                **calc,
                **images
            }

            data.append(row)

            time.sleep(random.uniform(1, 3))

        df = pd.DataFrame(data)

        flag = True if page == 1 else False

        if category not in ('pricepy','spectech', 'oborudovanie'):

            df['Пробег'] = (
                df['Пробег']
                    .str.replace('км', '', regex=False)
                    .str.replace('\xa0', '', regex=False)
                    .str.replace(' ', '', regex=False)
                    .astype(int)
            )

        df['Количество владельцев'] = (
            df['Количество владельцев']
                .str.replace(' владельцев', '', regex=False)
                .str.replace(' владельца', '', regex=True)
                .str.replace(' владелец', '', regex=True)
                .str.strip()
        )

        filename = write_profiles_to_csv(df, category, flag)

        df = df[0:0]
        if page > 1 and page % 50 != 0:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        page += 1
        if page % 50 == 0:
            driver.quit()
        current_url = (
            f"{URL}/{category}?page={page}"
        )

        time.sleep(random.uniform(3, 8))
    driver.switch_to.window(driver.window_handles[0])
    driver.quit()
    return filename


def load_db(filename):
    """
    Загрузка в stage слой.
    :param path:
    :return:
    """

    database_uri = (
        f"postgresql://{user}:{password}@{host}/{database}")

    engine = create_engine(database_uri)

    try:
        df = pd.read_csv(
            filename,
            encoding='utf-16',
            delimiter=';',
            header=0,
            engine='python',

        )
    except Exception as e:
        print(f"Ошибка при загрузке CSV: {e}")

    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    if 'is_check' in df.columns:
        df['is_check'] = df['is_check'].astype(bool)

    with engine.begin() as connection:
        df.to_sql(
            table_name,
            connection,
            schema=schema_name,
            if_exists='append',
            index=False
        )


if __name__ == '__main__':
    for category in  POST_TYPE:
        # load_db(
        #     all_profiles=scrape_all_profiles(f"{URL}/", page=1)
        # )
        print(all_profiles=scrape_all_profiles(f"{URL}/", category, page=1))

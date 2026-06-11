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


# POST_TYPE = (#'legkovye',
#              #'kommercheskij',
#              #'gruzovye',
#              #'pricepy',
#              #'spectech',
#              'oborudovanie'
#              )


URL = f"https://www.vtb-leasing.ru/auto/probeg/"


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
    """
    Get specs from t-tab-content layout.
    """

    specs = {}

    root = soup.select_one('.t-tab-content.active')
    if not root:
        return specs

    items = root.select('.t-tab-content-column-item')

    for item in items:
        # у каждого item обычно 2 прямых div:
        # 1 — название (span внутри)
        # 2 — значение

        divs = item.find_all('div', recursive=False)

        if len(divs) < 2:
            continue

        name = divs[0].get_text(" ", strip=True)
        value = divs[1].get_text(" ", strip=True)

        # чистим мусор (например "Записаться на осмотр")
        value = value.replace("Записаться на осмотр", "").strip()

        if name and value:
            specs[name] = value

    return specs


def clear_number(text):
    if not text:
        return None

    num = re.sub(r"[^\d]", "", text)
    return int(num) if num else None


def parse_calculator(soup):
    data = {}

    # цена
    price = soup.select_one(".t-calculator-card-price")
    data["price"] = clear_number(price.text) if price else None

    # старая цена
    old_price = soup.select_one(".t-calculator-card-old-price")
    data["old_price"] = clear_number(old_price.text) if old_price else None

    # ежемесячный платеж
    payment = soup.select_one(
        ".t-calculator-card-result__item.t-month-pay .t-calculator-card-result__value"
    )
    data["monthly_payment"] = clear_number(payment.text) if payment else None

    # сумма договора
    lease_sum = soup.select_one(
        ".t-calculator-card-result__item.t-leasing-sum .t-discount-sum"
    )
    data["lease_total"] = clear_number(lease_sum.text) if lease_sum else None

    # налоговая экономия
    tax_back = soup.select_one(
        ".t-calculator-card-result__item.t-leasing-tax-back .t-calculator-card-result__value"
    )
    data["tax_saving"] = clear_number(tax_back.text) if tax_back else None

    # скидка
    discount = soup.select_one(
        ".t-calculator-card-result__item.t-leasing-discount .t-calculator-card-result__value"
    )
    data["discount"] = clear_number(discount.text) if discount else None

    # аванс
    advance_inputs = soup.select(
        ".t-calculator-card__parameter:nth-of-type(1) input"
    )

    if len(advance_inputs) >= 2:
        data["advance_sum"] = clear_number(
            advance_inputs[0].get("value")
        )
        data["advance_percent"] = clear_number(
            advance_inputs[1].get("value")
        )

    # срок лизинга
    lease_term = soup.select_one(
        ".t-calculator-card__parameter:nth-of-type(2) input"
    )

    data["lease_term_months"] = (
        clear_number(lease_term.get("value"))
        if lease_term
        else None
    )

    return data


# def parse_images(soup):
#
#     images = list(set(
#         img.get('itemid')
#         for img in soup.select('img[data-carimg="true"]')
#         if img.get('itemid')
#     ))
#
#     return {
#         'main_image': images[0] if images else None,
#         'images': '|'.join(images)
#     }


@timer_wrapper
def scrape_all_profiles(start_url, page):
    """
    Извлекает основную информацию на все объявления
    :return:
    """
    current_url = start_url #+ category
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
        if page == 1 or page % 50 == 0: # нужно корректировать под ЛД
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

        #cards = soup.find_all(attrs={"data-test-id": "used-offer-card"})
        cards = soup.find_all("div", class_="t-market-item")



        data = []

        for card in cards:
            link = card.find("a", class_="t-market-item-bottom-link")

            if link:
                url = "https://www.vtb-leasing.ru" + link["href"]
                print(url)

        # for card in cards:
        #     url = card.get('href')
        #     if not url:
        #         continue

                driver.get(url)
                time.sleep(random.uniform(3, 6))

                soup_car = BeautifulSoup(driver.page_source, "html.parser")

                specs = parse_specs(soup_car)
                calc = parse_calculator(soup_car)
            # images = parse_images(soup_car)

            # row = {
            #     "url": url,
            #     **specs,
            #     **calc,
            #     **images
            # }
            #
            # data.append(row)

            time.sleep(random.uniform(1, 3))

        df = pd.DataFrame(data)

        flag = True if page == 1 else False

        # if category not in ('pricepy','spectech', 'oborudovanie'):
        #
        #     df['Пробег'] = (
        #         df['Пробег']
        #             .str.replace('км', '', regex=False)
        #             .str.replace('\xa0', '', regex=False)
        #             .str.replace(' ', '', regex=False)
        #             .astype(int)
        #     )
        #
        # df['Количество владельцев'] = (
        #     df['Количество владельцев']
        #         .str.replace(' владельцев', '', regex=False)
        #         .str.replace(' владельца', '', regex=True)
        #         .str.replace(' владелец', '', regex=True)
        #         .str.strip()
        # )
        #
        # filename = write_profiles_to_csv(df, category, flag)
        #
        # df = df[0:0]
        # if page > 1 and page % 50 != 0:
        #     driver.close()
        #     driver.switch_to.window(driver.window_handles[0])
        # page += 1
        # if page % 50 == 0:
        #     driver.quit()
        # current_url = (
        #     f"{URL}/{category}?PAGEN_1={page}"
        # )

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
    #for category in  POST_TYPE:
        # load_db(
        #     all_profiles=scrape_all_profiles(f"{URL}/", page=1)
        # )
       # print(all_profiles=scrape_all_profiles(f"{URL}/", category,PAGEN_1=1))

    all_profiles = scrape_all_profiles(f"{URL}/", page=1)

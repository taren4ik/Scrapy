import datetime
import os
import random
import time
from dataclasses import dataclass

import numpy as np
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


POST_TYPE = ('legkovye',
             'kommercheskij',
             'gruzovye',
             'pricepy',
             'spectech',
             'oborudovanie'
             )


URL = f"https://alfaleasing.ru/rasprodazha-avto-s-probegom/legkovye"


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


def write_profiles_to_csv(df, flag=False):
    """
    Запись информации в файл из DataFrame.
    :param df, flag:
    :return:
    """
    path = datetime.date.today().__str__().replace("-", "_")
    filename = f"profiles_farpost_{path}.csv"
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


class ApartmentAttribute:

    def __init__(self):
        self.area = []
        self.author = []
        self.square = []
        self.is_check = []
        self. room = []
        self.views = []
        self.post_id = []
        self.profile_links = []
        self.name_announcement = []

    def clean_attribute(self):
        self.area = []
        self.author = []
        self.square = []
        self.is_check = []
        self.room = []
        self.views = []
        self.post_id = []
        self.profile_links = []
        self.name_announcement = []



@timer_wrapper
def scrape_all_profiles(start_url, page):
    """
    Извлекает основную информацию на все объявления
    :return:
    """
    current_url = start_url
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-browser-side-navigation")
    chrome_options.add_argument("--disable-gpu")
    # chrome_options.add_argument('--allow-profiles-outside-user-dir')
    # chrome_options.add_argument('--enable-profile-shortcut-manager')
    # chrome_options.add_argument(r'user-data-dir=D:\developer\scrapy')
    # chrome_options.add_argument('--profile-directory=Profile 1')
    user_agents = USER_AGENTS
    apartament = ApartmentAttribute()

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

        # for card in cards:
        #     if card.get('href'):
        #         url_car = card.get('href')
        #         print(url_car)
        #         driver.execute_script("window.open('', '_blank');")
        #         # Переключение на новую вкладку (где 1 - вторая вкладка)
        #         driver.switch_to.window(driver.window_handles[1])
        #         driver.implicitly_wait(10)
        #         driver.get(url_car)
        #         time.sleep(random.uniform(3, 7))
        #         response_car = driver.page_source
        #         soup_car = BeautifulSoup(response_car, "html.parser")
        #         #spec = soup_car.find("div",
        #         # class_="styles_specifications__U_xnL")
        #
        #         items = soup_car.select('[data-test-id="specifications-item"]')
        #
        #         specs = {}
        #
        #         for item in items:
        #             name = item.select_one(
        #                 '[data-test-id="specifications-item-name"]').text.strip()
        #             value = item.select_one(
        #                 '[data-test-id="specifications-item-value"]').text.strip()
        #             specs[name] = value
        #
        #         df = pd.concat([df, pd.DataFrame([specs])], ignore_index=True)
        #         driver.close()

        data = []

        for card in cards:
            url_car = card.get('href')

            if not url_car:
                continue

            driver.get(url_car)

            time.sleep(random.uniform(3, 6))  # имитация просмотра

            soup_car = BeautifulSoup(driver.page_source, "html.parser")

            items = soup_car.select('[data-test-id="specifications-item"]')

            specs = {}

            for item in items:
                name = item.select_one(
                    '[data-test-id="specifications-item-name"]'
                ).text.strip()

                value = item.select_one(
                    '[data-test-id="specifications-item-value"]'
                ).text.strip()

                specs[name] = value

            # можно добавить URL как идентификатор
            specs["url"] = url_car

            data.append(specs)

            time.sleep(random.uniform(1, 3))  # пауза между объявлениями
        # flag = True if page == 1 else False
        # df['square'] = df['square'].replace('кв.', 0)
        columns_order = [
            "Коробка",
            "Привод",
            "Цвет",
            "VIN-номер",
            "Пробег",
            "Количество владельцев",
            "url"
        ]

        df = pd.DataFrame(data)[
            [c for c in columns_order if c in pd.DataFrame(data).columns]]
        filename = write_profiles_to_csv(df)



        df = df[0:0]
        if page > 1 and page % 50 != 0:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        page += 1
        if page % 50 == 0:
            driver.quit()
        current_url = (
            f"{URL}?page={page}"
        )
        apartament.clean_attribute()
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
    load_db(
        all_profiles=scrape_all_profiles(f"{URL}/", page=1)
    )
  # load_db(
  #       "D:\developer\Scrapy\selenium version\profiles_farpost_2025_10_08.csv"
  #   )

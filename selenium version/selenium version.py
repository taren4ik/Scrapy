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


POST_TYPE = ('rent_flats', 'sell_flats')


URL = f"https://www.farpost.ru/vladivostok/realty/sell_flats"


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
        if page == 1 or page % 50 == 0:
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
        time.sleep(random.uniform(3, 8))
        response = driver.page_source
        soup = BeautifulSoup(response, "html.parser")
        print(f'Страница: {page}')

        if soup.find_all("div", id="map", ):  # проверка на карту
            checkbox = driver.find_element(
                By.CSS_SELECTOR, '.bzr-toggle input[type="checkbox"]')
            driver.execute_script("arguments[0].click();", checkbox)

            time.sleep(random.uniform(3, 9))
            response = driver.page_source
            soup = BeautifulSoup(response, "html.parser")

        dict_class = {
            "bull-list-item-js -exact content": "div",
            "bull-list-item-js -exact content": "td",
            "bull-list-item-js -exact": "tr",
            "bulletinBlock bull-item-content": "div",
            "descriptionCell bull-item-content__cell "
            "bull-item-content__description-cell": "div",
        }

        posts = extract_post(soup, **dict_class)

        post_prep = [sublist for sublist in posts if len(sublist) > 0]
        if len(post_prep) > 1:
            full_post = max(post_prep, key=len)
        else:
            full_post = post_prep

        if full_post == 0:
            time.sleep(200)
            scrape_all_profiles(
                f"{URL}/",
                page=page
            )

        for post in full_post[0]:
            if post.find("a")["name"]:
                apartament.post_id.append(post.find("a")["name"] if post.find(
                    "a")[
                                                             "name"][
                                                             0] != '-' else
                               post.find("a")["name"][1:])
            else:
                apartament.post_id.append(post.parent.a["name"] if post.parent.a[
                                                            "name"][
                                                            0] != '-' else
                               post.parent.a["name"][1:])

            if post.find("div", class_="price-block__price"):
                apartament.is_check.append("True")
            else:
                apartament.is_check.append("False")

            if post.find("span", class_="views nano-eye-text"):
                apartament.views.append(
                    post.find("span", class_="views nano-eye-text").text
                )
            else:
                apartament.views.append("0")

        apartament.profile_links = [
            a["href"]
            for a in soup.find_all(
                "a", class_="bulletinLink bull-item__self-link auto-shy"
            )
        ]
        apartament.name_announcement = [
            a.text
            for a in soup.find_all(
                "a", class_="bulletinLink bull-item__self-link auto-shy"
            )
        ]
        for value in apartament.name_announcement:
            apartament.room.append(
                value[0]
                if value[0].isdigit() or value.startswith(
                    "Гостинка") or value.startswith("Комната")
                else 0
            )

        cost = [div.next for div in soup.find_all(
            "div", class_="price-block__price")]
        cost = [value.replace(" ", "") for value in cost]

        district = [
            div.text for div in soup.find_all(
                "div", class_="bull-item__annotation-row")
        ]
        for value in district:

            if value.split(",")[0] == "64":
                apartament.area.append("64," + value.split(",")[1])
                apartament.author.append(value.split(",")[2])
            else:
                apartament.area.append(value.split(",")[0])
                apartament.author.append(value.split(",")[1])

            if value.split()[-1] == 'этаж':
                apartament.square.append(value.split()[-5])

            elif value.split()[-2] == 'доля':
                apartament.square.append(
                    value.split(",")[-3] + ',' + value.split(",")[-2][0]
                )
            else:
                apartament.square.append(
                    value.split(",")[-2] + "," + value.split(",")[-1][0]

                    if len(value.split(",")) > 2
                    else 0
                )
        print(f"Пост {len(apartament.post_id)}  {len(apartament.name_announcement)} "
              f"url: {len(apartament.profile_links)} комнат: "
              f"{len(apartament.room)}"
              f" {len(apartament.is_check)}")

        df = pd.DataFrame(
            {
                "id": apartament.post_id,
                "text": apartament.name_announcement,
                "area": "None",
                "link": apartament.profile_links,
                "view": apartament.views,
                "cost": "None",
                "room": apartament.room,
                "is_check": apartament.is_check,
                "square": "None",
                "author": "None",
                "date": datetime.datetime.now().__str__(),
                "type_post": "sell"
            }
        )
        for i, row in enumerate(
                np.where(df["is_check"] == "True")[0].tolist()):
            df.loc[row, "cost"] = cost[i]
            df.loc[row, "area"] = apartament.area[i]
            df.loc[row, "square"] = apartament.square[i]
            df.loc[row, "author"] = apartament.author[i]

        flag = True if page == 1 else False
        df['square'] = df['square'].replace('кв.', 0)
        filename = write_profiles_to_csv(df, flag)
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

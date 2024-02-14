import datetime
import os
import random
import time

import numpy as np
import pandas as pd
import psycopg2
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
database_uri = (
    f"postgresql://{user}:{password}@{host}/{database}")

engine = create_engine(database_uri)


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
            f"Функция {func.__name__} выполнилась за {difference_time:.4f} секунд.")
        return result

    return wrapper


def write_profiles_to_csv(df):
    """
    Запись информации в файл.
    :param df:
    :return:
    """
    path = datetime.date.today().__str__().replace("-", "_")
    filename = f"profiles_farpost_{path}.csv"
    df.to_csv(
        f"{filename}", mode="a", sep=";", header=False, index=False,
        encoding="utf-16"
    )


@timer_wrapper
def scrape_all_profiles(start_url, page):
    """
    Извлекает основную информацию на все объявления
    :return:
    """
    area = []
    author = []
    square = []
    is_check = []
    room = []
    views = []
    post_id = []
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

#    page = 1
    while current_url:
        posts = []
        if page == 181:
            return True
        if page == 1 or page % 50 == 0:
            chrome_options.add_argument(
                f"user-agent={random.choice(user_agents)}"
            )
            driver = webdriver.Chrome(options=chrome_options)
        else:
            driver.execute_script("window.open('', '_blank');")
            # Переключение на новую вкладку (по индексу, где 1 - вторая вкладка)
            driver.switch_to.window(driver.window_handles[1])
        driver.implicitly_wait(10)
        driver.get(current_url)
        time.sleep(random.uniform(3, 9))
        response = driver.page_source
        soup = BeautifulSoup(response, "html.parser")

        if soup.find_all("div", id="map", ):  # проверка на карту
            checkbox = driver.find_element(
                By.CSS_SELECTOR, '.bzr-toggle input[type="checkbox"]')
            driver.execute_script("arguments[0].click();", checkbox)

            time.sleep(random.uniform(3, 9))
            response = driver.page_source
            soup = BeautifulSoup(response, "html.parser")

        full_post_v1 = [
            div
            for div in soup.find_all(
                "div",
                class_="descriptionCell bull-item-content__cell bull-item-content__description-cell",
            )
        ]
        posts.append(full_post_v1)
        full_post_v2 = [
            div
            for div in soup.find_all(
                "div",
                class_="bulletinBlock bull-item-content"
            )
        ]
        posts.append(full_post_v2)
        full_post_v3 = [
            div
            for div in soup.find_all(
                "div", class_="bull-item bull-item_block bull-item_block-js"
            )
        ]
        posts.append(full_post_v3)

        full_post_v4 = [
            div for div in soup.find_all(
                "tr",
                class_="bull-list-item-js -exact"
            )
        ]
        posts.append(full_post_v4)

        full_post = [sublist for sublist in posts if len(sublist) > 0]

        if full_post == 0:
            time.sleep(200)
            scrape_all_profiles(
                "https://www.farpost.ru/vladivostok/realty/sell_flats/",
                page=page
            )


        for post in full_post[0]:
            # post_id.append(post.find('a')['name'] if post.find('a')['name']
            # else post.parent.a['name'])
            if post.find("a")["name"]:
                post_id.append(post.find("a")["name"])
            else:
                post_id.append(post.parent.a["name"])

            if post.find("div", class_="price-block__price"):
                is_check.append("True")
            else:
                is_check.append("False")

            if post.find("span", class_="views nano-eye-text"):
                views.append(
                    post.find("span", class_="views nano-eye-text").text
                )
            else:
                views.append("0")
        # views = [div for div in soup.find_all("span", class_="views
        # nano-eye-text")]

        profile_links = [
            a["href"]
            for a in soup.find_all(
                "a", class_="bulletinLink bull-item__self-link auto-shy"
            )
        ]
        name_announcement = [
            a.text
            for a in soup.find_all(
                "a", class_="bulletinLink bull-item__self-link auto-shy"
            )
        ]
        for value in name_announcement:
            room.append(
                value[0]
                if value[0].isdigit()
                   or value.startswith("Гостинка")
                   or value.startswith("Комната")
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
                area.append("64," + value.split(",")[1])
                author.append(value.split(",")[2])
            else:
                area.append(value.split(",")[0])
                author.append(value.split(",")[1])
            square.append(
                value.split(",")[-2] + "," + value.split(",")[-1][0]
                if len(value.split(",")) > 2
                else 0
            )

        df = pd.DataFrame(
            {
                "id": post_id,
                "text": name_announcement,
                "area": "None",
                "link": profile_links,
                "view": views,
                "cost": "None",
                "room": room,
                "is_check": is_check,
                "square": "None",
                "author": "None",
                "date": datetime.datetime.now().__str__(),
            }
        )
        for i, row in enumerate(np.where(df["is_check"] == "True")[0].tolist()):
            df.loc[row, "cost"] = cost[i]
            df.loc[row, "area"] = area[i]
            df.loc[row, "square"] = square[i]
            df.loc[row, "author"] = author[i]

        write_profiles_to_csv(df)
        # df.to_sql(
        #     table_name,
        #     engine,
        #     schema=schema_name,
        #     if_exists='append',
        #     index=False
        # )
        df = df[0:0]
        author = []
        is_check = []
        square = []
        area = []
        room = []
        views = []
        post_id = []
        if page > 1 and page % 50 != 0:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        page += 1
        if page % 50 == 0:
            driver.quit()
        current_url = (
            f"https://www.farpost.ru/vladivostok/realty/sell_flats?page={page}"
        )

        time.sleep(random.uniform(3, 9))
    driver.switch_to.window(driver.window_handles[0])
    driver.quit()
    return True


all_profiles = scrape_all_profiles(
    "https://www.farpost.ru/vladivostok/realty/sell_flats/", page= 50
)

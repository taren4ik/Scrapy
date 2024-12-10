import datetime
import logging
import random
import time
import os

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine

from user_agents import USER_AGENTS

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

#load_dotenv()

# host = os.getenv("DB_HOST")
# database = os.getenv("DB_NAME")
# schema_name = os.getenv("DB_SCHEMA")
# table_name = os.getenv("DB_TABLE_NAME")
# user = os.getenv("DB_USER")
# password = os.getenv("DB_PASS")

host = Variable.get_variable_from_secrets('HOST')
db_port = Variable.get_variable_from_secrets('DB_PORT')
user = Variable.get_variable_from_secrets('USER')
password = Variable.get_variable_from_secrets('PASSWORD')
database = Variable.get_variable_from_secrets('DATABASE')
schema_name = Variable.get_variable_from_secrets('SCHEMA_NAME')
table_name = Variable.get_variable_from_secrets('TABLE_NAME')


POST_TYPE = ('rent_flats', 'sell_flats')
user_agents = USER_AGENTS

URL = f"https://www.farpost.ru/vladivostok/realty/sell_flats"

args = {
    'owner': 'dimon',
    'provide_context': True
}

param = {
    'start_url': 'https://www.farpost.ru/vladivostok/realty/sell_flats/',
    'page': 1
}

# ----------------подключение к dwh
postgres_conn_id = 'pg'

def get_path(**kwargs):
    ti = kwargs['ti']
    attribute = datetime.date.today().strftime('%Y_%m_%d')
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/profiles_farpost_{attribute}.csv"

    print(f'Сохраняем файл: {filename}')
    ti.xcom_push(key='filename', value=filename)
    print('RUN')


def write_profiles_to_csv(df, filename, flag=False):
    """
    Запись информации в файл из DataFrame.
    :param df, flag:
    :return:
    """
    df.to_csv(
        f"{filename}", mode="a", sep=";", header=flag, index=False,
        encoding="utf-16"
    )


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
        self.room = []
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
    type_rental = []
    current_url = start_url
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-browser-side-navigation")
    chrome_options.add_argument("--disable-gpu")

    user_agents = USER_AGENTS
    apartament = ApartmentAttribute()

    while current_url:

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

        posts = int(soup.find_all("span", id="itemsCount_placeholder")[0][
            "data-count"])

        page_limit = round(posts / 50 + 1)
        if page == page_limit:
            return True

        logging.info(f'Страница: {page}')

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
                post_id.append(post.find("a")["name"] if post.find("a")[
                                                             "name"][
                                                             0] != '-' else
                               post.find("a")["name"][1:])
            else:
                post_id.append(post.parent.a["name"] if post.parent.a[
                                                            "name"][
                                                            0] != '-' else
                               post.parent.a["name"][1:])

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

        profile_links = [
            a["href"]
            for a in soup.find_all(
                "a", class_="bulletinLink bull-item__self-link auto-shy")

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
            if 'аренда' in value.split(",")[-1]:
                type_rental.append(value.split(",")[-1])
            else:
                type_rental.append('None')

            if value.split(",")[0] == "64":
                area.append("64," + value.split(",")[1])
                author.append(value.split(",")[2])
            else:
                area.append(value.split(",")[0])
                author.append(value.split(",")[1])

            if 'кв.' in value:
                if value.split()[-3] == 'этаж,':
                    square.append(value.split()[-7])
                else:
                    square.append(
                        value.split(",")[-3] + "," + value.split(",")[-2][0]

                        if len(value.split(",")) > 2
                        else 0
                    )
            else:
                square.append(None)

        logging.info(f"Постов {len(post_id)}  {len(name_announcement)} "
              f"url: {len(profile_links)} комнат: {len(room)} "
              f"аквтивное: {len(is_check)}")

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
        for i, row in enumerate(
                np.where(df["is_check"] == "True")[0].tolist()):
            df.loc[row, "cost"] = cost[i]
            df.loc[row, "area"] = area[i]
            df.loc[row, "square"] = square[i]
            df.loc[row, "author"] = author[i]

        for i, row in enumerate(
                np.where(df["link"] == "javascript:void(0)")[0].tolist()):
            df.loc[row, "is_check"] = False
            df.loc[row, "link"] = None


        flag = True if page == 1 else False
        write_profiles_to_csv(df, flag)
        logging.info('Файл записан')
        # df.to_sql(
        #     table_name,
        #     engine,
        #     schema=schema_name,
        #     if_exists='append',
        #     index=False
        # )
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


def get_remove(**kwargs):
    ti = kwargs['ti']
    path = ti.xcom_pull(key='filename', task_ids='initial')
    if os.path.isfile(path):
        os.remove(path)


def load_db(**kwargs):
    """
    Загрузка в stage слой.
    :param path:
    :return:
    """
    ti = kwargs['ti']
    filename = ti.xcom_pull(key='filename', task_ids='initial')
    #filename = '/opt/airflow/data/profiles_farpost_2024_11_11.csv'
    database_uri = (
        f"postgresql://{user}:{password}@{host}/{database}"
    )

    engine = create_engine(database_uri)
    try:
        df = pd.read_csv(
            filename,
            encoding='utf-16',
            delimiter=';',
            header=0,
            engine='python',
        )
        df.drop_duplicates(subset=['id'], keep='first', inplace=True)

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'],
                                        errors='coerce')
        if 'is_check' in df.columns:
            df['is_check'] = df['is_check'].astype(bool)

    except Exception as e:
        print(f"Ошибка при загрузке CSV: {e}")

    with engine.begin() as connection:
        df.to_sql(
            table_name,
            connection,
            schema=schema_name,
            if_exists='append',
            index=False
        )


with DAG('farpost_dag',
         description='select and transform data',
         schedule_interval='0 */2 * * *',
         catchup=False,
         start_date=datetime.datetime(2024, 10, 21),
         default_args=args,
         tags=['farpost', 'etl', 'sell']
         ) as dag:
   # with TaskGroup(group_id='push_db') as processing_tasks:
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=scrape_all_profiles,
        op_kwargs=param
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_db,
        op_kwargs=param
    )

    initial = PythonOperator(task_id='initial', python_callable=get_path)

    get_procedure = PostgresOperator(
        task_id='get_procedure',
        postgres_conn_id="pg",

        sql="""
                  CALL insert_update_layer();
                  VACUUM FULL;
               """,
    )

    get_remove = PythonOperator(task_id='get_remove', python_callable=get_remove)

    initial >> extract_data >> load_data >> get_remove >> get_procedure

if __name__ == "__main__":
    dag.test()

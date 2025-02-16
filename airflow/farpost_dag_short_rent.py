import datetime
import logging
import random
import time
import os

import numpy as np
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine

from dags.user_agents import USER_AGENTS

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
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



user_agents = USER_AGENTS

URL = f"https://www.farpost.ru/vladivostok/realty/rent-apartment"

args = {
    'owner': 'dimon',
    'provide_context': True
}

param = {
    'start_url': 'https://www.farpost.ru/vladivostok/realty/rent-apartment/',
    'page': 1
}

# ----------------подключение к dwh
postgres_conn_id = 'pg'


def get_path(**kwargs):
    ti = kwargs['ti']
    date_now = datetime.date.today().strftime('%Y_%m_%d')
    path = "/opt/airflow/data"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/profiles_farpost_short_rent_{date_now}.csv"

    print(f'Сохраняем файл: {filename}')
    ti.xcom_push(key='filename', value=filename)
    ti.xcom_push(key="date_now", value=date_now)
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
        self.type_rental = []

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
        self.type_rental = []


def scrape_all_profiles(**kwargs):
    """
    Извлекает основную информацию на все объявления
    :return:
    """
    ti = kwargs['ti']
    filename = ti.xcom_pull(key='filename', task_ids='initial')
    current_url = kwargs['start_url']
    page = kwargs['page']

    remote_webdriver = 'http://remote_chromedriver'
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
            driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub',
                                      options=chrome_options)
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
                apartament.post_id.append(post.find("a")["name"] if
                                          post.find("a")[
                                                             "name"][
                                                             0] != '-' else
                               post.find("a")["name"][1:])
            else:
                apartament.post_id.append(post.parent.a["name"] if
                                          post.parent.a[
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
                "a", class_="bulletinLink bull-item__self-link auto-shy")

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
            if len(value.split(",")) < 2:
                apartament.type_rental.append('None')
                apartament.area.append('None')
                apartament.author.append(value.split(",")[0])
                apartament.square.append('None')
            else:

                if value.split(",")[0] == "64":
                    apartament.area.append("64," + value.split(",")[1])
                    if len(value.split(",")) == 2:
                        apartament.author.append('None')
                    else:
                        apartament.author.append(value.split(",")[2])
                else:
                    apartament.area.append(value.split(",")[0])
                    apartament.author.append(value.split(",")[1])

                if 'кв.' in value:
                    if value.split()[-3] == 'этаж,':
                        apartament.square.append(value.split()[-7])
                    else:
                        apartament.square.append(
                            value.split(",")[-3] + "," + value.split(",")[-2][0]

                            if len(value.split(",")) > 2
                            else 0
                        )
                else:
                    apartament.square.append(None)

        logging.info(f"Постов {len(apartament.post_id)}  {len(apartament.name_announcement)} "
              f"url: {len(apartament.profile_links)} комнат: {len(apartament.room)} "
              f"аквтивных : {len(apartament.is_check)}" )



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
                "type_post": "rent",
                "type_rental": "суточная аренда",

            }
        )
        logging.info(df)
        for i, row in enumerate(
                np.where(df["is_check"] == "True")[0].tolist()):
            df.loc[row, "cost"] = cost[i]
            df.loc[row, "area"] = apartament.area[i]
            df.loc[row, "square"] = apartament.square[i]
            df.loc[row, "author"] = apartament.author[i]


        for i, row in enumerate(
                np.where(df["link"] == "javascript:void(0)")[0].tolist()):
            df.loc[row, "is_check"] = False
            df.loc[row, "link"] = None


        flag = True if page == 1 else False
        write_profiles_to_csv(df, filename, flag)
        logging.info('Файл записан')

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
    date_now = ti.xcom_pull(key='date_now', task_ids='initial')
    #filename = f'/opt/airflow/data/profiles_farpost_rent_{date_now}.csv'
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


with DAG('farpost_dag_short_rent',
         description='select and transform data',
         schedule_interval='0 */24 * * *',
         catchup=False,
         start_date=datetime.datetime(2024, 10, 21),
         default_args=args,
         tags=['farpost', 'etl', 'rent']
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
                  CALL farpost.insert_update_layer_rent();
               """,
    )

    garbage_collection = PostgresOperator(
        task_id='garbage_collection',
        postgres_conn_id="pg",

        sql="""
                     VACUUM FULL;
                  """,
    )

    get_clean = PostgresOperator(
        task_id='get_clean',
        postgres_conn_id="pg",

        sql="""
             update farpost.farpost_staging
                set
                    square = NULL
                where length(square) > 5;
            """,
    )




    get_remove = PythonOperator(task_id='get_remove',
                                python_callable=get_remove
                                )
    initial >> extract_data >>load_data >> get_remove >> get_clean >> get_procedure >> garbage_collection


if __name__ == "__main__":
    dag.test()

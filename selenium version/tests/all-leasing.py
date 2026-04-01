import datetime
import os
import random
import time

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


from selenium import webdriver


driver = webdriver.Chrome()
driver.get("https://www.infullbroker.ru/leasing-companies/")
time.sleep(7)  # даём странице загрузиться

# скроллим страницу
for _ in range(5):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)
response = driver.page_source
soup = BeautifulSoup(response, "html.parser")
companies = []
driver.quit()
for a in soup.find_all("div", class_="styles_item__title__MHxbk"):
    name = a.get_text(strip=True)
    if name:
        companies.append(name)

for c in companies:
    print(c)

print("Всего:", len(companies))

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
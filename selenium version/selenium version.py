import requests
import random
from bs4 import BeautifulSoup
import csv

import pandas as pd
import numpy as np
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


# def extract_profile_info(profile_html):
#     soup = BeautifulSoup(profile_html, "html.parser")
#     flat_data = {}
#
#     if soup.find(
#             'span', {'data-field': 'street-district'}).text.strip():
#         flat_data['srteet_district'] = soup.find(
#             'span', {'data-field': 'street-district'}).text.strip()
#     else:
#         flat_data['srteet_district'] = None
#     if soup.find(
#             'span', {'data-field': 'subject'}).text.strip():
#
#         flat_data['title'] = soup.find(
#             'span', {'data-field': 'subject'}).text.strip()
#     else:
#         flat_data['title'] = None
#     if soup.find(
#             'span', {'data-field': 'wallMaterial'}).text.strip():
#         flat_data['material'] = soup.find(
#             'span', {'data-field': 'wallMaterial'}).text.strip()
#     else:
#         flat_data['material'] = None
#
#     if soup.find(
#             'span', {'data-field': 'areaTotal-share'}).text.strip()[:-7:]:
#
#         flat_data['area'] = soup.find(
#             'span', {'data-field': 'areaTotal-share'}).text.strip()[:-7:]
#     else:
#         flat_data['area'] = None
#
#     if soup.find(
#             'span', {'data-field': 'flatType'}).text.strip():
#
#         flat_data['count_rooms'] = soup.find(
#             'span', {'data-field': 'flatType'}).text.strip()
#     else:
#         flat_data['count_rooms'] = None
#
#     if soup.find('span',
#                  class_="viewbull-summary-price__value").text.strip()[
#        :-1:].replace(' ', ''):
#         flat_data['price'] = soup.find('span',
#                   class_="viewbull-summary-price__value").text.strip()[
#                              :-1:].replace(' ', '')
#     else:
#
#         flat_data['price'] = None
#     return flat_data


# def safe_request(url, headers):
#     """
#     Выполняет безопасный HTTP-запрос с повторными попытками
#     :param url:
#     :param headers:
#     :return:
#     """
#     while True:
#         try:
#             response = requests.get(url, headers=headers)
#             response.raise_for_status()  # проверяет, что ответ успешен
#             return response
#         except requests.RequestException as e:
#             print(f"Error fetching {url}: {e}. Retrying in 7 seconds...")
#             time.sleep(7)


def write_profiles_to_csv(df):
    """
    Запись информации в файл.
    :param df:
    :return:
    """
    filename = f"profiles_farpost_{datetime.date.today().__str__().replace('-','_')}.csv"
    df.to_csv(f'{filename}', mode='a',
                   sep=';',
                   header=True,
                   index=False,
                   encoding='utf-16')
    # fieldnames = set()
    # for profile in profiles:
    #     fieldnames.update(profile.keys())
    #
    # with open(filename, 'a', newline='', encoding='utf-8') as file:
    #     writer = csv.DictWriter(file, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for profile in profiles:
    #         writer.writerow(profile)


def scrape_all_profiles(start_url):
    """
    Извлекает основную информацию на все объявления
    :return:
    """
    area = []
    author = []
    square = []
    is_check = []
    room = []
    current_url = start_url
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(
        '--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-infobars')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-browser-side-navigation')
    chrome_options.add_argument('--disable-gpu')
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Linux; Android 9; SM-T385) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36',
        'Mozilla/5.0 (Linux; Android 9; Redmi Note 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.127 Mobile Safari/537.36',
        'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/87.0.4280.77 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 10; GM1910) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 10; MI 8 Lite) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.111 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 9; FLA-LX1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36 OPR/61.2.3076.56749',
        'Mozilla/5.0 (Linux; arm_64; Android 7.1.1; Lenovo TB-8504X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 YaBrowser/20.6.1.73.01 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/86.0.4240.93 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 8.1.0; 16th) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.81 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 9) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/74.0.3729.136 Mobile Safari/537.36 DuckDuckGo/5',
    ]

    chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
    page = 1
    while current_url:
        if current_url == 'https://www.farpost.ru/vladivostok/realty/sell_flats/':
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(7)
            driver.get(current_url)
        else:
            driver.execute_script("window.open('', '_blank');")

            # Переключение на новую вкладку (по индексу, где 1 - вторая вкладка)
            driver.switch_to.window(driver.window_handles[1])
            driver.get(current_url)
        time.sleep(random.uniform(3, 6))
        response = driver.page_source
        soup = BeautifulSoup(response, 'html.parser')

        full_post_v1 = [div for div in soup.find_all("div",
                                                   class_="descriptionCell bull-item-content__cell bull-item-content__description-cell")]

        full_post_v2 = [div for div in soup.find_all("div",
                                                     class_="bulletinBlock bull-item-content")]
        full_post = full_post_v1 if full_post_v1 else full_post_v2

        for post in full_post:
            if post.find('div',
                         class_='bull-item-content__price-info-container').text:
                is_check.append('True')
            else:
                is_check.append('False')

        profile_links = [a["href"] for a in soup.find_all("a",
                                                          class_="bulletinLink bull-item__self-link auto-shy")]
        name_announcement = [a.text for a in soup.find_all("a",
                                                           class_="bulletinLink bull-item__self-link auto-shy")]
        for value in name_announcement:
            #is_check.append('False' if ('ЖК' in value) else 'True')

            room.append(value[0] if value[0].isdigit() or value.startswith(
                'Гостинка') or value.startswith('Комната') else 0)

        cost = [div.next for div in soup.find_all("div",
                                                     class_="price-block__price")]
        cost = [value.replace(' ', '') for value in cost]

        district = [div.text for div in soup.find_all("div",
                                                      class_="bull-item__annotation-row")]
        for value in district:
            if value.split(',')[0] == '64':
                area.append('64,' + value.split(',')[1])
            else:
                area.append(value.split(',')[0])
            square.append(value.split(',')[-2] + ',' + value.split(',')[-1][
                0] if len(value.split(',')) > 2 else 0)
            author.append(value.split(',')[1] if value.split(',')[1] else
                           'None')

        views = [span.text for span in soup.find_all("span", class_="views nano-eye-text")]

        df = pd.DataFrame({
            'Название': name_announcement,
            'Район': 'None',
            'Ссылка': profile_links,
            'Просмотры': views,
            'Стоимость': 'None',
            'Комнат': room,
            'Формат': is_check,
            'Площадь': 'None'

        })
        for i, row in enumerate(np.where(df['Формат'] == 'True')[0].tolist()):
            df.loc[row, 'Стоимость'] = cost[i]
            df.loc[row, 'Район'] = area[i]
            df.loc[row, 'Площадь'] = square[i]

        write_profiles_to_csv(df)
        df = df[0:0]
        author = []
        is_check = []
        square = []
        area = []
        room = []
        if page > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        page += 1
        current_url = (
            f'https://www.farpost.ru/vladivostok/realty/sell_flats?page={page}')

        time.sleep(random.uniform(2, 5))
    driver.switch_to.window(driver.window_handles[0])
    driver.quit()
    return True


# Начните с извлечения всех объявлений
all_profiles = scrape_all_profiles(
    "https://www.farpost.ru/vladivostok/realty/sell_flats/")

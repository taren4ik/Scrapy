import datetime
import random
import time

from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import numpy as np


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
        print(f"Функция {func.__name__} выполнилась за {difference_time:.4f} секунд.")
        return result
    return wrapper


def write_profiles_to_csv(df):
    """
    Запись информации в файл.
    :param df:
    :return:
    """
    filename = f"profiles_farpost_{datetime.date.today().__str__().replace('-','_')}.csv"
    df.to_csv(f'{filename}', mode='a',
                   sep=';',
                   header=False,
                   index=False,
                   encoding='utf-16')


@timer_wrapper
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
    views = []
    post_id = []
    current_url = start_url
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-infobars')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-browser-side-navigation')
    chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument('--allow-profiles-outside-user-dir')
    # chrome_options.add_argument('--enable-profile-shortcut-manager')
    # chrome_options.add_argument(r'user-data-dir=D:\developer\scrapy')
    # chrome_options.add_argument('--profile-directory=Profile 1')
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

    page = 1
    start_time = time.no
    while current_url:
        if page == 1 or page % 50 == 0:
            chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
            driver = webdriver.Chrome(options=chrome_options)
        else:
            driver.execute_script("window.open('', '_blank');")

            # Переключение на новую вкладку (по индексу, где 1 - вторая вкладка)
            driver.switch_to.window(driver.window_handles[1])
        driver.implicitly_wait(10)
        driver.get(current_url)
        time.sleep(random.uniform(3, 9))
        response = driver.page_source
        soup = BeautifulSoup(response, 'html.parser')

        full_post_v1 = [div for div in soup.find_all("div",
                                                   class_="descriptionCell bull-item-content__cell bull-item-content__description-cell")]

        full_post_v2 = [div for div in soup.find_all("div",
                                                     class_="bulletinBlock bull-item-content")]
        full_post = full_post_v1 if full_post_v1 else full_post_v2

        for post in full_post:
            post_id.append(post.find('a')['name'] if post.find('a')['name'] else post.parent.a['name'])
            if post.find('div',
                         class_='bull-item-content__price-info-container').text:
                is_check.append('True')
            else:
                is_check.append('False')

            if post.find('span',  class_='views nano-eye-text'):
                views.append(
                    post.find('span',  class_='views nano-eye-text').text)
            else:
                views.append('0')

        profile_links = [a["href"] for a in soup.find_all("a",
                                                          class_="bulletinLink bull-item__self-link auto-shy")]
        name_announcement = [a.text for a in soup.find_all("a",
                                                           class_="bulletinLink bull-item__self-link auto-shy")]
        for value in name_announcement:
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
                author.append(value.split(',')[2])
            else:
                area.append(value.split(',')[0])
                author.append(value.split(',')[1])
            square.append(value.split(',')[-2] + ',' + value.split(',')[-1][
                0] if len(value.split(',')) > 2 else 0)

        df = pd.DataFrame({
            'Id': post_id,
            'Название': name_announcement,
            'Район': 'None',
            'Ссылка': profile_links,
            'Просмотры': views,
            'Стоимость': 'None',
            'Комнат': room,
            'Формат': is_check,
            'Площадь': 'None',
            'Автор': 'None',


        })
        for i, row in enumerate(np.where(df['Формат'] == 'True')[0].tolist()):
            df.loc[row, 'Стоимость'] = cost[i]
            df.loc[row, 'Район'] = area[i]
            df.loc[row, 'Площадь'] = square[i]
            df.loc[row, 'Автор'] = author[i]

        write_profiles_to_csv(df)
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
            f'https://www.farpost.ru/vladivostok/realty/sell_flats?page={page}')

        time.sleep(random.uniform(2, 7))
    driver.switch_to.window(driver.window_handles[0])
    driver.quit()
    return True


all_profiles = scrape_all_profiles(
    "https://www.farpost.ru/vladivostok/realty/sell_flats/")

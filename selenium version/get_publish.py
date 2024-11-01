import os
import time
import random
import requests

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL = os.getenv("CHANNEL")
email = os.getenv("EMAIL")
password = os.getenv("PASSWORD")


def send_image_to_telegram(image_path):
    """
    Отправка изображения в канал.
    :param image_path:
    :return:
    """
    url = f'https://api.telegram.org/bot{TOKEN}/sendPhoto'
    files = {'photo': open(image_path, 'rb')}
    data = {
        'chat_id': CHANNEL,
        'caption': "Недельный обзор"
    }
    response = requests.post(url, files=files, data=data)

    return response


def insert_form(*args):
    """
    Вставка в форму параметров.
    :return:
    """
    try:
        driver, type_value, value = args[0], args[1], args[2]
        input = driver.find_element(
            By.CSS_SELECTOR, f'input[type="{type_value}"]'
        )
        input.send_keys(f'{value}')
    except Exception as e:
        print(f'Error: {e}')

    time.sleep(random.uniform(2, 7))


def capture_dashboard():
    """
    Делаем скриншот дашборда.
    :return:
    """
    driver = webdriver.Chrome()
    driver.get('http://127.0.0.1:3000/dashboard/10')
    time.sleep(5)
    # email_input = driver.find_element(By.CSS_SELECTOR, 'input[type="email"]')
    # email_input.send_keys('')
    # time.sleep(random.uniform(2, 5))
    insert_form(driver, "email", f'{email}')
    insert_form(driver, "password", f'{password}')
    # password_input = driver.find_element(
    #     By.CSS_SELECTOR, 'input[type="password"]'
    # )
    # password_input.send_keys('')
    # time.sleep(random.uniform(2, 5))

    button = driver.find_element(By.CSS_SELECTOR, 'button[title="Войти"]')
    button.click()

    driver.get('http://127.0.0.1:3000/dashboard/10')
    time.sleep(5)
    driver.save_screenshot("sell.png")
    driver.quit()


if __name__ == '__main__':
    capture_dashboard()
    send_image_to_telegram("sell.png")

# URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
# dashboard_url = """http://vl_city.ru/public/dashboard/74dcfb3b-00fa-46a5
# -9528-ad04bd6e69f4"""
#
#
# params = {
#     "chat_id": CHAT_ID,
#     "text": f"Просмотр доступен по ссылке: {dashboard_url}",
# }
#
#
# response = requests.get(URL, params=params)
#
#
# if response.status_code == 200:
#     print("Сообщение отправлено!")
# else:
#     print("Ошибка:", response.status_code, response.text)


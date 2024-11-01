import os
import time
import random
import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.common.by import By

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")



def send_image_to_telegram(image_path):
    """
    Отправка изображения в канал.
    :param image_path:
    :return:
    """
    url = f'https://api.telegram.org/bot{TOKEN}/sendPhoto'
    files = {'photo': open(image_path, 'rb')}
    data = {'chat_id': CHAT_ID}
    response = requests.post(url, files=files, data=data)
    return response


def capture_dashboard():
    """
    Делаем скриншот дашборда.
    :return:
    """
    driver = webdriver.Chrome()
    driver.get('http://127.0.0.1:3000/dashboard/10')
    time.sleep(5)
    email_input = driver.find_element(By.CSS_SELECTOR, 'input[type="email"]')
    email_input.send_keys('admin@mail.ru')
    time.sleep(random.uniform(2, 5))

    password_input = driver.find_element(
        By.CSS_SELECTOR, 'input[type="password"]'
    )
    password_input.send_keys('292516')
    time.sleep(random.uniform(2, 5))

    button = driver.find_element(By.CSS_SELECTOR, 'button[title="Войти"]')
    button.click()

    driver.get('http://127.0.0.1:3000/dashboard/10')

    driver.execute_script("document.body.style.transform = 'scale(0.5)';")
    driver.execute_script("document.body.style.transformOrigin = '0 0';")
    driver.save_screenshot("sell.png")
    driver.quit()


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



from pathlib import Path
import time
import scrapy

from bs4 import BeautifulSoup


class Spyder(scrapy.Spider):
    name = 'farpost_spider'
    start_urls = ['https://www.farpost.ru/vladivostok/realty/sell_flats/']

    def parse(self, response):
        """
        Осуществляет переход по страницам и объявлениям внутри страницы.
        :param response:
        :return:
        """
        for link in response.css(
                'div.bull-item-content__subject-container a::attr(href)'):
            time.sleep(1)
            yield response.follow(link, callback=self.parse_flat)

        for i in range(1, 10):  # переход по страницам
            next_page = (f'https://www.farpost.ru/vladivostok/realty/sell_flats?page={i}')
            yield response.follow(next_page, callback=self.parse)

    def parse_flat(self, response):
        """
        Сбор информации по каждой квартире.
        :param response:
        :return:
        """
        soup = BeautifulSoup(response.text, "html.parser")
        yield {
              'srteet_district': soup.find(
                  'span', {'data-field': 'street-district'}).text.strip(),
              'title': soup.find(
                  'span', {'data-field': 'subject'}).text.strip(),
              'material': soup.find(
                  'span', {'data-field': 'wallMaterial'}).text.strip(),
              'area': soup.find(
                  'span', {'data-field': 'areaTotal-share'}).text.strip(),
              'count_rooms': soup.find(
                  'span', {'data-field': 'flatType'}).text.strip(),
              'price': soup.find(
                  'span', class_="viewbull-summary-price__value").text.strip()
        }

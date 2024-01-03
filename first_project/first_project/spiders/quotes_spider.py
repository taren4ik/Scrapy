import time
import  random

import scrapy
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess


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
            time.sleep(random.uniform(1, 4))
            yield response.follow(link, callback=self.parse_flat)

        for i in range(1, 200):  # переход по страницам
            next_page = (f'https://www.farpost.ru/vladivostok/realty/sell_flats?page={i}')
            yield response.follow(next_page, callback=self.parse)

    def parse_flat(self, response):
        """
        Сбор информации по каждой квартире.
        :param response:
        :return:
        """
        soup = BeautifulSoup(response.text, "html.parser")
        time.sleep(random.uniform(1, 3))
        yield {
              'srteet_district': soup.find(
                  'span', {'data-field': 'street-district'}).text.strip(),
              'title': soup.find(
                  'span', {'data-field': 'subject'}).text.strip(),
              'material': soup.find(
                  'span', {'data-field': 'wallMaterial'}).text.strip(),
              'area': soup.find(
                  'span', {'data-field':'areaTotal-share'}).text.strip()[:-7:],
              'count_rooms': soup.find(
                  'span', {'data-field': 'flatType'}).text.strip(),
              'price': soup.find('span', class_="viewbull-summary-price__value").text.strip()[:-1:].replace(' ', '')
        }


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(Spyder)
    process.start()

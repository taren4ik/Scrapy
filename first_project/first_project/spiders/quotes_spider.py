from pathlib import Path

import scrapy


class QuotesSpider(scrapy.Spider):
    name = 'farpost_flat'

    def start_requests(self):
        urls = [
            "https://www.farpost.ru/vladivostok/realty/sell_flats/",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for link in response.css('div.bull-item-content__subject-container a::attr(href)').get()
            yield  response.follow(link, callback=self.parse_flat)
        for i in range (1,10):
            next_page = (
            f'https://www.farpost.ru/vladivostok/realty/sell_flats?page={i}')
        yield response.follow(next_page, callback=self.parse)


    def parse_flat(self, response):
        yield {

            'buy': response.css('div.viewbull-summary-price__realty-price '
                                'span::attr(data-bulletin-price)').get(),
            'address': response.css('span.auto-shy::text').get()
        }
          #  'name':
          #  'district':
          #  'type':
          # 'square':


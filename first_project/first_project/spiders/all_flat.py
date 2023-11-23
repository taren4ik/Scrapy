import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class Spyder(CrawlSpider):
    name = 'all_flat'
    start_urls = ['https://www.farpost.ru/']
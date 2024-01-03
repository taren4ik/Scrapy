# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import time
import random
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from scrapy.http import HtmlResponse
from scrapy.utils.project import get_project_settings

from scrapy import signals
from fake_useragent import UserAgent
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class FirstProjectSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class FirstProjectDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class RandomUserAgentMiddleware:
    def __init__(self):
        self.ua = UserAgent()

    def process_request(self, request, spider):
        user_agent = self.ua.random
        request.headers.setdefault('User-Agent', user_agent)

class SeleniumMiddleware:
    def __init__(self):
        settings = get_project_settings()
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')  # Запустить Chrome в фоновом режиме
        self.driver = webdriver.Chrome(executable_path=settings.get('D:\developer\Scrapy\chromedriver\chromedriver.exe'), options=chrome_options)

    def process_request(self, request, spider):
        self.driver.get(request.url)
        time.sleep(5)  # Задержка для загрузки страницы и выполнения JavaScript

        # Находим и нажимаем кнопку "Продолжить"
        send_button = self.driver.find_element_by_id("send-button")
        send_button.click()
        body = self.driver.page_source
        return HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)

    def __del__(self):
        self.driver.quit()

class ProxyMiddleware(HttpProxyMiddleware):
    def process_request(self, request, spider):
        proxy_list = [
                'http://95.167.42.44:8080',
                'http://93.170.95.116:8080',
                'http://86.102.7.172:8080',
                'http://86.102.15.227:8080',
                'http://86.102.12.118:8080',
                'http://5.143.74.156:8080',
                'http://5.143.97.56:8080',
                'http://5.143.20.195:8080',
                'http://77.35.152.255:8080',
                'http://77.35.41.238:8080',
                'http://77.35.226.140:8080'
        ]
        # Выберите прокси случайным образом из списка
        request.meta['proxy'] = proxy_list[random.randint(0, len(proxy_list) - 1)]
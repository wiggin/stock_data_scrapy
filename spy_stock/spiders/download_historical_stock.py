# -*- coding: utf-8 -*-
import random
import time

import scrapy
from pymongo import MongoClient
from pymongo import errors
from bs4 import BeautifulSoup as bs
from spy_stock.items import downloadStockHistoricalItem
from spy_stock.settings import MONGO_URI, MONGO_DB, MONGO_USERNAME, MONGO_PWD
import datetime

class DownloadHistoricalStockSpider(scrapy.Spider):
    collection = 'stock_code_list'
    data_collection = 'stock_data'
    name = 'download_historical_stock'
    start_urls = ['http://quotes.money.163.com/']
    headers = {
        'Referer': 'http://quotes.money.163.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36'
    }


    def __init__(self):
        scrapy.Spider.__init__(self)  # 必须显式调用父类的init
        self.current_stock_code = ''
        self.mongo_url = MONGO_URI
        self.mongo_db = MONGO_DB
        self.client = MongoClient(self.mongo_url)
        self.db = self.client[self.mongo_db]
        self.client.admin.authenticate(MONGO_USERNAME, MONGO_PWD)
        self.stock_list = self.db[self.collection].find({}, {'code': 1,
                          '_id': 0}).sort([('code',1)])   # 只取code这列， _id需手动设置0
        self.now_time = datetime.datetime.now().strftime('%Y-%m-%d').replace('-', '')


    def start_requests(self):
        for stock_code in list(self.stock_list):
            self.current_stock_code = str(stock_code.get('code'))
            url = 'http://quotes.money.163.com/trade/lsjysj_{}.html'.format(
                self.current_stock_code)
            yield scrapy.Request(url=url,callback=self.parse)

    def parse(self, response):
        # 解析response中的开始日期和结束日期
        text = response.text
        soup = bs(text, 'lxml')     # 解析爬虫数据
        start_time = soup.find('input', {'name': 'date_start_type'}).get(
            'value').replace('-', '')  # 获取起始时间
        # self.log('start_time % s' % start_time)
        end_time = soup.find('input', {'name': 'date_end_type'}).get(
            'value').replace('-', '')  # 获取结束时间
        code = soup.find('meta', {'name': 'keywords'}).get(
            'content').split(',')[1]

        # 若已有缓存则获取缓存最后一日的日期
        stock_data = self.db[self.data_collection].find({"stock_code": code}).sort([("date", -1)]).limit(1)

        if stock_data.count() > 0:
            for item in stock_data:
                start_time = item["date"].replace('-', '')

        # self.log('end_time %s' % end_time)
        time.sleep(random.choice([1, 2]))
        # self.log('start link')
        # self.log('stock_code %s' % self.current_stock_code)
        file_item = downloadStockHistoricalItem()
        if len(self.current_stock_code) > 0:
            stock_code_a = str(code)
            # 由于东方财富网上获取的代码一部分为基金，无法获取数据，故将基金剔除掉。
            # 沪市股票以6,9开头，深市以0,2,3开头，但是部分基金也是2开头，201/202/203/204这些也是基金
            # 另外获取data的网址股票代码 沪市前加0， 深市前加1
            if int(stock_code_a[0]) in [0, 2, 3, 6, 9]:
                if int(stock_code_a[0]) in [6, 9]:
                    new_stock_code = '0' + stock_code_a
                if int(stock_code_a[0]) in [0, 2, 3]:
                    if not int(stock_code_a[0:3]) in [201, 202, 203, 204]:
                        new_stock_code = '1' + stock_code_a

            download_url = "http://quotes.money.163.com/service/chddata.html" \
                           "?code={}&start={}&end={" \
                           "}&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG" \
                           ";TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(
                new_stock_code, start_time, end_time)
            file_item['file_urls'] = [download_url]
            yield file_item

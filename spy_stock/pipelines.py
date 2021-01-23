# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import os
from pymongo import MongoClient
from pymongo import errors  # mongodb的错误信息
from spy_stock.settings import MONGO_URI, MONGO_DB, MONGO_USERNAME, MONGO_PWD
import pandas as pd
from scrapy.pipelines.files import FilesPipeline

# 获取股票代码
class SpyStockPipeline(object):
    collection = 'stock_code_list'

    def __init__(self):
        self.mongo_url = MONGO_URI
        self.mongo_db = MONGO_DB
        self.client = MongoClient(self.mongo_url)  # 连接mongo客户端
        self.db = self.client[self.mongo_db]  # 选择库
        self.client.admin.authenticate(MONGO_USERNAME, MONGO_PWD)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        data = dict(item)
        table = self.db[self.collection]  # 选择表
        try:
            stock_code_is_exist = self.db[self.collection].find({'code':data["code"]}, {'code': 1,'_id': 0}).sort([('code', 1)]).count()

            if stock_code_is_exist == 0:
                table.insert(data)  # 插入数据
        except errors.DuplicateKeyError:  # 处理主键重复错误
            pass
        return item

class CsvWriterPipeline(object):
    def process_item(self, item, spider):
        try:
            path = os.path.abspath('') + '/historical_data/' + item['code'] + \
                                      '.csv'
            pd.set_option('display.float_format', lambda x: '%.2f' % x)
            data = self.deal_with_historical_data(item)

            if os.path.exists(path):
                data.to_csv(path, header=None, index=False, mode='a',
                         encoding='gbk')
            else:
                pd.DataFrame(columns=['code', 'date', 'open', 'close', 'high', 'low', 'volume']) \
                    .to_csv(path, header=None, index=False, mode='a',
                            encoding='gbk')
                data.to_csv(path, header=None, index=False, mode='a',
                         encoding='gbk')

            return item
        except KeyError as e:
            print(e)


    def deal_with_historical_data(self, item):
        df = item['data']
        res = pd.DataFrame({'code':'股票代码', 'date':'交易日期', 'open':'开盘价',
                              'close':'收盘价','high':'最高价','low':'最低价',
                              'volume':'交易量'}, index=[0])
        for i in df.index:
            t = df.loc[i]
            t['code'] = item['code']
            t['date'] = i.strftime('%Y-%m-%d')
            res = res.append(t, ignore_index=True)
            res = res[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
        return res

class myFilePipline(FilesPipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def file_path(self, request, response=None, info=None):
        str_temp = str(request.url)
        name = str_temp[54:61]
        name = name + '.csv'
        return name

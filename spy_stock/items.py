# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class SpyStockItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # pass

    symbol = scrapy.Field()
    name = scrapy.Field()
    open = scrapy.Field()
    high = scrapy.Field()
    low = scrapy.Field()
    trade = scrapy.Field()
    settlement = scrapy.Field()
    volume = scrapy.Field()
    amount = scrapy.Field()

class StockCodeItem(scrapy.Item):
    code = scrapy.Field()
    name = scrapy.Field()

class StockHistoricalItem(scrapy.Item):
    date = scrapy.Field()
    open = scrapy.Field()
    high = scrapy.Field()
    low = scrapy.Field()
    close = scrapy.Field()
    volume = scrapy.Field()

class StockDataFrame(scrapy.Item):
    data = scrapy.Field()
    code = scrapy.Field()

class StockItem(scrapy.Item):
    stock_id = scrapy.Field()  # 股票编号
    stock_name = scrapy.Field()  # 股票名称
    last_price = scrapy.Field()  # 最新价格
    increase_percent = scrapy.Field()  # 最新涨幅
    increase_amount = scrapy.Field()  # 最新增长额
    turn_over_hand = scrapy.Field()  # 成交量（手）
    turn_over_amount = scrapy.Field()  # 成交额
    amplitude = scrapy.Field()  # 振幅
    highest = scrapy.Field()  # 最高
    lowest = scrapy.Field()  # 最低
    today_open = scrapy.Field()  # 今开
    yest_close = scrapy.Field()  # 昨收
    quantity_relative_ratio = scrapy.Field()  # 量比
    turn_over_rate = scrapy.Field()  # 换手率
    PE_ratio = scrapy.Field()  # 市盈率
    PB_ratio = scrapy.Field()  # 市净率
    update_time = scrapy.Field()  # 更新时间

class downloadStockHistoricalItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()  # 文件下载队列

# -*- coding: utf-8 -*-
import scrapy
from scrapy import Selector, Request

from spy_stock.items import StockCodeItem


class SaveAllStockCodeSpider(scrapy.Spider):
    name = 'save_all_stock_code'
    start_urls = [
        "http://app.finance.ifeng.com/list/stock.php?t=ha&f=symbol&o=asc&p=1",
        "http://app.finance.ifeng.com/list/stock.php?t=sa&f=symbol&o=asc&p=1",
        "http://app.finance.ifeng.com/list/stock.php?t=kcb&f=symbol&o=asc&p=1"
    ]

    def parse(self, response):
        sel = Selector(response)
        # // 选取任意节点, @ 选取属性
        links = sel.xpath('//*[@class= "tab01"]/table/tr[position()>1]')
        for link in links:
            code = link.xpath('td[1]/a/text()').extract()
            name = link.xpath('td[2]/a/text()').extract()

            if len(code) == 0 :
                code = link.xpath('td[1]/text()').extract()
                name = link.xpath('td[2]/text()').extract()

            if ('下一页' not in code and '上一页' not in code):
                stock_code = StockCodeItem()
                stock_code['name'] = name[0]
                stock_code['code'] = code[0]
                yield stock_code
            else:
                continue

        for url in sel.xpath(
                '//*[@class= "tab01"]/table/tr[52]/td/a/@href').extract():
            url = "http://app.finance.ifeng.com/list/stock.php" + url
            yield Request(url, callback=self.parse)

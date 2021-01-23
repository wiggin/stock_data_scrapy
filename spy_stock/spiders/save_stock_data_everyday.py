"""
更新每日股票日k数据
http://vip.stock.finance.sina.com.cn/mkt/#stock_hs_up
从新浪网址的上述的网址，逐页获取最近一个交易日所有股票的数据
:return: 返回一个存储股票数据的DataFrame
"""

from urllib.request import urlopen  # python自带爬虫库
import pandas as pd
from datetime import datetime
import time
import re  # 正则表达式库
import os  # 系统库
import json  # python自带的json数据库
from pymongo import MongoClient
from pymongo import errors
from spy_stock.settings import MONGO_USERNAME, MONGO_PWD, MONGO_URI, MONGO_DB
import random


# 保存每日日k数据
# ===数据网址 hs_a代表沪深a股,kcb代表科创板

def save_stock_data_everyday(stock_type):
    raw_url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={}' \
              '&num=100&sort=code&asc=1&node={}&symbol=&_s_r_a=sort'
    page_num = 1

    random_list = [1, 2, 3, 4, 5]
    random_num = random.choice(random_list)

    # ===获取上证指数最近一个交易日的日期。此段代码在课程视频中没有，之后补上的
    df = get_today_data_from_sinajs(code_list=['sh000001'])
    sh_date = df.iloc[0]['candle_end_time'].date()  # 上证指数最近交易日

    # ===开始逐页遍历，获取股票数据
    while True:
        # 构建url
        url = raw_url.format(page_num, stock_type)
        print('开始抓取页数：', page_num)

        # 抓取数据
        content = get_content_from_internet(url)
        content = content.decode('gbk')

        # 判断页数是否为空
        if 'null' in content:
            print('抓取到页数的尽头，退出循环')
            break

        # 通过正则表达式，给key加上引号
        content = re.sub(r'(?<={|,)([a-zA-Z][a-zA-Z0-9]*)(?=:)', r'"\1"', content)

        # 将数据转换成dict格式
        content = json.loads(content)

        if len(content) > 0:
            # 将数据转换成DataFrame格式
            df = pd.DataFrame(content, dtype='float')
            # 对数据进行整理
            # 重命名
            rename_dict = {'symbol': 'stock_code', 'name': 'stock_name', 'open': 'open_price', 'high': 'highest_price',
                           'low': 'lowest_price',
                           'trade': 'close_price', 'settlement': 'last_closing_price', 'volume': 'turn_over_volume',
                           'amount': 'turn_over_amount',
                           'turnoverratio': 'turn_over_rate', 'pricechange': 'rise_amount',
                           'changepercent': 'rise_range', 'mktcap': 'market_value',
                           'nmc': 'circulation_market_value'}
            df.rename(columns=rename_dict, inplace=True)

            # 添加交易日期
            date = pd.to_datetime(sh_date)
            df['date'] = date.strftime("%Y-%m-%d")  # 最近交易日

            # 取需要的列
            df = df[['stock_code', 'stock_name', 'date', 'open_price', 'highest_price', 'lowest_price', 'close_price',
                     'last_closing_price',
                     'turn_over_volume', 'turn_over_amount', 'turn_over_rate', 'rise_amount', 'rise_range',
                     'market_value', 'circulation_market_value']]

            df['stock_code'] = df['stock_code'].str[2:]
            # 合并数据
            # all_df = all_df.append(df, ignore_index=True)

            # 先将 dataframe 转成 dict
            # 保存进mongodb
            data = df.to_dict(orient='record')
            # print(data)

            try:
                collection.insert_many(data)
            except errors.BulkWriteError:
                # 插入错误转换成更新
                for item in data:
                    try:
                        collection.update_one(
                            {"stock_code": item["stock_code"], "date": item["date"]},
                            {"$set": item}, True)
                    except errors.WriteError:
                        continue
                pass

            # 将页数+1
            page_num += 1
            time.sleep(random_num)
        else:
            print((page_num - 1) + "为最后一页, 爬取结束")
            break

    # ===将当天停盘的股票删除
    # all_df = all_df[all_df['开盘价'] - 0 > 0.00001]
    # all_df.reset_index(drop=True, inplace=True)


# =====函数：从网页上抓取数据
def get_content_from_internet(url, max_try_num=10, sleep_time=5):
    """
    使用python自带的urlopen函数，从网页上抓取数据
    :param url: 要抓取数据的网址
    :param max_try_num: 最多尝试抓取次数
    :param sleep_time: 抓取失败后停顿的时间
    :return: 返回抓取到的网页内容
    """
    get_success = False  # 是否成功抓取到内容
    # 抓取内容
    for i in range(max_try_num):
        try:
            content = urlopen(url=url, timeout=10).read()  # 使用python自带的库，从网络上获取信息
            get_success = True  # 成功抓取到内容
            break
        except Exception as e:
            print('抓取数据报错，次数：', i + 1, '报错内容：', e)
            time.sleep(sleep_time)

    # 判断是否成功抓取内容
    if get_success:
        return content
    else:
        raise ValueError('使用urlopen抓取网页数据不断报错，达到尝试上限，停止程序，请尽快检查问题所在')


# =====函数：判断今天是否是交易日
def is_today_trading_day():
    """
    判断今天是否是交易日
    :return: 如果是返回True，否则返回False
    """

    # 获取上证指数今天的数据
    df = get_today_data_from_sinajs(code_list=['sh000001'])
    sh_date = df.iloc[0]['candle_end_time']  # 上证指数最近交易日

    # 判断今天日期和sh_date是否相同
    return datetime.now().date() == sh_date.date()


# =====函数：从新浪获取指定股票的数据
def get_today_data_from_sinajs(code_list):
    """
    返回一串股票最近一个交易日的相关数据
    从这个网址获取股票数据：http://hq.sinajs.cn/list=sh600000,sz000002,sz300001
    正常网址：https://finance.sina.com.cn/realstock/company/sh600000/nc.shtml,
    :param code_list: 一串股票代码的list，可以多个，例如[sh600000, sz000002, sz300001],
    :return: 返回一个存储股票数据的DataFrame
    """

    # 构建url
    url = "http://hq.sinajs.cn/list=" + ",".join(code_list)

    # 抓取数据
    content = get_content_from_internet(url)
    content = content.decode('gbk')

    # 将数据转换成DataFrame
    content = content.strip()  # 去掉文本前后的空格、回车等
    data_line = content.split('\n')  # 每行是一个股票的数据
    data_line = [i.replace('var hq_str_', '').split(',') for i in data_line]
    df = pd.DataFrame(data_line, dtype='float')  #

    # 对DataFrame进行整理
    df[0] = df[0].str.split('="')
    df['stock_code'] = df[0].str[0].str.strip()
    df['stock_name'] = df[0].str[-1].str.strip()
    df['candle_end_time'] = df[30] + ' ' + df[31]  # 股票市场的K线，是普遍以当跟K线结束时间来命名的
    df['candle_end_time'] = pd.to_datetime(df['candle_end_time'])
    rename_dict = {1: 'open', 2: 'pre_close', 3: 'close', 4: 'high', 5: 'low', 6: 'buy1', 7: 'sell1',
                   8: 'amount', 9: 'volume', 32: 'status'}  # 自己去对比数据，会有新的返现
    # 其中amount单位是股，volume单位是元
    df.rename(columns=rename_dict, inplace=True)
    # df['status'] = df['status'].str.strip('";')
    df = df[['stock_code', 'stock_name', 'candle_end_time', 'open', 'high', 'low', 'close', 'pre_close', 'amount',
             'volume', 'buy1', 'sell1', 'status']]

    return df


if __name__ == '__main__':
    client = MongoClient(MONGO_URI)  # 连接mongodb
    client.admin.authenticate(MONGO_USERNAME, MONGO_PWD)
    db = client[MONGO_DB]  # 选择库
    collection = db['stock_data']  # 选择表

    # save_stock_data_everyday('hs_a')

    save_stock_data_everyday('kcb')

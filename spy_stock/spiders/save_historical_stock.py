"""
将下载的csv股票文件存储到mongodb
"""

import os
import pymongo
import csv
from pymongo import MongoClient
from pymongo import errors
import codecs
import pandas as pd

from spy_stock.settings import MONGO_USERNAME, MONGO_PWD, MONGO_URI, MONGO_DB

def save_historical_stock(file):
    # csv_file = csv.reader(file)  # 将读入的文件转换成csv.reader对象
    csv_file = pd.read_csv(file)  # 将读入的文件转换成csv.reader对象
    # head_row = next(csv_file)  # 去除第一行标题

    for row in csv_file.values:
        historical_stock = dict()

        # 判断是否有数据，不用判断，空的csv会自动跳过
        # stock_mongo_data = collection.find({"stock_code": row[1].replace("'",""), "date": row[0]}).sort([("date", -1)]).limit(1)
        #
        # if stock_mongo_data.count() > 0 :
        #     continue

        historical_stock['date'] = row[0]
        historical_stock['stock_code'] = row[1].replace("'","")
        historical_stock['stock_name'] = row[2]
        historical_stock["close_price"] = row[3]
        historical_stock["highest_price"] = row[4]
        historical_stock["lowest_price"] = row[5]
        historical_stock["open_price"] = row[6]
        historical_stock["last_closing_price"] = row[7]
        historical_stock["rise_amount"] = row[8]
        historical_stock["rise_range"] = row[9]
        historical_stock["turn_over_rate"] = row[10]
        historical_stock["turn_over_volume"] = row[11]
        historical_stock["turn_over_amount"] = row[12]
        historical_stock["market_value"] = row[13]
        historical_stock["circulation_market_value"] = row[14]

        # print(historical_stock)
        try:
            collection.insert_one(historical_stock)
        except errors.DuplicateKeyError:
            continue
            # pass


def read_files(file_path):
    file_list = os.listdir(file_path)  # 读取路径下所有文件名
    for file in file_list:
        if file[-3:] != 'csv':  # 检查是不是csv文件
            continue
        path = os.path.join(file_path, file)  # 通过文件夹路径和文件名生成文件绝对路径
        print(path)
        stock_file = codecs.open(path, 'rb', 'gbk')  # 使用适合的编码打开文件，不知道的话就多试几下
        save_historical_stock(stock_file)


if __name__ == '__main__':
    client = MongoClient(MONGO_URI)  # 连接mongodb
    client.admin.authenticate(MONGO_USERNAME, MONGO_PWD)
    db = client[MONGO_DB]  # 选择库
    collection = db['stock_data']  # 选择表
    read_files(os.path.abspath('') + '\..' + '\historical_data')
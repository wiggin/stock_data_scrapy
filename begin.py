from scrapy import cmdline

# 保存所有股票号码以及名称
# cmdline.execute("scrapy crawl save_all_stock_code".split())

# 保存股票历史数据
cmdline.execute("scrapy crawl download_historical_stock".split())
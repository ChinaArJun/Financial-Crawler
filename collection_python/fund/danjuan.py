# -*- coding: utf-8 -*-

"""
抓取蛋卷基金数据。主要是为了导出我关注的一些优秀主动基金的季度重仓股。随手写的仅供参考

pip install requests
pip install SQLAlchemy -i https://pypi.doubanio.com/simple --user
pip install pymysql -i https://pypi.doubanio.com/simple --user

# https://github.com/numpy/numpy/issues/15947 numpy 版本高 mac 有问题
pip3 install numpy==1.18.0 -i https://pypi.doubanio.com/simple
pip3 install pandas -i https://pypi.doubanio.com/simple
pip3 install openpyxl -i https://pypi.doubanio.com/simple

"""

import collections
import json
import random
import time

import pandas as pd
import requests
import sqlalchemy as db

"""
curl 'https://danjuanfunds.com/djapi/fund/detail/110011' \
  -H 'Connection: keep-alive' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36' \
  -H 'elastic-apm-traceparent: 00-ce58f82d3f8e8e6fcd54397fd0f38574-adb2e157029a1192-01' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Referer: https://danjuanfunds.com/funding/110011?channel=1300100141' \
  -H 'Accept-Language: zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7' \
  -H 'Cookie: device_id=web_SkS2df508; _ga=GA1.2.1397406205.1593612461; gr_user_id=2ac24d8b-927e-475d-8f29-27589058f70f; Hm_lvt_d8a99640d3ba3fdec41370651ce9b2ac=1602344637,1602344758,1602344764,1603369369; acw_tc=2760822016038852981828649e5a0c60b368285cb530c1a2e8d169c1867d83; xq_a_token=c2974070ad952835feab798d5278f70696c9f25c; Hm_lpvt_d8a99640d3ba3fdec41370651ce9b2ac=1603885299; channel=1300100141; timestamp=1603885317702' \
  --compressed
"""


def get_fund_json(fund_code, fund_name="", to_file=True):
    """抓取蛋卷基金数据"""
    url = "https://danjuanfunds.com/djapi/fund/detail/{}".format(fund_code)
          # "https://danjuanfunds.com/djapi/fund/detail/005825"
    print("抓取:{} {}".format(fund_code, fund_name))
    resp = requests.get(
        url,
        headers={
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://danjuanfunds.com/funding/110011?channel=1300100141",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
            "elastic-apm-traceparent": "00-ce58f82d3f8e8e6fcd54397fd0f38574-adb2e157029a1192-01"},
        cookies={
            "Hm_lpvt_d8a99640d3ba3fdec41370651ce9b2ac": "1603885299",
            "Hm_lvt_d8a99640d3ba3fdec41370651ce9b2ac": "1602344637,1602344758,1602344764,1603369369",
            "_ga": "GA1.2.1397406205.1593612461",
            "acw_tc": "2760822016038852981828649e5a0c60b368285cb530c1a2e8d169c1867d83",
            "channel": "1300100141",
            "device_id": "web_SkS2df508",
            "gr_user_id": "2ac24d8b-927e-475d-8f29-27589058f70f",
            "timestamp": "1603885317702",
            "xq_a_token": "c2974070ad952835feab798d5278f70696c9f25c"},
    )
    jsontext = resp.text  # 重定向到 json 文件可以格式化查看

    if to_file:  # 写到本地 json 文件方便调试
        filename = "./requestJson/{}.json".format(fund_code)
        with open(filename, "w") as f:
            json.dump(resp.json(), f, indent=2, ensure_ascii=False)

    return jsontext


Connection = None
Table = None


def init_conn():
    global Connection, Table  # 全局使用
    url = "mysql+pymysql://root:root@127.0.0.1:3306/python_fund"  # 测试地址，改成你的本地 mysql 数据库地址
    engine = db.create_engine(url)
    metadata = db.MetaData()
    Connection = engine.connect()
    Table = db.Table('danjuan_fund', metadata, autoload=True, autoload_with=engine)


def fund_name_from_manager_list(fund_code, achievement_list):
    """每个管理者都会管理很多基金，找到当前这个基金并返回名字"""
    for fund in achievement_list:
        if fund.get('fund_code') == fund_code:
            return fund['fundsname']
    return ""


def get_managers(manager_list):
    """找到所有基金管理人返回空格分割字符串 eg: 李晓星 张坤"""
    name_list = []
    for manager in manager_list:
        name_list.append(manager['name'])
    return " ".join(name_list)


def parse_danjuan_fund(fund_code, json_text):
    d = json.loads(json_text)
    print("抓取状态:%s"%d)
    print('manager_list' in d)
    if 'manager_list' in d == False or 'data' in d == False:
        return "", "", ""
    if 'manager_list' in d['data'] == False or 'data' in d == False:
        return "", "", ""
    if 'manager_list' in d['data'] == False or 'data' in d == False:
        return "", "", ""
    if d['result_code'] == 600001 or 'manager_list' in d['data'] == False:
        return "", "", ""
    data = d['data']
    manager_list = data['manager_list']
    if len(manager_list) <= 0:
        return "", "", ""
    achievement_list = manager_list[0]['achievement_list']  # 找到一个管理者

    fund_name = fund_name_from_manager_list(fund_code, achievement_list)
    managers = get_managers(manager_list)
    try:
        enddate = data['fund_position']['enddate']  # 季报披露日期
    except KeyError:
        enddate = ""
    return fund_name, managers, enddate


# 在你的 mysql 创建这个表
"""
CREATE TABLE `danjuan_fund` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `fund_name` varchar(64) DEFAULT '' COMMENT '基金名称',
    `fund_code` varchar(16) NOT NULL DEFAULT '' COMMENT '基金代码',
    `managers` varchar(32) NOT NULL DEFAULT '' COMMENT '管理人',
    `enddate` varchar(32) NOT NULL DEFAULT '' COMMENT '季报日期',
    `type` varchar(32) NOT NULL DEFAULT '' COMMENT '基金类型',
    `detail_json` text NOT NULL COMMENT '蛋卷基金详细信息 json',
    PRIMARY KEY (`id`),
    KEY `idx_code` (`fund_code`),
    KEY `idx_name` (`fund_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def save_mysql(fund_code, fund_name, managers, enddate, detail_json, type):
    query = db.insert(Table).values(
        fund_name=fund_name,
        fund_code=fund_code,
        managers=managers,
        enddate=enddate,
        detail_json=detail_json,
        type=type,
    )
    Connection.execute(query)


def request_and_save(fund_code, fund_name, type):
    json_text = get_fund_json(fund_code, fund_name)
    fund_name, managers, enddate = parse_danjuan_fund(fund_code, json_text)
    if fund_code != '':
        save_mysql(fund_code, fund_name, managers, enddate, json_text, type)


def get_my_xueqiu_fund_codes(type):
    codes = []  # [ (code, name) ]
    """ 从我的雪球获取我关注的所有基金代码。如果你有雪球账号并且有关注的基金，可以用这段代码自动化查询。
    https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&pid=-110&category=2
    """
    # resp = requests.get(
    #     "https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?size=1000&pid=-110&category=2",
    #     headers={
    #         "Accept": "application/json, text/plain, */*",
    #         "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    #         "Connection": "keep-alive",
    #         "Origin": "https://xueqiu.com",
    #         "Referer": "https://xueqiu.com/",
    #         "Sec-Fetch-Dest": "empty",
    #         "Sec-Fetch-Mode": "cors",
    #         "Sec-Fetch-Site": "same-site",
    #         "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"},
    #     cookies={}, # TODO 用浏览器查询请求，改成你自己的 uncurl requests 代码，注意你的 cookie 不要随便泄露出去
    # )
    #
    # with open("./funds.json", "w") as f:
    #     json.dump(resp.json(), f, indent=2, ensure_ascii=False)
    #




    # with open("./funds.json") as f:
    #     res = json.load(f)
    #     stocks = res['data']['stocks']
    #     for stock in stocks:
    #         symbol = stock['symbol']
    #         fund_code = ''.join(char for char in symbol if char.isdigit())
    #         fund_name = stock['name']
    #         codes.append((fund_code, fund_name))
    # return codes
    return get_danke_all_funds(type)

def get_danke_all_funds(type = 1):
    page =  1
    currentItems = [1]
    items = []
    codes = []  # [ (code, name) ]

    # with open("./all_funds_%s.json"%type) as f:
    #     res = json.load(f)
    #     if len(res) > 0:
    #         for item in res:
    #             fund_code = item["fd_code"]
    #             fund_name = item['fd_name']
    #             codes.append((fund_code, fund_name))
    #             items.append(item)
    #         currentItems = []
    #         return codes


    while len(currentItems) != 0:
        resp = requests.get(
            "https://danjuanfunds.com/djapi/v3/filter/fund?type=%s&order_by=1m&size=100&page=%s"%(type,page),
            headers={
                "Accept": "application/json; charset=utf-8",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Origin": "https://danjuanfunds.com/rank/performance",
                "Referer": "https://danjuanfunds.com/rank/performance",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"},
            cookies={

            },  # TODO 用浏览器查询请求，改成你自己的 uncurl requests 代码，注意你的 cookie 不要随便泄露出去
        )
        print(resp.text)

        res = json.loads(resp.text)
        for item in res['data']['items']:
            fund_code = item["fd_code"]
            fund_name = item['fd_name']
            codes.append((fund_code, fund_name))
            items.append(item)
        currentItems = res['data']['items']
        page+=1
    with open("all_funds_%s.json"%type, "w") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
    return codes

def crawl_all_my_funds_to_mysql(type):
    funds = get_my_xueqiu_fund_codes(type)
    __import__('pprint').pprint(funds)

    for fund_code, fund_name in funds:
        request_and_save(fund_code, fund_name, type)
        # time.sleep(random.randint(5, 10))  # 注意慢一点，随机 sleep 防止命中反作弊


def export_all_mysql_funds_stocks_to_dict():
    """导出所有基金的股票前十大重仓股票到 excel"""
    query = db.select([Table]).order_by(db.desc(Table.columns.id)).limit(100)
    rows = Connection.execute(query).fetchall()
    fund_dict = {}
    for row in rows:
        d = json.loads(row.detail_json)
        try:
            stock_list = d['data']['fund_position']['stock_list']
        except KeyError:  # 新基金没披露可能为空
            stock_list = []

        stocks = []
        for stock in stock_list:
            name, code, percent = stock['name'], stock['code'], stock['percent']
            stock_fmt = u"{}[{}]({}%)".format(name, code, percent)
            stocks.append(stock_fmt)

        if len(stocks) < 10: # 十大重仓股
            stocks += (10 - len(stocks)) * [""]

        key = row.fund_name
        vals = [row.fund_code, row.managers, row.enddate] + stocks
        fund_dict[key] = vals

    return fund_dict


# https://www.geeksforgeeks.org/how-to-create-dataframe-from-dictionary-in-python-pandas/
def export_all_mysql_funds_stocks_to_excel_vertical():
    fund_dict = export_all_mysql_funds_stocks_to_dict()
    index = ['代码', '管理人', '季报日期'] + ['重仓股'] * 10  # 十大重仓
    df = pd.DataFrame(fund_dict, index=index)
    df.to_excel("./funds_stock_vertical.xlsx")


def export_all_mysql_funds_stocks_to_excel():  # 横着
    fund_dict = export_all_mysql_funds_stocks_to_dict()
    # index = ['代码', '管理人', '季报日期'] + ['重仓股'] * 10
    df = pd.DataFrame.from_dict(fund_dict, orient='index')
    df.to_excel("./fund_stock.xlsx")


def export_all_stock_funds():
    """导出每个股票被多少基金持有，比较容易看出哪些股票被抱团"""
    query = db.select([Table]).order_by(db.desc(Table.columns.id)).limit(10000)
    rows = Connection.execute(query).fetchall()
    stock_funds = collections.defaultdict(list)
    for row in rows:
        d = json.loads(row.detail_json)
        try:
            stock_list = d['data']['fund_position']['stock_list']
        except KeyError:  # 新基金没披露可能为空
            stock_list = []

        for stock in stock_list:
            name = stock['name'] if 'name' in stock else ""
            code, percent = stock['code'], stock['percent']
            stock_name = u"{}({})".format(name, code)
            stock_funds[stock_name].append(row.fund_name)

    keys = sorted(stock_funds, key=lambda k: len(stock_funds[k]), reverse=True)
    sorted_stock_dict = {k: stock_funds[k] for k in keys}
    df = pd.DataFrame.from_dict(sorted_stock_dict, orient='index')
    df.to_excel("./stock.xlsx")


def main():
    # get_fund_json("007300", "汇添富中盘", True) # 单独抓取一个基金数据到文件
    crawl_all_my_funds_to_mysql(1) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(2) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(3) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(4) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(5) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(6) # 抓取所有我关注的雪球上的基金到 mysql

    crawl_all_my_funds_to_mysql(11) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(1001) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(1002) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(1003) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(1004) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(1005) # 抓取所有我关注的雪球上的基金到 mysql

    crawl_all_my_funds_to_mysql(2001) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(2002) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(2003) # 抓取所有我关注的雪球上的基金到 mysql
    crawl_all_my_funds_to_mysql(2004) # 抓取所有我关注的雪球上的基金到 mysql

    crawl_all_my_funds_to_mysql(2005) # 抓取所有我关注的雪球上的基金到 mysql

    export_all_mysql_funds_stocks_to_excel_vertical()  # 导出基金十大重仓股
    export_all_mysql_funds_stocks_to_excel()  # 导出横版基金十大重仓

    export_all_stock_funds()  # 导出每个重仓股票分别被多少基金持有


if __name__ == "__main__":
    init_conn()
    main()
    print("success")

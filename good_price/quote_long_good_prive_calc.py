#-*-coding:utf-8-*

import codecs
import urllib2
import json
import bs4
import time
import requests
from interval import Interval
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located

CN_symbol = u'代码'
CN_name = u'名称'
CN_pe_ttm = u'ttm市盈率'
CN_dividend = u'ttm股息'
CN_dividend_yield = u'ttm股息率'
CN_current = u'最新价格'
CN_high52w = u'52周最高价'
CN_low52w = u'52周最低价'

def print_json(js, info ='', support_cn = True):
    if len(info) > 0:
        info += ': '
    if js is None:
        print info,"is None"
    else:
        if support_cn:
            print info + json.dumps(js, encoding='UTF-8', ensure_ascii=False, sort_keys=True)
        else:
            print info,js

class ReqXueqiuByWebDriver:
    def __init__(self):
        self.driver = webdriver.Firefox()
        self.wait = WebDriverWait(self.driver, 10)
        pass

    def __del__(self):
        self.driver.close()

    def http_get(self, url, headers={}, data = None):
        print 'url:',url
        try:
            page=urllib2.urlopen(url, data, headers)
            return page.read()
            hm=page.read().decode('gbk', errors='ignore').encode('utf-8', errors='ignore')
            return hm
        except Exception,e:
            print Exception,":",e
            return ''

    '''
    #from https://xueqiu.com/S/%s
    #json接口，不能直接请求 https://stock.xueqiu.com/v5/stock/quote.json?symbol=SH603886&extend=detail
    '''
    def req_quote_ttm(self, symbol):
        url = "https://xueqiu.com/S/%s" % (symbol)
        self.driver.refresh()
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        data = {CN_symbol:symbol}
        name = soup.find('div', attrs={'class':'stock-name'}).get_text()
        data[CN_name] = name #name[:name.find('(')].encode('utf-8')
        current = soup.find('div', attrs={'class':'stock-current'}).find('strong').get_text()
        current = current[1:]
        float(current)
        data[CN_current] = current
        quote_info_tbl = soup.find('table', attrs={'class':'quote-info'})
        pe_ttm = quote_info_tbl.find(text=u'市盈率(TTM)：').parent()[0].get_text()
        if pe_ttm == u'亏损':
            pe_ttm = '-1.0'
        data[CN_pe_ttm] = pe_ttm
        dividend = quote_info_tbl.find(text=u'股息(TTM)：').parent()[0].get_text()
        float(dividend)
        data[CN_dividend] = dividend
        dividend_yield = quote_info_tbl.find(text=u'股息率(TTM)：').parent()[0].get_text()
        float(dividend_yield[:-1])
        data[CN_dividend_yield] = dividend_yield
        high52w = quote_info_tbl.find(text=u'52周最高：').parent()[0].get_text()
        float(high52w)
        data[CN_high52w] = high52w
        low52w = quote_info_tbl.find(text=u'52周最低：').parent()[0].get_text()
        float(low52w)
        data[CN_low52w] = low52w
        return data

    '''
    #中国10年期国债收益率：http://yield.chinabond.com.cn/cbweb-czb-web/czb/moreInfo?locale=cn_ZH
    return fload
    '''
    def req_chinabond(self):
        url = "http://yield.chinabond.com.cn/cbweb-czb-web/czb/moreInfo?locale=cn_ZH"
        driver = webdriver.Firefox()
        driver.get(url)
        #soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        try:
            #five_name = self.driver.find_element_by_xpath('//*[@id="gjqxData"]/table/tbody/tr[6]/td[1]').text
            #five_value = self.driver.find_element_by_xpath('//*[@id="gjqxData"]/table/tbody/tr[6]/td[2]').text
            #print five_name,five_value
            #if five_name == u'5年':
            #    data['5years'] = float(five_value)
            ten_name = driver.find_element_by_xpath('//*[@id="gjqxData"]/table/tbody/tr[8]/td[1]').text
            ten_value = driver.find_element_by_xpath('//*[@id="gjqxData"]/table/tbody/tr[8]/td[2]').text
            #print ten_name,ten_value
            if ten_name == u'10年':
                driver.close()
                return float(ten_value)
        except Exception,e:
            print '[req_chinabond]',e
        driver.close()
        raise Exception('[req_chinabond] : parse error')

    '''
    #深证A股市盈率：https://www.legulegu.com/stockdata/shenzhenPE
    return: float
    '''
    def req_shenzhenPE(self):
        url = "https://www.legulegu.com/stockdata/shenzhenPE"
        driver = webdriver.Firefox()
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.close()
        try:
            txt = soup.find('td', id="table-average-value").get_text()
            return float(txt)
        except Exception,e:
            print '[req_shenzhenPE]',e
        raise Exception('[req_shenzhenPE] :req error')

CN_chinabond10 = u'10年期国债收益率'
CN_szpe = u'深证A股平均市盈率'
#CN_symbol = u'代码'
#CN_name = u'名称'
#CN_pe_ttm = u'ttm市盈率'
#CN_dividend = u'ttm股息'
#CN_dividend_yield = u'ttm股息率'
#CN_current = u'最新价格'
#CN_high52w = u'52周最高价'
#CN_low52w = u'52周最低价'
CN_goodbuy_pe15 = u'买入好价-15倍市盈率'
CN_goodbuy_cb10 = u'买入好价-10年期国债收益'
CN_normalbuy_pe25 = u'买入合理价格-25倍市盈率'
CN_sell_pe = u'卖出好价-50倍市盈率'
CN_sell_dividend = u'卖出好价-股息率计算'
CN_ratio_buy_current = u'收益率-当前价买入'
CN_ratio_buy_good = u'收益率-按好价买入'
CN_ratio_buy_normal = u'收益率-按正常价买入'
CN_buy_info = u'是否可买入'
CN_sell_info = u'是否可卖'

def calc_good_price(chinabond10, szpe, symbol, ttm_data):
    data = ttm_data
    data[CN_chinabond10] = "%.3f%%" % (chinabond10*100)
    data[CN_szpe] = str(szpe)
    current = float(ttm_data[CN_current])
    pe_ttm  = float(ttm_data[CN_pe_ttm])
    dividend  = float(ttm_data[CN_dividend])
    dividend_yield = float(ttm_data[CN_dividend_yield][:-1]) / 100
    #print pe_ttm,ttm_data[CN_pe_ttm]
    if (pe_ttm <= 0.001) or (dividend <= 0.001) :
        return data
    buy_good_price = 0.0
    buy_good_price_h = 0.0
    #buy-好价:通过市盈率计算好价格，X=15 *（当前股价 / 个股TTM市盈率）
    buy_good_price = 15 * current / pe_ttm
    data[CN_goodbuy_pe15] = "%.2f" % (buy_good_price)
    #buy-好价:通过股息率计算好价格，X=每股股息 /十年期国债收益率 
    goodbuy_cb10 = dividend / chinabond10
    data[CN_goodbuy_cb10] = "%.2f" % (goodbuy_cb10)
    if buy_good_price > goodbuy_cb10:
        buy_good_price_h = buy_good_price
        buy_good_price = goodbuy_cb10
    else:
        buy_good_price_h = goodbuy_cb10
    #buy-正常价:通过市盈率计算好价格，X=25 *（当前股价 / 个股TTM市盈率）
    buy_normal = 25 * current / pe_ttm
    data[CN_normalbuy_pe25] = "%.2f" % (buy_normal)
    #sell:通过市盈率计算好价格，X=50 *（当前股价 / 个股TTM市盈率）
    sell_price = 50 * current / pe_ttm
    data[CN_sell_pe] = "%.2f" % (sell_price)
    #sell:通过股息率计算好价格，X=每股股息 / (十年期国债收益率÷3)
    sell_price1 = goodbuy_cb10 * 3
    data[CN_sell_dividend] = "%.2f" % (sell_price1)
    if sell_price < sell_price1:
        sell_price = sell_price1

    #收益率:按照当前价购买
    ratio = (sell_price - current) / current
    data[CN_ratio_buy_current] = "%.2f%%" % (ratio * 100)
    #收益率:按照好价购买
    ratio = (sell_price - buy_good_price) / buy_good_price
    data[CN_ratio_buy_good] = "%.2f%%" % (ratio * 100)
    #收益率:按照正常价购买
    ratio = (sell_price - buy_normal) / buy_normal
    data[CN_ratio_buy_normal] = "%.2f%%" % (ratio * 100)
    print_json(data)

    '''购买条件判断:两个条件同时满足时即可买入
    1、 深证 A 股的市盈率小于 20 且目标公司股票的 TTM 市盈率小于 15；
    2、 动态股息率大于 10 年期国债收益率。
    '''
    info = u''
    def level(levels, v):
        lv = 0
        for (a,b) in levels:
            lv += 1
            if v in Interval(a,b):return lv
    level_list = []
    #市场风险
    market_intervals = [(1,20), (20,30), (30, 40), (40,60), (60,1000), (-100, 0)]
    level_szpe = level(market_intervals, szpe)
    level_list.append((level_szpe, u'市场风险'))

    #该股市盈率风险
    pettm_intervals = [(1,20), (20,30), (30, 40), (40,60), (60,1000), (-100, 0)]
    level_ttmpe = level(pettm_intervals, pe_ttm)
    level_list.append((level_ttmpe, u'市盈率风险'))

    #价格风险
    price_intervals = [(0,buy_good_price), (buy_good_price,buy_good_price_h), (buy_good_price_h, buy_normal), (buy_normal, sell_price), (sell_price, sell_price*10)]
    level_price = level(price_intervals, current)
    level_list.append((level_price, u'价格风险'))

    #动态股息风险(公司盈利情况)
    dividend_intervals = [(chinabond10,chinabond10*10000), (chinabond10/1.5,chinabond10), (chinabond10/2, chinabond10/1.5), (chinabond10/3, chinabond10/2), (-100, chinabond10/3)]
    level_dividend = level(dividend_intervals, dividend_yield)
    level_list.append((level_dividend, u'公司盈利风险(动态股息率)'))

    #print_json(dividend_intervals)
    #print_json(level_list)
    lv_avg = sum([l for (l,t) in level_list if l is not None]) / len(level_list)
    lv_max = max([l for (l,t) in level_list if l is not None])
    if lv_avg <= 2 and lv_max <=2:
        info = u'%d:低风险 ' % (lv_avg)
    elif (lv_avg <= 4):
        info = u'%d:中风险 ' % (lv_avg)
    else:
        info = u'%d:高风险 ' % (lv_avg)
    for (l,t) in level_list:
        info += t + "(%d);"  % l
    #info += '-'.join([[t for (l,t) in level_list]])

    data[CN_buy_info] = info

    '''卖出条件判断:当【深证A股的市盈率】>60 或  该股【ttm市盈率】>50  或 【ttm股息】< 【10年期国债收益率】的1/3    都是卖出的好时机
    1.目标公司市盈率50倍以上，深证A股市盈率60倍左右
    2.动态股息率小于10年期国债收益率三分之一
    3.目标公司有变坏的迹象
    '''
    info = u''
    if szpe >= 60 or pe_ttm > 50 or dividend < chinabond10/3:
        info = u'卖:赶紧卖，达到最佳卖点'
    else:
        if szpe >= 50 or pe_ttm > 40 or dividend < chinabond10/2:
            info = u'减仓：高风险'
        elif szpe >= 40 or pe_ttm > 35:
            info = u'适当建仓:中等风险'
        else:
            info = u'继续持有'
    data[CN_sell_info] = info
    print_json(data)
    return data

CVS_TITLES = [CN_chinabond10, CN_szpe, CN_symbol, CN_name, CN_pe_ttm, CN_dividend, CN_dividend_yield, CN_current, CN_high52w, CN_low52w,
            CN_goodbuy_pe15, CN_goodbuy_cb10, CN_normalbuy_pe25, 
            CN_sell_pe, CN_sell_dividend, 
            CN_ratio_buy_current, CN_ratio_buy_good, CN_ratio_buy_normal,
            CN_buy_info, CN_sell_info]
def save_to_cvs(fo, symbol, itemdata = {}):
    #print_json(itemdata, symbol, support_cn=False)
    if len(itemdata) == 0:
        fo.write(symbol + u'\n')
    for key in CVS_TITLES:
        if (itemdata is not None) and (key in itemdata.keys()):
            fo.write(itemdata[key])
        fo.write(',')
    fo.write('\n')
    fo.flush()

def req_quote(quote_list):
    req = ReqXueqiuByWebDriver()
    chinabond10 = 3.20 #req.req_chinabond()
    chinabond10 = chinabond10 / 100
    print 'chinabond10:',chinabond10
    szpe = 33.39 #req.req_shenzhenPE()
    print 'szpe:',szpe
    fo = codecs.open('good_price.csv', 'w+', encoding='gbk')
    #fo.write(CN_chinabond10 + ',' + "%.3f%%\n" % (chinabond10*100))
    #fo.write(CN_szpe + ',' + str(szpe) + '\n')
    fo.write(','.join(CVS_TITLES) + '\n')
    for (code, mk, symbol) in quote_list:
        print symbol,'...'
        if True:
            try:
                ttm_data = req.req_quote_ttm(symbol)
                print_json(ttm_data)
                itemdata = calc_good_price(chinabond10, szpe, symbol, ttm_data)
                save_to_cvs(fo, symbol, itemdata)
            except Exception,e1:
                print "[req_quote]",e1
                data = {CN_symbol:symbol, CN_szpe:str(szpe), CN_chinabond10:"%.3f%%" % (chinabond10*100)}
                save_to_cvs(fo, symbol, data)
                continue
        else:
            ttm_data = req.req_quote_ttm(symbol)
            print_json(ttm_data)
            itemdata = calc_good_price(chinabond10, szpe, symbol, ttm_data)
            save_to_cvs(fo, symbol, itemdata)
        time.sleep(2)
    fo.close()
    
def load_quotes(filename='list.txt'):
    quote_list = []
    with open(filename, 'r') as fi:
        for line in fi:
            arr = line.strip('\r').strip('\n').split('.')
            if len(arr) >= 2:
                quote_list.append((arr[0], arr[1], arr[1] + arr[0]))
        fi.close()
    return quote_list

#req_quote([('600519', 'SH', 'SH600030')])
#req_quote([('600519', 'SH', 'SZ000100'), ('600885', 'SH', 'SZ000568'), ('002833', 'SZ', 'SZ002081'), ('002833', 'SZ', 'SZ002613'), ('002833', 'SZ', 'SH600331'), ('002833', 'SZ', 'SH600396'), ('002833', 'SZ', 'SH601318')])
#req_quote([('600519', 'SH', 'SH600519'), ('600885', 'SH', 'SH600885'), ('002833', 'SZ', 'SZ002833')])
req_quote(load_quotes())

print 'finish...'
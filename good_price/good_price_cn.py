#-*-coding:utf-8-*

import codecs
import json
import time
import requests
from interval import Interval
import sys
import datetime
import os
sys.path.append("../fox_data")
from index import *
import chinabond
import xueqiu
import alog

import util

#CN_market = u'_市场环境'
CN_chinabond10 = u'10年期国债收益率'
CN_szpe = u'深证A股平均市盈率'

#CN_base = u'基本信息'
CN_symbol = u'0代码'
CN_name = u'0名称'
CN_pe_ttm = u'ttm市盈率'
CN_dividend = u'ttm股息'
CN_dividend_yield = u'ttm股息率'
CN_current = u'最新价'
CN_high52w = u'最高价(52周)'
CN_low52w = u'最低价(52周)'

#CN_goodprice = u'_好价格信息'
CN_goodbuy_pe15 = u'买入好价(15倍市盈率)'
CN_goodbuy_cb10 = u'买入好价(10年期国债收益)'
CN_normalbuy_pe25 = u'买入合理价格(25倍市盈率)'
CN_sell_pe = u'卖出好价(50倍市盈率)'
CN_sell_dividend = u'卖出好价(股息率计算)'
CN_ratio_buy_current = u'收益率(当前价买入)'
CN_buy_info = u'评估买入建议'
CN_sell_info = u'评估卖出建议'

class DataGether(object):
    @staticmethod
    def _cache(key, set_value=None):
        dts = datetime.datetime.now().strftime('%Y%m%d')
        nkey = key + dts
        if set_value is None:
            if os.environ.get(nkey) is not None:
                return os.environ[nkey]
            raise Exception('env not key:' + nkey)
        else:
            os.environ[nkey] = set_value
            return set_value

    @staticmethod
    def chinabond10():
        ckey = 'chinabond10'
        try:
            value = DataGether._cache(ckey)
            return float(value)
        except:
            chinabond10 = chinabond.chinabond()
            chinabond10 = chinabond10 / 100
            alog.info("http get:" + str(chinabond10), 'good_price', 'DataGether::chinabond10')
            DataGether._cache(ckey, str(chinabond10))
            return chinabond10

    @staticmethod
    def szpe():
        ckey = 'szpe'
        try:
            value = DataGether._cache(ckey)
            return float(value)
        except:
            pe = ShenZhen.szpe()
            alog.info("http get:" + str(pe), 'good_price', 'DataGether::szpe')
            DataGether._cache(ckey, str(pe))
            return pe

    @staticmethod
    def quote_ttm(symbol):
        xueqiu.set_token('xq_a_token=db48cfe87b71562f38e03269b22f459d974aa8ae')
        resp = xueqiu.fetch(symbol, 'quote_detail', params={})
        #jsresp = json.loads(resp, encoding='utf-8')
        #util.print_json(resp, 'xueqiu.fetch quote_detail:'+symbol)
        quote=resp['data']['quote']
        data = {CN_symbol:symbol}
        data[CN_name] = quote['name']
        data[CN_current] = str(quote['current'])
        data[CN_pe_ttm] = str(quote['pe_ttm'])
        data[CN_dividend] = str(quote['dividend'])
        data[CN_dividend_yield] = str(quote['dividend_yield'])
        data[CN_high52w] = str(quote['high52w'])
        data[CN_low52w] = str(quote['low52w'])
        return data

class GoodPriceCalc(object):
    def __init__(self):
        self.chinabond10 = 0.0
        self.market_pe   = 0.0

    def calc_good_price(self, symbol, ttm_data):
        data = {}
        current = float(ttm_data[CN_current])
        pe_ttm  = float(ttm_data[CN_pe_ttm])
        dividend  = float(ttm_data[CN_dividend])
        dividend_yield = float(ttm_data[CN_dividend_yield][:-1]) / 100
        #print pe_ttm,ttm_data[CN_pe_ttm]
        if (pe_ttm <= 0.001) or (dividend <= 0.001) :
            return {'error':'pe_ttm:'+str(pe_ttm)+' dividend:'+str(dividend)}
        buy_good_price = 0.0
        buy_good_price_h = 0.0
        #buy-好价:通过市盈率计算好价格，X=15 *（当前股价 / 个股TTM市盈率）
        buy_good_price = 15 * current / pe_ttm
        data[CN_goodbuy_pe15] = "%.2f" % (buy_good_price)
        #buy-好价:通过股息率计算好价格，X=每股股息 /十年期国债收益率 
        goodbuy_cb10 = dividend / self.chinabond10
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
        #util.print_json(data)

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
        level_szpe = level(market_intervals, self.market_pe)
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
        if dividend_yield > self.chinabond10:
            level_dividend = 1
            level_list.append((level_dividend, u'公司盈利风险(动态股息率)'))
        elif dividend_yield < self.chinabond10/3:
            level_dividend = 5
            level_list.append((level_dividend, u'公司盈利风险(动态股息率)'))

        #print_json(dividend_intervals)
        #print_json(level_list)
        lv_avg = sum([l for (l,t) in level_list if l is not None]) / len(level_list)
        lv_max = max([l for (l,t) in level_list if l is not None])
        if (lv_avg <= 2 and lv_max <=2) or (dividend_yield > self.chinabond10 and self.market_pe < 20 and pe_ttm < 16):
            info = u'%d:买，低风险，可以考虑购买 ' % (lv_avg)
        elif (lv_avg < 4):
            info = u'%d:观望，中风险，可以继续观望 ' % (lv_avg)
        else:
            info = u'%d:观望，高风险,等待好价格 ' % (lv_avg)
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
        if self.market_pe >= 60 or pe_ttm > 50 or dividend < self.chinabond10/3:
            info = u'卖:赶紧卖，达到最佳卖点'
        else:
            if self.market_pe >= 50 or pe_ttm > 40:
                info = u'减仓：高风险'
            elif self.market_pe >= 40 or pe_ttm > 35:
                info = u'适当建仓:中等风险'
            else:
                info = u'继续持有'
        data[CN_sell_info] = info
        #util.print_json(data)
        return data

    '''input:
        codes:      eg: '000001,600300,300200'
    '''
    def calc(self, codes, use_cache=True):
        code_list = codes.split(',')
        if len(codes) < 5 or len(code_list) < 1:
            return {'code':{'error':'codes is error:'+codes}}
        dts = datetime.datetime.now().strftime('%Y%m%d')
        cachefile = '.cache/' + codes + '-' + dts + '.json'
        if use_cache is True:
            try:
                redata = util.load_json(cachefile)
                alog.info('hit cache:'+codes + ' cache file:'+cachefile, 'good_price', 'calc')
                return redata
            except: pass
        self.chinabond10 = DataGether.chinabond10()
        self.market_pe   = DataGether.szpe()
        market_data = {CN_chinabond10:"%.3f%%" % (self.chinabond10*100), CN_szpe:str(self.market_pe)}
        alog.info('market_pe:' + str(self.market_pe) + ' chinabond10:' + str(self.chinabond10), 'good_price', 'calc')
        redata = {}
        for code in code_list:
            symbol = util.code2symbal(code)
            alog.info(symbol + '...', 'good_price', 'calc')
            if True:
                try:
                #if True:
                    ttm_data = DataGether.quote_ttm(symbol)
                    #util.print_json(ttm_data)
                    itemdata = {}
                    itemdata['a'] = ttm_data
                    itemdata['b'] = market_data
                    itemdata['c'] = self.calc_good_price(symbol, ttm_data)
                    redata[symbol] = itemdata
                #else:
                except Exception as e1:
                    print("[req_quote] error:" + str(e1))
                    itemdata = {}
                    itemdata['a'] = {CN_symbol:symbol}
                    itemdata['b'] = market_data
                    itemdata['c'] = {}
                    redata[symbol] = itemdata
                    continue
            else:
                ttm_data = DataGether.quote_ttm(symbol)
                #util.print_json(ttm_data)
                itemdata = self.calc_good_price(symbol, ttm_data)
                redata[symbol] = itemdata
            time.sleep(2)
        util.dump_json(cachefile, redata)
        return redata

    def save_to_cvs(self, price_data, file='good_price.csv'):
        CVS_TITLES = [CN_chinabond10, CN_szpe, CN_symbol, CN_name, CN_pe_ttm, CN_dividend, CN_dividend_yield, CN_current, CN_high52w, CN_low52w,
            CN_goodbuy_pe15, CN_goodbuy_cb10, CN_normalbuy_pe25, 
            CN_sell_pe, CN_sell_dividend, 
            CN_ratio_buy_current,
            CN_buy_info, CN_sell_info]
        titles_info = [{} for i in range(10)]
        for (symbol, itemdata) in price_data.items():
            for idx in range(len(itemdata)):
                if len(itemdata[idx]) > len(titles_info[idx]):
                    titles_info[idx] = itemdata[idx].keys()
        with codecs.open('good_price.csv', 'w+', encoding='gbk') as fo:
            fo.write(','.join(CVS_TITLES) + '\n')
            for (symbol, itemdata) in price_data.items():
                if len(itemdata) == 0:
                    fo.write(symbol + u'\n')
                    continue
                for idx in range(len(itemdata)):
                    for key in titles_info[idx]:
                        if key in itemdata[idx].keys():
                            fo.write(itemdata[key])
                        fo.write(',')
                fo.write('\n')
            fo.flush()
            fo.close()

    def to_json(self, price_data, indent=None):
        s = json.dumps(price_data, ensure_ascii=False, sort_keys=True, indent=indent)
        return s

    def to_text(self, price_data, split = '\n'):
        res = ''
        for (symbol, itemdata) in price_data.items():
            res += symbol + u'===:' + split
            for (a, groupdata) in sorted(itemdata.items(), key=lambda x: x[0]):
                for (k,v) in sorted(groupdata.items(), key=lambda x: x[0]):
                    res += k + ':' + v + split
        return res

def calc_code(code_list = ['600585']):
    calc = GoodPriceCalc()
    price_data = calc.calc(code_list)
    #calc.save_to_cvs(price_data)
    print(calc.to_json(price_data)) 


def calc_file(filename='list.txt'):
    code_list = []
    with open(filename, 'r') as fi:
        for line in fi:
            arr = line.strip('\r').strip('\n').split('.')
            if len(arr) >= 2:
                code_list.append(arr[0])
        fi.close()
    calc_code(code_list)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        calc_code(sys.argv[1].split(','))
        print('finish...')
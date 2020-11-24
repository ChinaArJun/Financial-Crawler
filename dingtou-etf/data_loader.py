# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os
from datetime import date


'''
    http://vip.stock.finance.sina.com.cn/mkt/#etf_hq_fund
    http://finance.sina.com.cn/fund/quotes/510050/bc.shtml
'''
class DataLoader:
    '''
    return ['sh510000':{xxx}]
    '''
    def etf_info(self, filename):
        etf_map = {}
        raw_pe_data = json.load(open(filename))
        for jitem in raw_pe_data:
            symbol  = jitem['symbol']
            etf_map[symbol] = jitem
        #print len(index_data)
        return etf_map

    '''
    return [('2020-09-01',23.5),('2020-09-02',22.5),('2020-09-03',20.5),('2020-09-04',22.5)]
    '''
    def daily_etf_data(self, filename, dt_start, dt_end):
        index_data = []
        raw_pe_data = json.load(open(filename))
        for jitem in raw_pe_data:
            fbrq  = jitem['fbrq'][:10]
            price = float(jitem['jjjz'])
            item = (fbrq, price)
            if ((len(dt_start) == 0) or (fbrq >= dt_start)) and ((len(dt_end) == 0) or (fbrq <= dt_end)):
                index_data.append(item)
            else:
                continue
        #print len(index_data)
        return index_data
        
    '''
    return {'2020-05':12.5, '2020-06':12.6, '2020-07':32.5}
    '''
    def monthly_pe_data(self, filename):
        pe_data = {}
        raw_pe_data = json.load(open(filename))
        for item in raw_pe_data:
            ym = datetime.datetime.utcfromtimestamp(item['date']/1000).strftime("%Y-%m")
            if ym >= '2000-01':
                pe_data[ym] = item['pe']
        #print len(pe_data)
        return pe_data
        

#db = IndexData('../data/510050.csv')

# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os
import util

'''
    http://vip.stock.finance.sina.com.cn/mkt/#etf_hq_fund
    单个基金页面:
    http://finance.sina.com.cn/fund/quotes/510050/bc.shtml
      '''

def history_etf(code, datefrom='', dateto='', timestamp=''):
    host='https://stock.finance.sina.com.cn/fundInfo/api/openapi.php/CaihuiFundInfoService.getNav?'
    url = host + 'symbol=' + code + '&datefrom=' + datefrom + '&dateto=' + dateto + '&_=' + timestamp
    def check_code(jdata):
        code = -1
        if ('result' in jdata) and ('status' in jdata['result']) and ('code' in jdata['result']['status']):
            code = jdata['result']['status']['code']
        return code
    jdata = util.http_get(url+'&page=1')
    code = check_code(jdata)
    if check_code(jdata) != 0:
        raise Exception(u'请求失败，返回的json种code异常:' + str(code))
    total_num = int(jdata['result']['data']['total_num'])
    max_page = (total_num + 19) / 20
    dlist = jdata['result']['data']['data']
    dlist.reverse()
    for page in range(2, max_page + 1):
        print 'page:',page,'/',max_page
        jdata = util.http_get(url+'&page=' + str(page))
        if check_code(jdata) != 0:
            raise Exception(u'请求失败，返回的json种code异常:' + str(code))
        if ('result' in jdata) and ('data' in jdata['result']) and ('data' in jdata['result']['data']):
            pagedata = jdata['result']['data']['data']
            #print pagedata
            pagedata.reverse()
            dlist = pagedata + dlist
        if page % 5 == 0:
            time.sleep(1)
    return dlist

#print history_etf('510300', datefrom='2018-01-01')

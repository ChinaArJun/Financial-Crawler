#-*-coding:utf-8-*

import codecs
import json
import time
import requests
import datetime
import util

'''
深证A股市盈率：http://www.szse.cn/api/report/index/overview/onepersistenthour/szse
return: float
    #req: http://www.szse.cn/api/report/index/overview/onepersistenthour/szse
    #resp:
'''
class ShenZhen:
    @staticmethod
    def szse_json():
        url = 'http://www.szse.cn/api/report/index/overview/onepersistenthour/szse'
        jsdata = util.http_get(url)
        #print jsdata
        if ('code' in jsdata) and (jsdata['code'] == 0):
            return jsdata
        raise Exception(jsdata['message'])

    @staticmethod
    def szpe():
        jsdata = ShenZhen.szse_json()
        for item in jsdata['result']['basicmap']['main']:
            if item['enname'] == 'Average P/E Ratio':
                return float(item['value'])
        raise Exception(u'解析深圳市场平均市盈率失败')

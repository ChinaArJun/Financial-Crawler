#-*-coding:utf-8-*

import codecs
import urllib2
import json

Ratio_date = []

class ReqSZPE:
    def http_get(url):
        print 'url:',url
        try:
            page=urllib2.urlopen(url)
            return page.read()
            hm=page.read().decode('gbk', errors='ignore').encode('utf-8', errors='ignore')
            return hm
        except Exception,e:
            print Exception,":",e
            return ''

    '''
    [
        {
            "id": 3250,
            "market": 2,
            "close": 13143.46,
            "date": 1600153203000,
            "pe": 32.02
        }
        {...}
    ]
    '''
    def req_shenzhenpe(self, symbol):
        url = "https://stock.xueqiu.com/v5/stock/quote.json?symbol=%s&extend=detail" % (symbol)
        rawdata = self.http_get(url)
        try:
            jdata = json.loads(rawdata)
            if 'quote' in jdata:
                return jdata['quote']
        except Exception,e:
            print e,'[json.loads] url:',url
        return {}

def req_shenzhenpe():
    req = ReqSZPE()
    return req.req_shenzhenpe(symbol)



# -*- coding=utf-8 -*-
import json
import os
import requests
import util

'''
cmd:(url, default_param, need_token)
'''
cmd_map = {
# finance, 参数type可以不填
'finance_cash_flow':('https://stock.xueqiu.com/v5/stock/finance/cn/cash_flow.json?symbol=',  {'type':'Q4', 'count':10}, True),
'finance_indicator':('https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json?symbol=',  {'type':'Q4', 'count':10}, True),
'finance_balance':('https://stock.xueqiu.com/v5/stock/finance/cn/balance.json?symbol=',      {'type':'Q4', 'count':10}, True),
'finance_income':('https://stock.xueqiu.com/v5/stock/finance/cn/income.json?symbol=',        {'type':'Q4', 'count':10}, True),
'finance_business':('https://stock.xueqiu.com/v5/stock/finance/cn/business.json?symbol=',    {'type':'Q4', 'count':10}, True),

# report
'report_latest':('https://stock.xueqiu.com/stock/report/latest.json?symbol=',                   {}, True),
'report_earningforecast':('https://stock.xueqiu.com/stock/report/earningforecast.json?symbol=', {}, True),

# capital
'capital_margin':('https://stock.xueqiu.com/v5/stock/capital/margin.json?symbol=',         {'page':1, 'size':180}, True),
'capital_blocktrans':('https://stock.xueqiu.com/v5/stock/capital/blocktrans.json?symbol=', {'page':1, 'size':30}, True),
'capital_assort':('https://stock.xueqiu.com/v5/stock/capital/assort.json?symbol=',         {}, True),
'capital_history':('https://stock.xueqiu.com/v5/stock/capital/history.json?symbol=',       {'count':20}, True),
'capital_flow':('https://stock.xueqiu.com/v5/stock/capital/flow.json?symbol=',             {}, True),

# f10
'f10_skholderchg':('https://stock.xueqiu.com/v5/stock/f10/cn/skholderchg.json?symbol=',{}, True),
'f10_skholder':('https://stock.xueqiu.com/v5/stock/f10/cn/skholder.json?symbol=',      {}, True),
'f10_industry':('https://stock.xueqiu.com/v5/stock/f10/cn/industry.json?symbol=',      {}, True),
'f10_holders':('https://stock.xueqiu.com/v5/stock/f10/cn/holders.json?&symbol=',       {}, True),
'f10_bonus':('https://stock.xueqiu.com/v5/stock/f10/cn/bonus.json?&symbol=',           {'page':1,'size':10}, True),
'f10_org_holding_change':('https://stock.xueqiu.com/v5/stock/f10/cn/org_holding/change.json?symbol=', {}, True),
'f10_industry_compare':('https://stock.xueqiu.com/v5/stock/f10/cn/industry/compare.json?type=single&symbol=', {}, True),
'f10_business_analysis':('https://stock.xueqiu.com/v5/stock/f10/cn/business_analysis.json?symbol=',  {}, True),
'f10_shareschg':('https://stock.xueqiu.com/v5/stock/f10/cn/business_analysis.json?symbol=',    {'count':5}, True),
'f10_top_holders':('https://stock.xueqiu.com/v5/stock/f10/cn/top_holders.json?&symbol=',       {'circula':1}, True),
'f10_indicator':('https://stock.xueqiu.com/v5/stock/f10/cn/indicator.json?symbol=',            {}, True),

# real time
'realtime_quote':('https://stock.xueqiu.com/v5/stock/realtime/quotec.json?symbol=',  {}, False),
'realtime_pankou':('https://stock.xueqiu.com/stock/pankou.json?symbol=',             {}, True),

#detail info
'quote_detail':('https://stock.xueqiu.com/v5/stock/quote.json?extend=detail&symbol=',     {}, True),
}

def get_token():
    if os.environ.get('XUEQIUTOKEN') is None:
        raise Exception(NOTOKEN_ERROR_MSG)
    else:
        return os.environ['XUEQIUTOKEN']

def set_token(token):
    os.environ['XUEQIUTOKEN'] = token
    return os.environ['XUEQIUTOKEN']

NOT200_ERROR_MSG = u"请求错误"
NOTOKEN_ERROR_MSG = u"未设置TOKEN"

### 试试数据
def fetch(symbols, cmd, params={}):
    if cmd not in cmd_map.keys():
        raise Exception(u'请求命令错误, 请参考cmd_map')
    info = cmd_map[cmd]
    url = info[0]
    new_param = info[1].copy()
    need_token = info[2]
    new_param.update(params)
    url += symbols
    for (k,v) in new_param.items():
        url += '&' + k + '=' + str(v)
    headers = {'Host': 'stock.xueqiu.com',
        'Accept': 'application/json',
        'User-Agent': 'Xueqiu iPhone 11.8',
        'Accept-Language': 'zh-Hans-CN;q=1, ja-JP;q=0.9',
        'Accept-Encoding': 'br, gzip, deflate',
        'Connection': 'keep-alive'}
    if need_token is True:
        headers['Cookie'] = get_token()
    print(url)
    return util.http_get(url, headers)

def test(symbol):
    set_token('xq_a_token=636e3a77b735ce64db9da253b75cbf49b2518316')
    #print fetch(symbol, 'realtime_quote')
    #return
    for cmd in sorted(cmd_map.keys()):
        js = fetch(symbol, cmd)
        print('==>' + cmd + ':' + len(js))

#test('SZ000001')
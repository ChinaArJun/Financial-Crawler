# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os
import codecs
import util
import sina

'''
loft基金所有数据(手动下载数据):
http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple?page=1&num=300&sort=symbol&asc=1&node=lof_hq_fund&_s_r_a=page
'''
def fox_etf_history_all(all_filename, outpath, fox_type='new'):
    jsdata = util.load_json(all_filename)
    cnt = 0
    timestamp = str(int(time.time()*1000))
    for item in jsdata:
        fn = outpath+item['symbol']+'_sina.json'
        if fox_type == 'new' and os.path.exists(fn) is True:
            continue
        dlist = sina.history_etf(item['code'], timestamp=timestamp)
        util.dump_json(fn, dlist)
        cnt += 1
        if cnt % 5 == 0:
            time.sleep(10)

#fox_etf_history_all('./fund/etf-all.json', 'fund/etf/')
fox_etf_history_all('./fund/etf-lof-all.json', 'fund/etf-lof/')

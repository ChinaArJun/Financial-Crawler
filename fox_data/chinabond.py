#-*-coding:utf-8-*

import codecs
import json
import time
import requests
import datetime

def fetch(url, headers={}):
    print('url:'+url)
    resp=requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(resp.status_code)
        raise Exception(resp.content)
    return json.loads(resp.content)

'''
中国10年期国债收益率：http://yield.chinabond.com.cn/cbweb-czb-web/czb/moreInfo?locale=cn_ZH
return float
#req: http://yield.chinabond.com.cn/cbweb-czb-web/czb/czbQueryXy?zblx=xy&workTime=2020-09-27
#resp:[{"ycDefId":"2c9081e50a2f9606010a3068cae70001","ycDefName":"中债国债收益率曲线","worktime":"2020-09-27",
"seriesData":[[[1.0,2.6005],[1.2,2.6273],[5.0,3.0086],[10.0,3.1324],[15.0,3.5823],[20.0,3.7038],[30.0,3.8305]],
"isPoint":false,"dcq":0.0}]
'''
def chinabond(year=10, workTime=None):
    def parse_chinabond_data(jsdata, year):
        if (len(jsdata) > 0) and ('seriesData' in jsdata[0]):
            #print jsdata
            for item in jsdata[0]['seriesData']:
                if len(item) == 2:
                    if item[0] == year:
                        return item[1]
                else:
                    raise Exception(u'国债数据格式异常1')
        else:
            #print jsdata
            raise Exception(u'国债数据格式异常2')
    url = "http://yield.chinabond.com.cn/cbweb-czb-web/czb/czbQueryXy?zblx=xy&workTime="
    now = now = datetime.datetime.now()
    if workTime is None or len(workTime) < 10:
        now = datetime.datetime.now()
        for i in range(0, 10):
            dt = now - datetime.timedelta(days=i)
            workTime = dt.strftime('%Y-%m-%d')
            jsdata = fetch(url+workTime)
            try:
                return parse_chinabond_data(jsdata, year)
            except Exception as e:
                print('no data date:' + workTime)
                #print e
                continue
    else: 
        jsdata = fetch(url+workTime)
        return parse_chinabond_data(jsdata, year)
    raise Exception(u'请求国债数据异常！')

#print chinabond()

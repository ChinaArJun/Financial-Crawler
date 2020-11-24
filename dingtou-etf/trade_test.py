# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os
import codecs
from datetime import date
from data_loader import DataLoader
from buy_policy import *
from sell_policy import *
from trade_center import TradeCenter

dt_str = datetime.datetime.now().strftime("%Y-%m-%d")
         
def trace_test(name, daily_etf_data, monthly_pe_data, buy_policy=[], sell_policy=[]):
    print "\n\n",name
    cnt = 0
    data = daily_etf_data
    tc = TradeCenter(data[-1][0])
    for idx in range(0, len(data)):
        dts = data[idx][0]
        price = data[idx][1]
        year = dts[:4]
        year_month = dts[:7]
        no_market_pe = True
        hspe = 0.0
        if year_month in monthly_pe_data:
            hspe = monthly_pe_data[year_month]
            no_market_pe = False
        #print '##current ',dts,hspe,data[idx],tc.income_total_str(price, True)
        #check buy
        buy_amount = 0
        tc.set_price(dts, price)
        hold_income_ratio = tc.hold_income()
        for bp in buy_policy:
            if len(bp.adjuest_buy_market_pe) > 0 and no_market_pe:
                raise Exception(u"根据市场市盈率购买策略：没有找到市盈率数据，date:%s len(pe_data):%d" % (dts, len(monthly_pe_data)))
            buy_amount += bp.check_buy(price, hold_income_ratio, hspe)
            #print '##',bp.name,buy_amount,price, hold_income_ratio, hspe
        if buy_amount > 0:
            #print '##curr ',dts,hspe,data[idx],tc.income_total_str(price, True)
            #print ' ++buy ',year, price, buy_amount,hold_income_ratio, hspe,'\t',tc.income_total.current_income_str(tc.hold_value(price))
            tc.buy(dts, buy_amount)
        #check sell
        sell_num = 0
        hold_income_ratio = tc.hold_income()
        for sp in sell_policy:
            if len(bp.adjuest_buy_market_pe) > 0 and no_market_pe:
                raise Exception(u"根据市场市盈率购买策略：没有找到市盈率数据，date:%s len(pe_data):%d" % (dts, len(monthly_pe_data)))
            sell_num += sp.check_sell(price, hold_income_ratio, tc.num, hspe)
        if sell_num > 0:
            #print '##curr ',dts,hspe,data[idx],tc.income_total_str(price, True)
            #print ' --sell ',year, price, sell_num, hold_income_ratio, hspe, tc.income_total.current_income_str(tc.hold_value(price))
            tc.sell(dts, sell_num)
        cnt += 1
        if cnt > 50000:
            break
        #print id,len(data),tc.income_total_str(data[0][1], True)
    print tc.income_total_str()
    return (name, tc.yearly_ratio())

def test(flag, code, daily_etf_data_path, monthly_pe_data_filename = '', dt_start='2000-01-01', dt_end=''):
    def trans_to_symbal(code):
        symbal = code
        if code[:1] == '0' or code[:1] == '1':
            symbal = 'sz' + code
        elif code[:1] == '3' or code[:1] == '5' or code[:1] == '6'or code[:1] == '7':
            symbal = 'sh' + code
        return symbal
    symbal = trans_to_symbal(code)
    daily_etf_data_filename = daily_etf_data_path + symbal + '_sina.json'
    db = DataLoader()
    daily_etf_data = db.daily_etf_data(daily_etf_data_filename, dt_start, dt_end)
    if len(daily_etf_data) == 0: 
        print u'没有读到历史数据:',daily_etf_data_filename
        return
    if len(monthly_pe_data_filename) > 0:
        monthly_pe_data = db.monthly_pe_data(monthly_pe_data_filename)
    income_data = []
    if flag & 0x01 == 0x01:
        yearly_income = trace_test(u'每周定额购买，盈利6%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[BuyPolicy('CicleBuy', cicle_buy=True, buy_amount=1000)]
                    #, sell_policy=[]
                    , sell_policy=[SellPolicy('6%Sell', sell_by_income=[(0.06, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周定额购买，盈利10%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[BuyPolicy('CicleBuy', cicle_buy=True, buy_amount=1000)]
                    , sell_policy=[SellPolicy('10%Sell', sell_by_income=[(0.10, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周定额购买，盈利20%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[BuyPolicy('CicleBuy', cicle_buy=True, buy_amount=1000)]
                    , sell_policy=[SellPolicy('20%Sell', sell_by_income=[(0.20, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
    if flag & 0x02 == 0x02:
        yearly_income = trace_test(u'每周根据当前盈亏比例购买，盈利6%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&adjust', cicle_buy=True, buy_amount=1000, adjuest_buy_income_ratio=[(-0.2, 4), (-0.1, 2), (-0.05, 1.5), (0.1, 1), (1, 0)]) 
                    ], 
                    sell_policy=[SellPolicy('6%Sell', sell_by_income=[(0.06, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周根据当前盈亏比例购买，盈利10%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&adjust', cicle_buy=True, buy_amount=1000, adjuest_buy_income_ratio=[(-0.2, 4), (-0.1, 2), (-0.05, 1.5), (0.1, 1), (1, 0)]) 
                    ], 
                    sell_policy=[SellPolicy('10%Sell', sell_by_income=[(0.10, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周根据当前盈亏比例购买，盈利20%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&adjust', cicle_buy=True, buy_amount=1000, adjuest_buy_income_ratio=[(-0.2, 4), (-0.1, 2), (-0.05, 1.5), (0.1, 1), (1, 0)]) 
                    ], 
                    sell_policy=[SellPolicy('20%Sell', sell_by_income=[(0.20, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
    if flag & 0x04 == 0x04:
        #策略买卖: 根据恒生指数市盈率购买1:根据pe=20作为分界点
        yearly_income = trace_test(u'每周根据恒生指数市盈率倍数购买，超过20倍卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(8,4),(10,3),(12,2),(15,1),(20,0),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('10%Sell', sell_by_market_pe=[(20,0.0),(100,1.0)])]
                    )
        income_data.append(yearly_income)
        #策略买卖: 根据恒生指数市盈率购买1:根据pe=15作为分界点
        yearly_income = trace_test(u'每周根据恒生指数市盈率倍数购买，超过15倍卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(20,4),(25,3),(30,2),(35,1),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('10%Sell', sell_by_market_pe=[(60,0.0),(100,1.0)])]
                    )
        income_data.append(yearly_income)
    if flag & 0x08 == 0x08:
        yearly_income = trace_test(u'每周根据深圳指数市盈率倍数购买，超过60倍卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(20,4),(25,3),(30,2),(35,1),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('10%Sell', sell_by_market_pe=[(60,0.0),(100,1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周根据深圳指数市盈率4分位购买，超过50倍卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(20,4),(25,3),(35,2),(41,1),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('10%Sell', sell_by_market_pe=[(50,0.0),(100,1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周根据深圳指数市盈率4分位购买，赚6%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(20,4),(25,3),(35,2),(41,1),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('6%Sell', sell_by_income=[(0.06, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周根据深圳指数市盈率4分位购买，赚10%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(20,4),(25,3),(35,2),(41,1),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('6%Sell', sell_by_income=[(0.10, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
        yearly_income = trace_test(u'每周根据深圳指数市盈率4分位购买，赚20%卖出',daily_etf_data,monthly_pe_data,
                    buy_policy=[
                        BuyPolicy('Cicle&HSPE', cicle_buy=True, buy_amount=1000, adjuest_buy_market_pe = [(20,4),(25,3),(35,2),(41,1),(100,0)]) 
                    ], 
                    sell_policy=[SellPolicy('6%Sell', sell_by_income=[(0.20, 0), (1, 1.0)])]
                    )
        income_data.append(yearly_income)
    etf_info = db.etf_info('../fox_data/fund/etf-all.json')
    etf_name = 'Unkown'
    if symbal in etf_info:
        etf_name = etf_info[symbal]['name']
    year_set = []
    for (name, yearly) in income_data:
        year_set += yearly.keys()
    year_set = list(set(year_set))
    year_set.sort()
    with codecs.open('output/'+symbal+'_'+etf_name+'_'+dt_str+'.csv', 'w', encoding='gbk') as f:
        f.write(symbal+','+etf_name + '\n')
        f.write('year,'+','.join([n for (n,r) in income_data]))
        f.write('\n')
        for year in year_set:
            f.write(year+',')
            for (name, yearly) in income_data:
                if year in yearly.keys():
                    f.write("%.1f%%," % (yearly[year]*100))
                else:
                    f.write(',')
            f.write('\n')
        f.close()


'''
ETF代码：SH51XXXX   SZ159XXX
ETF-LOF代码:SH501XXX SZ16XXXX
'''

'''
常用指数etf:
沪深 300（510300）、上证红利（510880）、深红利（159905）、标普红利（501029）
'''
def test_etf_hk():
    test(0x07, '159920', '../fox_data/fund/etf/',  monthly_pe_data_filename='../fox_data/index/hk_hangseng_pe_his.json')

'''
常用指数etf:
沪深 300（510300）、上证红利（510880）、深红利（159905）、标普红利（501029）
'''
def test_etf1():
    for code in ['159905']:
    #for code in ['510300', '510880', '159905', '501029']:
        test(0x0B, code, '../fox_data/fund/etf/',  monthly_pe_data_filename='../fox_data/index/sz_pe_his.json', dt_start='2000-01-01')
test_etf1()

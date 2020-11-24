# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os

class SellPolicy:
    def __init__(self, name
            , sell_by_income = [] #收益小于10%的时候不卖，大于10%的时候全部卖
            , sell_by_market_pe = [] #根据市场市盈利倍数来卖
             ):
        self.name = name
        self.sell_by_income = sell_by_income
        self.sell_by_market_pe = sell_by_market_pe

    #返回卖的数量
    def check_sell(self, current_price, hold_income_ratio, hold_num, market_pe):
        #print "check_sell:",self.name,current_price, hold_income_ratio, hold_num, market_pe
        sell_num = 0
        if len(self.sell_by_market_pe) > 0:
            for (pe, sell_ratio) in self.sell_by_market_pe:
                if market_pe <= pe:
                    sell_num = int(hold_num * sell_ratio)
                    break
        elif len(self.sell_by_income) > 0:
            for (inc_ratio, sell_ratio) in self.sell_by_income:
                if hold_income_ratio <= inc_ratio:
                    sell_num = int(hold_num * sell_ratio)
                    break
        return sell_num
 
        #break
#1赚10%全部卖
Income10Sell = SellPolicy('10%Sell', sell_by_income=[(0.1, 0), (1, 1.0)])
#1赚10%全部卖一半，赚20%全卖
Income10Sell = SellPolicy('10%Sell', sell_by_income=[(0.1, 0), (0.2, 0.5), (1.0, 1.0)])
#HK HS ETF卖法：大于15全买
Income10Sell = SellPolicy('HSPE-Sell', sell_by_income=[(15, 0), (20, 1.0)])
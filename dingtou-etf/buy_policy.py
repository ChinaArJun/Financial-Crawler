# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os

#盈利情况下调整周期购买额度
class BuyPolicy:
    '''
        name:策略名字
        cicle_buy:是否定期购买
        buy_amount:每次购买的总额基数 
        #根据持有盈亏比例购买倍数，
        adjuest_buy_income_ratio=[(-0.2, 4), (-0.1, 0.5), (0, 1), (0.1, 0.5), (0.2, 0.2)]    adjuest_buy_amount_type==2时，如果这个值为负，表示亏损比例，为正时表示盈利比例
        #按照市盈率倍数定投
        adjuest_buy_market_pe = [(10,4),(11,2),(12,1),(20,0)]   
    '''
    def __init__(self, name, cicle_buy=True, buy_amount = 1000
            , adjuest_buy_income_ratio = []
            , adjuest_buy_market_pe = []
            ):
        self.name = name
        self.cicle_buy = cicle_buy
        self.buy_amount = buy_amount
        self.adjuest_buy_income_ratio = adjuest_buy_income_ratio
        self.adjuest_buy_market_pe   = adjuest_buy_market_pe
        self.check_idx = 0
    
    def check_buy(self, current_price, hold_income_ratio, market_pe):
        buy_amount = 0
        #周期购买
        if self.cicle_buy:
            #print self.check_idx,self.check_idx%5
            if self.check_idx % 5 != 0:
                self.check_idx += 1
                return 0
            self.check_idx += 1
            #根据市场市盈率倍数购买
            if len(self.adjuest_buy_market_pe) > 0:
                for (pe, times) in self.adjuest_buy_market_pe:
                    if market_pe <= pe:
                        buy_amount = self.buy_amount * times
                        break
            #根据盈亏调整购买比例
            elif len(self.adjuest_buy_income_ratio) > 0:
                #2亏损到一定比例时按照比例调整
                for (ratio, times) in self.adjuest_buy_income_ratio:
                    if hold_income_ratio <= ratio:
                        buy_amount = self.buy_amount * times
                        break
            #0不调整购买金额
            else:
                buy_amount = self.buy_amount
            return int(buy_amount)
        #非周期购买
        elif not self.cicle_buy:
            #2 根据盈亏调整购买比例
            if len(self.adjuest_buy_market_pe) > 0:
                for (pe, times) in self.adjuest_buy_market_pe:
                    if market_pe <= pe:
                        buy_amount = self.buy_amount * times
                        break
            #根据盈亏调整购买比例
            elif len(self.adjuest_buy_income_ratio) > 0:
                #2亏损到一定比例时按照比例调整
                for (ratio, times) in self.adjuest_buy_income_ratio:
                    if hold_income_ratio <= ratio:
                        buy_amount = self.buy_amount * times
            return int(buy_amount)
        return int(buy_amount)
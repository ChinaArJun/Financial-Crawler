# -*- coding=utf-8 -*-
import datetime
import time
import string
import json
import os
from datetime import date

class IncomeStat:
    def __init__(self, year=''):
        self.year = year
        self.buy_amount = 0.0
        self.sell_amount = 0.0
        self.hold_value = 0.0
        self.last_ratio = 0.0
        self.sub_value = None

    def add(self, hold, buy=0.0, sell = 0.0):
        if self.sub_value is None:
            self.sub_value = hold
        self.hold_value = hold
        self.sell_amount += sell
        if buy > 0.1 and self.sell_amount > buy:
            self.sell_amount -= buy
        else:
            self.buy_amount += buy
        self.last_ratio = 0.0
        income_amount = 0.0
        if self.buy_amount > 0.01:
            income_amount = self.sell_amount + hold - self.buy_amount - self.sub_value
            self.last_ratio =  income_amount / self.buy_amount

    def details(self):
        ss = '''%s   %.2f%%(buy:%.2f, total:%.2f, sell:%.2f, sub:%.2f)\n''' % (self.year, self.last_ratio*100, self.buy_amount, self.sell_amount + self.hold_value, self.sell_amount, self.sub_value)
        return ss

class IncomeTotal:
    def __init__(self, end_dt):
        self.current = IncomeStat()
        self.last_1year = IncomeStat(u'最近1年')
        self.last_2year = IncomeStat(u'最近2年')
        self.last_3year = IncomeStat(u'最近3年')
        self.last_5year = IncomeStat(u'最近5年')
        self.income_yearly_ratio = {}
        self.income_yearly_ratio[self.last_1year.year] = self.last_1year
        self.income_yearly_ratio[self.last_2year.year] = self.last_2year
        self.income_yearly_ratio[self.last_3year.year] = self.last_3year
        self.income_yearly_ratio[self.last_5year.year] = self.last_5year
        dt = datetime.datetime.strptime(end_dt, '%Y-%m-%d')
        dt_1year = dt - datetime.timedelta(days=365)
        dt_2year = dt - datetime.timedelta(days=365*2)
        dt_3year = dt - datetime.timedelta(days=365*3)
        dt_5year = dt - datetime.timedelta(days=365*5)
        self.last_1year_begin_dt = dt_1year.strftime('%Y-%m-%d')
        self.last_2year_begin_dt = dt_2year.strftime('%Y-%m-%d')
        self.last_3year_begin_dt = dt_3year.strftime('%Y-%m-%d')
        self.last_5year_begin_dt = dt_5year.strftime('%Y-%m-%d')
        print self.last_1year_begin_dt,self.last_2year_begin_dt,self.last_3year_begin_dt,self.last_5year_begin_dt

    def add(self, dt, hold, buy=0.0, sell = 0.0):
        year = dt[:4]
        #current year
        if self.current.year != year:
            self.current = IncomeStat(year)
            self.income_yearly_ratio[year] = self.current
        self.current.add(hold, buy, sell)
        if dt >= self.last_1year_begin_dt:
            self.last_1year.add(hold, buy, sell)
        if dt >= self.last_2year_begin_dt:
            self.last_2year.add(hold, buy, sell)
        if dt >= self.last_3year_begin_dt:
            self.last_3year.add(hold, buy, sell)
        if dt >= self.last_5year_begin_dt:
            self.last_5year.add(hold, buy, sell)

    def current_income_str(self, hold):
        return self.current.details()

    def yearly_ratio(self, hold = 0):
        ratios = {}
        for (year,item) in self.income_yearly_ratio.items() :
            ratios[year] = item.last_ratio
        return ratios

    def income_str(self, hold):
        ss = ''
        for (year,item) in sorted(self.income_yearly_ratio.items(), key=lambda s: s[0]):
            ss += item.details()
        return ss

class TradeCenter:
    def __init__(self, end_year):
        self.num = 0.0              #持有数量
        self.curr_price = 0.0
        self.base_value = 0.0       #成本市值=成本价*持有数量
        self.sell_count = 0
        self.sell_income = 0.0    #卖出后的收入
        #只是用于统计
        self.buy_money_total = 0.0  #总买入
        self.buy_count = 0
        self.income_total = IncomeTotal(end_year)

    def set_price(self, dt, price):
        self.curr_price = price
        self.income_total.add(dt, self.hold_value(), buy=0, sell=0)
        
    def buy(self, dt, money):
        self.income_total.add(dt, self.hold_value(), buy=money)
        self.buy_count += 1
        if self.sell_income >= money:
            self.sell_income -= money
        else:
            self.buy_money_total += money
        self.num += money / self.curr_price
        self.base_value += money

    #卖: 价格，卖的数量，或是持仓比例
    def sell(self, dt, num=0):
        sell_num = num
        if sell_num > 0 and sell_num <= self.num:
            sell_money = sell_num * self.curr_price
            self.income_total.add(dt, self.hold_value(), sell=sell_money)
            self.sell_count += 1
            self.num -= sell_num
            self.sell_income += sell_money
            self.base_value = self.num * self.curr_price
            
    #持仓总额
    def hold_value(self):
        return self.num * self.curr_price

    #持仓盈亏比例
    def hold_income(self):
        try:
            return (self.hold_value() - self.base_value) / self.base_value
        except:
            return 0.0

    def yearly_ratio(self):
        value,ratio = 0.0,0.0
        if self.buy_money_total > 0.1:
            value = self.hold_value() + self.sell_income
            ratio = (value-self.buy_money_total) / self.buy_money_total
        income_yearly = self.income_total.yearly_ratio(self.hold_value())
        income_yearly[u'累计收益'] = ratio
        return income_yearly

    def income_total_str(self, debug=False):
        value,ratio = 0.0,0.0
        if self.buy_money_total > 0.1:
            value = self.hold_value() + self.sell_income
            ratio = (value-self.buy_money_total) / self.buy_money_total
        s = ''
        s += 'buy:%d-%.2f' % (self.buy_count, self.buy_money_total)
        s += '  sell:%d-%.2f' % (self.sell_count, self.sell_income)
        s += '  hold:%d-%.2f-%.2f-%.2f%%' % (self.num, self.base_value, self.hold_value(), self.hold_income() * 100)
        s += '  total_value:%.2f' % value
        s += '  income:%.2f' % (value - self.buy_money_total)
        s += '  ratio:%.2f%%' % (ratio * 100)
        if debug is False:
            s += '\n' + self.income_total.income_str(self.hold_value())
        return s
         

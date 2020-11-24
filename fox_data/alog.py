# -*- coding:utf-8 -*-
import time
import datetime

def error(msg, prefix1='', prefix2=''):
    s = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S[') + prefix1 + '][' + prefix2 + ']' + str(msg)
    print(s)

def warring(msg, prefix1='', prefix2=''):
    error(msg, prefix1, prefix2)

def info(msg, prefix1='', prefix2=''):
    error(msg, prefix1, prefix2)

def debug(msg, prefix1='', prefix2=''):
    error(msg, prefix1, prefix2)
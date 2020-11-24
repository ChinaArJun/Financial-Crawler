# -*- coding:utf-8 -*-
import os
import datetime
from bottle import (run, route, get, post, put, delete, request, static_file, debug, touni)
import sys
import time
import getopt
import xml.etree.ElementTree as ET
import hashlib
import wx_receive
import wx_reply


sys.path.append("../good_price")
from good_price_cn import GoodPriceCalc
import alog

debug_mode = False

@get('/wx')
def check_token():
    alog.info('check_token', 'check_token')
    param_raw = {}
    def getValue(key, default=''):
        value = request.GET.get(key,default).strip()
        if value is None or len(value) == 0:
            value = default
            param_raw.setdefault(key, value)
        else:
            param_raw.setdefault(key,value)
        return value
    try:
        signature  = getValue('signature')
        timestamp  = getValue('timestamp')
        nonce  = getValue('nonce')
        echostr  = getValue('echostr')
        token = "xxxx" #请按照公众平台官网\基本配置中信息填写

        list = [token, timestamp, nonce]
        list.sort()
        sha1 = hashlib.sha1()
        map(sha1.update, list)
        hashcode = sha1.hexdigest()
        print("handle/GET func: hashcode:" + hashcode + " signature:" + signature)
        if hashcode == signature:
            alog.info('check ok:'+echostr, 'check_token')
            return echostr
        else:
            return ""
    except Exception as Argument:
        return Argument

@post('/wx')
def goodprice_wx():
    xmldata = touni(request._get_body_string())
    alog.info(xmldata, 'goodprice_wx')
    #try:
    if True:
        recMsg = wx_receive.parse_xml(xmldata)
        if isinstance(recMsg, wx_receive.Msg) and recMsg.MsgType == 'text':
            toUser = recMsg.FromUserName
            fromUser = recMsg.ToUserName
            recv_content = touni(recMsg.Content)
            calc = GoodPriceCalc()
            alog.debug(recv_content, 'goodprice_wx', 'recMsg.Content')
            price_data = calc.calc(recv_content)
            content = calc.to_text(price_data, '<br>')
            replyMsg = wx_reply.TextMsg(toUser, fromUser, recv_content)
            s = replyMsg.send()
            alog.debug(s, 'goodprice_wx', 'reply')
            return s
        else:
            alog.info(s, 'goodprice_wx', u'不支持')
            return "success"
    """ except Exception as Argment:
        alog.info(Argment, 'goodprice_wx', u'error')
        return Argment
    except:
        alog.info(u'error', 'goodprice_wx', u'error')
        return 'error' """

@route('/goodprice/<codes>')
def goodprice(codes):
    alog.info(codes, 'goodprice')
    if len(codes) == 0:
        return u'goodprice/<codes>'
    calc = GoodPriceCalc()
    price_data = calc.calc(codes)
    return calc.to_json(price_data, indent=4)

 #------- static -------
@route('/cache/<filename>')
def cache_file(filename):
    alog.info(filename, 'cache_file')
    return static_file(filename, root='.cache/')

if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], 'dp:', ['debug', 'port='])
    port = 8888
    opts = []
    for opt, arg in opts:
        if opt in ("-p", "--port"):
            port = int(arg)
        elif opt in ("-d", "--debug"):
            debug(True)
            debug_mode = True
        else:
            print("web.py -p[--port] <port>  -d[--debug]")
            exit(2)
    alog.info(port, '__main__', 'port')
    alog.info(debug_mode, '__main__', 'debug')
    run(host='0.0.0.0', port=port, reloader=True)

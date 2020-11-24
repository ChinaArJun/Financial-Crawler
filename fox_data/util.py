#-*-coding:utf-8-*

import codecs
import json
import time
import requests
import datetime

def http_get(url, headers={}, json_rs = True):
    print('url:' + url)
    resp=requests.get(url, headers=headers)
    if resp.status_code != 200:
        print ( url + resp.status_code )
        raise Exception(resp.content)
    if json_rs is True:
        return json.loads(resp.content)
    return resp.content

def http_post(url, headers={}, params={}, json_rs = True):
    resp = requests.post(url, headers=headers, params=params)
    #print r.status_code
    #print r.content
    if resp.status_code != 200:
        print ( url + resp.status_code )
        raise Exception(resp.content)
    if json_rs is True:
        return json.loads(resp.content)
    return resp.content


def dump_str(filename, s):
    with codecs.open(filename, 'w', encoding='utf-8') as f:
        f.write(s)
        f.close()

def dump_json(filename, js):
    with codecs.open(filename, 'w', encoding='utf-8') as f:
        json.dump(js, f, indent=4, ensure_ascii=False)
        f.close()

def load_json(filename):
    with codecs.open(filename, 'r', encoding='utf-8') as f:
        js = json.load(f)
        f.close()
        return js
    raise Exception('[load_json] open failed:' + filename)

def code2symbal(code, market=''):
    symbal = code
    if code.isdecimal():
        #sh sz
        if len(code) == 6: 
            if code[:1] == '0' or code[:1] == '1':
                symbal = 'sz' + code
            elif code[:1] == '3' or code[:1] == '5' or code[:1] == '6'or code[:1] == '7':
                symbal = 'sh' + code
        #hk
        elif len(code) == 5:pass
    return symbal

def print_json(js, info ='', support_cn = True, indent=4):
    if len(info) > 0:
        info += ': '
    if js is None:
        print(info + "is None")
    else:
        if support_cn:
            print(info + json.dumps(js, ensure_ascii=False, sort_keys=True, indent=indent))
        else:
            print(info + js)
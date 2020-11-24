#-*-coding:utf-8-*

import codecs
import json
import time
import sys
import getopt

if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], 'p:', ['port='])

    print(opts)
    print(args)


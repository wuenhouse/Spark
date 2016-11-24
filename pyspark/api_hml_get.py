#!/usr/bin/env python
#coding=UTF-8
#Objective: get HML data to api
#Author: Wendell
#Created Date: 2016.03.01

import os, sys, getopt
import subprocess
import shutil
import math
sys.path.append("/data/migo/pandora/lib")
sys.path.append("/data/migo/athena/lib")
from athena_variable import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

import datetime, time
import util_hadoop
import numpy as np
from scipy import stats
import json

if __name__ == "__main__":

    '''
    schema
    spark
    storeid, memberid, nes, hml, npt, pr, order, nptD
    zhoushanxm^0009	2216000010415	E0	H	0.199862592053	1	1	20160311

    output schema
    StoreID, MemberID, NES, HML

         JobTask: get hml data
       Objective: get hml by storeid, caldate to get json format return
          Author: Wendell
    CreationDate: 2016/03/01
          Source: Schema
                  storeid, memberid, nes, hml, npt, pr, order, nptD
     Destination: None
           Usage: python /data/migo/athena/lib/util/api_hml_get.py shopid caldate
    '''
    JSON_RES = {"code":0, "msg":"", "category":"", "idx":-1, "name":"", "data":{}}

    shopid, storeid, caldate, hml, nes, page, count  = sys.argv[1:]
    storeid = revert_storeid(storeid)

    nes_l = nes.split(',')
    hml_l = hml.split(',')

    start = int(count) * (int(page))

    path = "/user/athena/{shopid}/hazard_hml/{caldate}".format(shopid=shopid, caldate=caldate)
    sc = SparkContext(appName="hml_list_get_{shopid}".format(shopid=shopid))

    hml_src = sc.textFile(path)
    if nes == '-1' and hml == '-1':
        R = hml_src.filter(lambda l: l.split('\t')[0].encode('utf-8') == storeid ).map(lambda l: (l.split('\t')[1:])).collect()
    elif nes == '-1':
        R = hml_src.filter(lambda l: l.split('\t')[0].encode('utf-8') == storeid and l.split('\t')[3] in hml_l ).map(lambda l: (l.split('\t')[1:])).collect()
    elif hml == '-1':
        R = hml_src.filter(lambda l: l.split('\t')[0].encode('utf-8') == storeid and l.split('\t')[2] in nes_l ).map(lambda l: (l.split('\t')[1:])).collect()
    else:
        R = hml_src.filter(lambda l: l.split('\t')[0].encode('utf-8') == storeid and l.split('\t')[2] in nes_l and l.split('\t')[3] in hml_l ).map(lambda l: (l.split('\t')[1:])).collect()

    sc.stop()
    total = 0
    ls = []
    if page == '-1':
        ls = []
        for obj in sorted(R, key=lambda m: m[4]):
            memberid, nes, hml, npt, pr, order, npd, ran = obj
            member = {'MemberID':memberid.encode('utf-8'), 'NES':nes.encode('utf-8'), 'NPT7_HML':hml.encode('utf-8'), 'NPT7':npt.encode('utf-8'), 'NPT_PR':pr.encode('utf-8'), 'NPD':npd.encode('utf-8') }
            ls.append(member)
    else:
        D = {}
        total = len(R)
        g = total % int(count)
        pages = total/ int(count)
        if g != 0:
            pages = pages + 1

        if page >= pages:
            page = pages
        n = 0
        for obj in sorted(R, key=lambda m: m[4]):
            if n < start:
                n = n + 1
                continue
            if len(ls) >= int(count):
                break
            memberid, nes, hml, npt, pr, order, npd, ran = obj
            member = {'MemberID':memberid.encode('utf-8'), 'NES':nes.encode('utf-8'), 'NPT7_HML':hml.encode('utf-8'), 'NPT7':npt.encode('utf-8'), 'NPT_PR':pr.encode('utf-8'), 'NPD':npd.encode('utf-8') }
            ls.append(member)
    RR = []
    RR.append(total)
    RR.append(ls)
    print RR

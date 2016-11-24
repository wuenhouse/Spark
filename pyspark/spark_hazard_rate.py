#!/usr/bin/env python
#coding=UTF-8
#Objective: Segement HML by hazard rate filter S3
#Author: Wendell
#Created Date: 2016.02.18

import os, sys, getopt
import subprocess
import shutil
import itertools
import math
sys.path.append("/data/migo/pandora/lib")
sys.path.append("/data/migo/athena/lib")
from athena_variable import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

import pymongo
import datetime, time
import copy
import util_hadoop
import numpy as np
from scipy import stats
import operator
import random

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"hs:d:",["shopid=","caldate="])
    except getopt.GetoptError:
        print 'test.py -s <shopid> -d <caldate>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'test.py -s <shopid> -d <caldate>'
            sys.exit()
        elif opt in ("-s", "--shopid"):
            shopid = arg
        elif opt in ("-d", "--caldate"):
            caldate = arg

    return shopid, caldate

def p_score(ls):

    ls2 = sorted(ls, reverse=False)
    total = len(ls2)
    n = 1
    i = 1
    R = {}
    perious = ""
    for x in ls2:
        if x == perious:
            i = i + 1
            R[x] = int((float((total - n))/total) * 100) + 1
        else:
            n = i
            i = i + 1
            R[x] = int((float((total - n))/total) * 100) + 1
            perious = x
    return R

if __name__ == "__main__":

    '''
    spark
    circle	1981	68.1482615346	2015-05-10T00:00:00	2015-05-10T00:00:00	0.981286768494	0.00198356182268
    circle	1999tinaeTl02yahoo.com.tw	68.1482615346	2015-12-02T00:00:00	2015-12-02T00:00:00	0.481559178935	0.0118748456603
    circle	Y4146508420	50.1548002706	2014-02-25T00:00:00	2014-04-08T00:00:00	1.0	2.24871030702e-14

    nes_tag_week
    circle^EC	Y2803369762	S3
    circle^EC	Y5308300388	S3
    circle^EC	Y6604501877	S3

    output schema
    StoreID, MemberID, NES, HML

         JobTask: Hazard rate HML and order
       Objective: Segement Hazard rate HML
          Author: Wendell
    CreationDate: 2016/02/18
          Source: Schema
                  hazard rate, nes
     Destination: Adjusted Purchasing Interval (/user/athena/hazard_rate_hml/date/)
           Usage: python spark_hazard_rate.py -s shopid -d caldate
    '''
    shopid, caldate = main(sys.argv[1:])
    hazard_path = "/user/athena/{shopid}/hazard_rate/{caldate}/*".format(shopid=shopid, caldate=caldate)
    nes_path = "/user/athena/{shopid}/nes_tag_week/{caldate}/*".format(shopid=shopid, caldate=caldate)
    outpath = "/user/athena/{shopid}/hazard_hml/{caldate}".format(shopid=shopid, caldate=caldate)
    sc = SparkContext(appName="hazard_calculation__{shopid}".format(shopid=shopid))

    hazard_src = sc.textFile(hazard_path, 40)
    nes_src = sc.textFile(nes_path)

    #filter S3
    r_nes = nes_src.filter(lambda l: l.split('\t')[2] != 'S3').map(lambda l: ("{}_{}".format(l.split('\t')[0].encode('utf-8'),l.split('\t')[1].encode('utf-8')),l.split('\t')[2]))
    #loading hazard and format it to prepare to join
    r_hazard = hazard_src.map(lambda l: ("{}_{}".format( \
        l.split('\t')[0].encode('utf-8'), \
        l.split('\t')[1].encode('utf-8'), \
        ),(l.split('\t')[5],(datetime.datetime.strptime(l.split('\t')[4][0:10].replace('-',''),'%Y%m%d').date() + datetime.timedelta(days = math.floor(float(l.split('\t')[2])))).strftime('%Y%m%d'))))
    #join src
    R1 = r_nes.join(r_hazard).cache() 
    R = R1.map(lambda l: (l[0].split('_')[0],float(l[1][1][0].encode('utf-8'))) ).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
    
    D = {}
    for obj in R:
        storeid = obj[0]
        ls = obj[1]
        if ls:
            p50 = reduce(lambda x, y: x + y, ls) / len(ls)
            ls2 = [i for i in ls if i > p50]
            if ls2:
                p75 = reduce(lambda x, y: x + y, ls2) / len(ls2)
            # asc so get p75 be high and middle seperate
            D[storeid] = (p50, p75)

    def hml(storeid, values, D):
        vs = []
        for v in values:
            vs.append(v[2])
        PRD = p_score(vs)
        ls = sorted(values, key=lambda l: l[2], reverse=True)
        R = []
        n = 0
        for h in ls:
            n = n + 1
            ran = random.random() 
            memberid, nes, npt, npd = h
            PR = PRD[npt]
            if npt <= D[storeid][0]:
                hml = "L"
            elif npt > D[storeid][1]:
                hml = "H"
            else:
                hml = "M"
            R.append("{storeid}\t{memberid}\t{nes}\t{hml}\t{npt}\t{PR}\t{order}\t{npd}\t{ran}".format(storeid=storeid, memberid=memberid, nes=nes, hml=hml, npt=npt, PR=PR, order=n, npd=npd, ran=ran))
        return R

    Result = R1.map(lambda l: (l[0].split('_')[0], \
    (l[0].split('_')[1], l[1][0].encode('utf-8') ,float(l[1][1][0].encode('utf-8')), l[1][1][1].encode('utf-8')) \
    )).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : hml(x[0], x[1], D)).flatMap(lambda x : x)

    serial = Result.count()
    if serial > 0:
        if util_hadoop.PathIsExit(outpath):
            cmd = "hadoop fs -rm -r {}".format(outpath)
            subprocess.call(cmd, shell=True)
        try:
            Result.saveAsTextFile(outpath)
        except:
            pass
        finally:
            sc.stop()

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
from scipy.stats import norm
from operator import add
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

def hazard_cal(line, cal_date):
    shop_id, member_id, gap_mean_adj, first_transaction_date, last_transaction_date = line.strip().split(MIGO_SEPARATOR_LEVEL1)
    if float(gap_mean_adj) > 0.0:
        rc = (datetime.datetime.strptime(cal_date,'%Y%m%d').date() + datetime.timedelta(days=int(7)) - datetime.datetime.strptime(last_transaction_date[0:10].replace('-',''), '%Y%m%d').date()).days
        sigma_adj = float(gap_mean_adj)*(math.sqrt(math.pi)/math.sqrt(2))

        x = rc / (float(gap_mean_adj)* math.sqrt(math.pi))
        cdf = float(2 * norm.cdf(x) - 1)
        pdf = (math.sqrt(2)/(sigma_adj * math.sqrt(math.pi)))*math.exp(-1* (rc**2/(2*(sigma_adj**2))))
        if cdf == 1:
            hazard = 0
        else:
            hazard = pdf/( 1- cdf)
        return (shop_id.encode('utf-8'),member_id.encode('utf-8'),gap_mean_adj,first_transaction_date,last_transaction_date,hazard)
    else:
        return (shop_id.encode('utf-8'),member_id.encode('utf-8'),gap_mean_adj,first_transaction_date,last_transaction_date,-999)

def pre_prob(key, ls):
        r = []
        for x in ls:
            total = sum(float(y[1]) for y in ls if float(y[0]) >= float(x[0]))
            r.append(("{}_{}".format(key ,x[0]), ((total - float(x[1])) / total)))
        return r

if __name__ == "__main__":

    '''
    Usage: python /data/migo/athena/lib/util/spark_pre_hazard_rate.py -s meishilin -d 20160508
    '''
    shopid, caldate = main(sys.argv[1:])
    #SparkContext.setSystemProperty('spark.executor.cores', '8')
    SparkContext.setSystemProperty('spark.cores.max', '56')

    sc = SparkContext(appName="hazard_calculation_{shopid}_{date}".format(shopid=shopid, date=caldate))

    lrfm_path='/user/athena/{shopid}/lrfm/{caldate}'.format(shopid=shopid, caldate=caldate)
    adjusted_path = "/user/athena/{shopid}/adjusted_interval/{caldate}/".format(shopid=shopid, caldate=caldate)
    nes_path = "/user/athena/{shopid}/nes_tag_week/{caldate}/*".format(shopid=shopid, caldate=caldate)
    outpath = "/user/athena/{shopid}/hazard_hml/{caldate}".format(shopid=shopid, caldate=caldate)

    rdd1 = sc.textFile(lrfm_path).map(lambda x: x.split("\t")).cache()
    a = rdd1.map(lambda x: ('{}_{}'.format(x[0].encode('utf-8'), str(int(float(x[4]) - 1))),1)).reduceByKey(add).map(lambda x: (x[0].split('_')[0], (x[0].split('_')[1], x[1]))).groupByKey() \
        .map(lambda x: pre_prob(x[0] ,list(x[1]))).flatMap(lambda x: x)

    Fre = rdd1.map(lambda x: ('{}_{}'.format(x[0].encode('utf-8'), str(int(float(x[4]) - 1))),x[1])).leftOuterJoin(a).map( lambda j: ('{}_{}'.format(j[0].split('_')[0], j[1][0].encode('utf-8')),j[1][1]))

    r1 = sc.textFile(adjusted_path, 40).map(lambda x: hazard_cal(x, caldate)).filter(lambda x: x[5] != -999).map(lambda h: ("{}_{}".format(h[0], h[1]),(h[2], h[3], h[4], h[5])) ).cache()
    r = r1.leftOuterJoin(Fre).map(lambda p: (p[0].split('_')[0], p[0].split('_')[1], p[1][0][0], p[1][0][1], p[1][0][2], float(p[1][0][3])* float(p[1][1] if p[1][1] != None else 1)))
    r_nes = sc.textFile(nes_path).filter(lambda l: l.split('\t')[2] != 'S3').map(lambda l: ("{}_{}".format(l.split('\t')[0].encode('utf-8'),l.split('\t')[1].encode('utf-8')),l.split('\t')[2])).cache()
    r_hazard = r.filter(lambda o: float(o[2]) > 0 ).map(lambda l: ("{}_{}".format(l[0],l[1]) \
                       ,(l[5],(datetime.datetime.strptime(l[4][0:10].replace('-',''),'%Y%m%d').date() + datetime.timedelta(days = math.floor(float(l[2])))).strftime('%Y%m%d')))).cache()

    R1 = r_nes.join(r_hazard).cache()
    R = R1.map(lambda l: (l[0].split('_')[0],float(l[1][1][0])) ).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()

    D = {}
    for obj in R:
        storeid = obj[0]
        ls = obj[1]
        if ls:
            p50 = reduce(lambda x, y: x + y, ls) / len(ls)
            ls2 = [i for i in ls if i > p50]
            if ls2:
                p75 = reduce(lambda x, y: x + y, ls2) / len(ls2)
            else:
                p75 = p50
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
    (l[0].split('_')[1], l[1][0] ,float(l[1][1][0]), l[1][1][1]) \
    )).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : hml(x[0], x[1], D)).flatMap(lambda x : x)
    
    serial = Result.count()
    if serial > 0:
        if util_hadoop.PathIsExit(outpath):
            cmd = "hadoop fs -rm -r {}".format(outpath)
            subprocess.call(cmd, shell=True)
        try:
            Result.coalesce(10).saveAsTextFile(outpath)
        except:
            pass
        finally:
            pass
    sc.stop()

#!/usr/bin/env python
#coding=UTF-8

import os
import sys
sys.path.append("/data/migo/athena/lib")
from athena_variable import *
import sys
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import datetime
import json
import util_hadoop
import log

DICT = [{ "ALL_W"     :"0,0,0" },
        { "N0_W"      :"40,0,0" },
        { "E0_W"      :"128,0,0" },
        { "VALID_W"   :"1704,0,0" },
        { "EXISTING_W":"1664,0,0" },
        { "S1_W"      :"512,0,0" },
        { "S2_W"      :"1024,0,0" },
        { "S3_W"      :"2048,0,0" },
        { "BE_N0_D"   :"1073741824,0,0" },
        { "BE_S1_D"   :"134217728,0,0" },
        { "BE_S2_D"   :"268435456,0,0" },
        { "BE_S3_D"   :"536870912,0,0" },
        { "BE_BUY_D"  :"67108864,0,0" },
        { "BE_N0_W"   :"33554432,0,0" },
        { "BE_S1_W"   :"4194304,0,0" },
        { "BE_S2_W"   :"8388608,0,0" },
        { "BE_S3_W"   :"16777216,0,0" },
        { "BE_BUY_W"  :"2097152,0,0" }]

if __name__ == "__main__" :
    """
    error code :    1. ['-1']   => no ta_done file
    """
    try:
        shop = sys.argv[1]
        caldate = sys.argv[2]
        ta = "/user/athena/{shop}/ta_done/{caldate}".format(shop=shop, caldate=caldate)
        MSG = ""
        if util_hadoop.PathIsExit(ta): 
            start_time = datetime.datetime.now().second

            cf = SparkConf().setAppName("[ET_update_member-{}-{}] {}".format(shop, caldate, start_time)).set("spark.cores.max", "40")
            sc = SparkContext(conf = cf)
            raw_ta = sc.textFile(ta, 20).filter(lambda x: x.split("\t")[0] == shop).cache()
            R = []

            for x in DICT:
                tag1 = int(x[x.keys()[0]].split(",")[0])
                tag = x.keys()[0]
                if tag1 == 0:
                    counts = raw_ta.filter(lambda x: x.split("\t")[2] == "L7D").count()
                else:
                    counts = raw_ta.filter(lambda x: x.split("\t")[2] == "L7D" and int(x.split("\t")[4]) & int(tag1)).count()
                R.append("{tag}:{counts}".format(tag=tag, counts=counts))

            print ",".join(R)
            sc.stop()
            end_time = datetime.datetime.now().second
        else:
            MSG = "ta file is not exist"
            print "#migoError#\t{}".format(MSG)
    except Exception as e:
        MSG = str(e)
        print "#migoError#\t{}".format(MSG)

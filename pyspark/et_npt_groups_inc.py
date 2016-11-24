#!/usr/bin/env python
#coding=UTF-8

import os, sys, getopt
import subprocess
import shutil
import itertools
import math
sys.path.append("/data/migo/pandora/lib")
sys.path.append("/data/migo/athena/lib")
sys.path.append("/data/migo/athena/lib/util")
from athena_variable import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

import pymongo
import datetime, time
import copy
import util_hadoop
import urllib2, json
import ast

DICT_TAG = {str(MIGO_TAG1_N0_W):'N' ,str(MIGO_TAG1_EB_W):'N',str(MIGO_TAG1_S1):'S1', str(MIGO_TAG1_S2):'S2', str(MIGO_TAG1_S3):'S3', str(MIGO_TAG1_E0_W):'E0'}

def filters(line):
    for x in DICT_TAG:
        if int(line[1][1][0].encode('utf-8')) & int(x):
            return ("{}#migo#{}#migo#{}".format(line[1][1][1].encode('utf-8'),DICT_TAG[x], line[1][0].encode('utf-8')), line[0].encode('utf-8'))

def profiles(x):
    try:
        ls = x[1:-1].split('","')
        email = '1' if ls[3] !="" else '0'
        cell = '1' if ls[4] !="" else '0'
        wechat = '1' if ls[7] !="" else '0'
        return (ls[1],'{},{},{}'.format(cell,wechat,email))
    except:
        return (ls[1],'0,0,0')

def puthdfs(local_p, hdfs_p):

    cmd = "hadoop fs -test -d {}".format(os.path.dirname(hdfs_p))
    r = subprocess.call(cmd, shell=True)
    if r != 0:
        #mkdir path
        cmd = "hadoop fs -mkdir -p {}".format(os.path.dirname(hdfs_p))
        print 'mkdir path'
        r = subprocess.call(cmd, shell=True)
    else:
        #path is exists , so test if file is exists
        cmd = "hadoop fs -test -f {}".format(hdfs_p)
        r = subprocess.call(cmd, shell=True)
        if r == 0:
            #file is exists , so rm file
            cmd = "hadoop fs -rm {}".format(hdfs_p)
            r = subprocess.call(cmd, shell=True)

    #rename tmp file
    ran = int(time.mktime(datetime.datetime.now().timetuple()))
    tmpfile = '/tmp/etgroup_{}.txt'.format(ran)
    cmd = "mv {} {}".format(local_p, tmpfile)
    subprocess.call(cmd, shell=True)

    #put file up to hadoop
    cmd = "hadoop fs -put {} {}".format(tmpfile, hdfs_p)
    print 'put file'
    r = subprocess.call(cmd, shell=True)

    #remove tmp file
    if r == 0:
        print 'remove tmp file'
        os.remove(tmpfile)
    return r

def getabgroup(setting, D):

    l = setting.split(",")
    R = []
    n = 1
    for x in l:
        rato = float(x)/float(100)
        R.append({'g': D[n], 'r':rato})
        n = n + 1
    return R

if __name__ == "__main__":

    '''
    TA_Done
    circle	20151231	L31D	125fish	2048	0	0
    circle	20151231	L31D	Elin1214	4112	0	0
    circle	20151231	L31D	Natalie chen	2048	0	0

    ABList
    circle^EC	1,0,0	E0	chingshieeTl02yahoo.com.tw	@B@
    circle^EC	1,0,0	E0	michelle7924	@A@
    circle^EC	1,0,0	E0	cindy200399	@B@
    '''
    D = {1:'A', 2:'B', 3:'C', 4:'D', 5:'E'}

    #parameter
    shopid = sys.argv[1]
    caldate = sys.argv[2]
    
    hdfs = "/user/athena/{}/et_group/groups.txt".format(shopid)
    src = "/user/migo/starterDIY/{}/member/done/*".format(shopid)
    ta_parent = "/user/athena/{}/ta_done/{}".format(shopid,caldate)
    ta_path = ta_parent + "/*"
    et_init = "/laplace/enablecompanycodes"
    eturl = "{host}{url}".format(host=MIGO_AP_ET_PREFIX, url=et_init)

    #get all et enable clients
    enable_client = []
    cmd = 'curl -s -k {}'.format(eturl)
    try:
        r = os.popen(cmd).read().strip()
        if r != '':
            data = ast.literal_eval(r)
            if data['code'] == 0:
                enable_client = data['result']['enable_company_codes'].split(',')
    except:
        pass
 
    if shopid in enable_client:
        #spark init
        if util_hadoop.PathIsExit(os.path.dirname(hdfs)):
            #get laste ta
            if not util_hadoop.PathIsExit(ta_parent):
                ls = util_hadoop.GetPath(ta_parent.rsplit("/",1)[0])
                ta_path = max(ls) + "/*"
                lsdate = max(ls).rsplit("/",1)[1] #the laste hadoop ta date
        
            #ta is exist
            SparkContext.setSystemProperty('spark.executor.cores', '4')
            SparkContext.setSystemProperty('spark.cores.max', '48')
            sc = SparkContext(appName="et_groups_inc_calculation_member_{}".format(shopid))
            def mapp(line):
                try:
                    return line.split('\t')[3]
                except:
                    pass
            ab_list = sc.textFile(hdfs, 20).filter(lambda line:line.split('\t')[0] == shopid).map(lambda line : mapp(line)).collect()
            ta_pool_tmp = sc.textFile(ta_path, 100).filter(lambda line: line.split('\t')[2] == 'L7D' and int(line.split('\t')[4]) & 40 and line.split('\t')[3] not in ab_list ).cache()
            ta_pool = ta_pool_tmp.map(lambda l: (l.split('\t')[3],(l.split('\t')[4],l.split('\t')[0])))
            members = sc.textFile(src)
            sms = members.map(lambda line: profiles(line)).cache()
            ta_nes = sms.join(ta_pool).map(lambda line: filters(line)).groupByKey().collect()
            #config path /user/athena/circle/et_group/config/*
            configpath = "/user/athena/{}/et_group/config/*".format(shopid)
            setting = sc.textFile(configpath).collect()[0].split("\t")[1]
            R = getabgroup(setting, D)
            sc.stop()

            path = "/tmp/et/ab/{}/{}".format(shopid,caldate)
            logpath = "/tmp/et/ablog/{}/{}".format(shopid, caldate)

            files = path + "/member.txt"
            logfile = logpath + "/{}log.txt".format(caldate)

            if not os.path.exists(path):
                os.makedirs(path)
            else:
                if os.path.isfile(files):
                    os.remove(files)

            if not os.path.exists(logpath):
                os.makedirs(logpath)
            else:
                if os.path.isfile(logfile):
                    os.remove(logfile)
            #getmerge
            cmd = "hadoop fs -get {} {}".format(hdfs, files)
            r = subprocess.call(cmd, shell=True)
            fh = open(files, "a")
            fh2 = open(logfile, "wb")

            if len(ta_nes) > 0 :
                for x in ta_nes:
                    n = 0
                    tag = x[0].split('#migo#')[1]
                    storeid = x[0].split('#migo#')[0]
                    channel = x[0].split('#migo#')[2]
                    linex = "{storeid}{sep}{allcount}\n".format(storeid=storeid, allcount=len(x[1]), sep="\t")
                    fh2.write(linex)

                    line = ""
                    for rr in R:
                        C = rr['r']*len(x[1])
                        rr['count'] = C
                    for y in x[1]:
                        for o in R:
                            if o['count'] > 0:
                                o['count'] = o['count'] - 1
                                line = "{}{sep}{}{sep}{}{sep}{}{sep}@{}@\n".format(storeid, channel, tag, y, o['g'], sep="\t")
                                #print line
                                fh.write(line)
                                line2 = "{storeid}{sep}{g}{sep}{count}\n".format(storeid=storeid, g=o['g'], count=o['count'], sep="\t" )
                                fh2.write(line2)
                                break
                            else:
                                continue
                fh.close()
                fh2.close()
                hdfs = "/user/athena/{}/et_group/groups.txt".format(shopid)
                puthdfs(files, hdfs)
        else:
            print "error no ab data"

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

import pymongo
import datetime, time
import util_hadoop
import log
import sendmail

DICT_TAG = {str(MIGO_TAG1_N0_W):'N' ,str(MIGO_TAG1_EB_W):'N',str(MIGO_TAG1_S1):'S1', str(MIGO_TAG1_S2):'S2', str(MIGO_TAG1_S3):'S3', str(MIGO_TAG1_E0_W):'E0'}

def filters(line):
    for x in DICT_TAG:
        if int(line[1][1][0].encode('utf-8')) & int(x):
            return ("{}#migo#{}#migo#{}".format(line[1][1][1],DICT_TAG[x], line[1][0]), line[0])

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
        #print 'mkdir path'
        r = subprocess.call(cmd, shell=True)
    else:
        #path is exists , so test if file is exists
        cmd = "hadoop fs -test -f {}".format(hdfs_p)
        r = subprocess.call(cmd, shell=True)
        if r == 0:
            #file is exists , so rm file
            cmd = "hadoop fs -rm {}".format(hdfs_p)
            r = subprocess.call(cmd, shell=True)

    #put file up to hadoop
    cmd = "hadoop fs -put {} {}".format(local_p, hdfs_p)
    #print 'put file'
    r = subprocess.call(cmd, shell=True)
    #remove tmp file
    
    if r == 0:
        #print 'remove tmp file'
        os.remove(local_p)
        os.removedirs(os.path.dirname(local_p))
    
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
    circle	20151231	L31D	125fish	2048	0	0
    circle	20151231	L31D	Elin1214	4112	0	0
    circle	20151231	L31D	Natalie chen	2048	0	0
    circle	20151231	L31D	Pingjuhuang	2048	0	0
    circle	20151231	L31D	Y5541192264	2048	0	0
    circle	20151231	L31D	a0983302187	2048	0	0
    '''
    logger = log.log()
    D = {1:'A', 2:'B', 3:'C', 4:'D', 5:'E'}
    caldate = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

    #parameter
    if len(sys.argv) == 3:
        shopid = sys.argv[1]
        setting = sys.argv[2]
    elif len(sys.argv) == 4:
        shopid = sys.argv[1]
        setting = sys.argv[2]
        caldate = sys.argv[3]
    else:
        print 'parameter error ! usage:et_npt_groups.py shopid setting or et_npt_groups.py shopid setting caldate'
        sys.exit(1)

    try:
        logger.add('clientid={clientid}, setting={se}, groups seperate is init ....'.format(clientid=shopid, se=setting))
        R = getabgroup(setting, D)
        #spark init
        prefix = "hdfs://{hdfsmaster}:8020".format(hdfsmaster=MIGO_HDFS_MASTER)

        src = prefix + "/user/migo/starterDIY/{}/member/done/*".format(shopid)
        ta_parent = prefix + "/user/athena/{}/ta_done/{}".format(shopid,caldate)
        ta_path = ta_parent + "/*"

        #get laste ta
        if not util_hadoop.PathIsExit(ta_parent):
            ls = util_hadoop.GetPath(ta_parent.rsplit("/",1)[0])
            ta_path = max(ls) + "/*"
            caldate = max(ls).rsplit("/",1)[1] #the laste hadoop ta date
            

        sc = SparkContext(appName="ab_group_init_{}".format(shopid))
        configpath = "/user/athena/{}/et_group/config".format(shopid)

        if util_hadoop.PathIsExit(configpath):
            cmd = "hadoop fs -rm -r {}".format(configpath)
            subprocess.call(cmd, shell=True)

        sc.parallelize(["{}\t{}".format(shopid,setting)]).coalesce(1).saveAsTextFile(configpath)

        members = sc.textFile(src)
        sms = members.map(lambda line: profiles(line))

        ta_pool = sc.textFile(ta_path)
        ta_join = ta_pool.filter(lambda line: line.split('\t')[2] == 'L7D').map(lambda l: (l.split('\t')[3].encode('utf-8'),(l.split('\t')[4].encode('utf-8'),l.split('\t')[0].encode('utf-8'))))
        ta_nes = sms.join(ta_join).map(lambda line: filters(line)).groupByKey().collect()
        sc.stop()
        logger.add('clientid={clientid}, setting={se}, groups spark job is done ....'.format(clientid=shopid, se=setting))

        path = "/tmp/et/ab/{}/{}".format(shopid, caldate)
        files = path + "/member.txt"
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            if os.path.isfile(files):
                os.remove(files)
        
        fh = open(files, "wb")
        for x in ta_nes:
            n = 0
            tag = x[0].split('#migo#')[1]
            storeid = x[0].split('#migo#')[0]
            channel = x[0].split('#migo#')[2]
            line = ""
            for rr in R:
                C = rr['r']*len(x[1])
                rr['count'] = C

            for y in x[1]:
                for o in R:
                    if o['count'] > 0:
                        o['count'] = o['count'] - 1
                        line = "{}{sep}{}{sep}{}{sep}{}{sep}@{}@\n".format(storeid, channel, tag, y, o['g'], sep="\t")
                        fh.write(line)
                        break
                    else:
                        continue
        fh.close()
        #put hadoop
        hdfs = "/user/athena/{}/et_group/groups.txt".format(shopid)
        puthdfs(files, hdfs)
        print '{migo}caldate={}{migo}'.format(caldate, migo='#migo#')
        print 'migo0'
        logger.add('clientid={clientid}, setting={se}, groups spark job is success ....'.format(clientid=shopid, se=setting))
    except Exception as e:
        logger.add('clientid={clientid}, setting={se}, groups spark job is Error ...\n{msg}'.format(clientid=shopid, se=setting, msg=str(e)))
        print 'migo1'

#!/usr/bin/env python
#coding=UTF-8

import os, sys, getopt
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
import log
import sendmail

DICT_TAG = {str(MIGO_TAG1_N0_W):'N' ,str(MIGO_TAG1_EB_W):'N',str(MIGO_TAG1_S1):'S1', str(MIGO_TAG1_S2):'S2', str(MIGO_TAG1_S3):'S3', str(MIGO_TAG1_E0_W):'E0'}
DICT = {}
CAMPAGIN1 = {'shopid':'', 'cid':'', 'storeid':'', 'type':'O', 'start':'', 'end':'', 'reset':0, 'period':'w', 'tag1':0, 'tag2':0 ,'tag3':0, 'exclude_cid':0, 'flag':0, 'top':[], 'member':{}, 'channel':[], 'filter':{} }
chdict = {'0':'sms','1':'wechat','2':'edm'}

def gettag(x):
    #because have only one channel so 1,0,0
    ls = x.split("\t")
    tag = ls[4]
    if tag == '0':
        return (ls[3], ('-999', [1,0,0]))
    else:
        for x in DICT_TAG:
            if int(tag) & int(x):
                return (ls[3], (DICT_TAG[x], [1,0,0]))

def getchannel(x):
    ls = x[1:-1].split('","')
    try:
        #0- ShopID, 1-MemberID, 3 - email , 4.cellphone 7.wechat
        email = 1 if ls[3] !="" else 0
        cell = 1 if ls[4] !="" else 0
        wechat = 1 if ls[7] !="" else 0
        return (ls[1],[cell,wechat,email])
    except:
        return (ls[1],[0,0,0])

if __name__ == "__main__":
    '''
    old ta
    usage:python ta_query.py kgtp 20140101 40 00A 20140101 20140105 O 7 1 0

    new ta_done
    kgtp	20141231	L31D	100192	41216	0	0
    '''
    shopid = sys.argv[1]
    period = sys.argv[2]
    cid = sys.argv[3]
    start = sys.argv[4]
    end = sys.argv[5]
    channel = sys.argv[6]
    dictory = {'sms':1, 'wechat':2, 'edm':3}
    logger = log.log()
    try:
        logger.add('et_update_list is init by {shopid} {period} {cid} {start} {end} {channel}'.format(shopid=shopid, period=period, cid=cid, start=start, end=end, channel=channel))
        client1 = pymongo.MongoClient(MIGO_MONGO_TA_URL, 27017)
        db1 = client1['ET_{0}'.format(shopid)]
        types = db1['campaign'].find_one({'cid':cid})['type']

        if types.lower() == 'o':
            days = db1['campaign'].find_one({'cid':cid})['member'].keys()
            lsdate = ""
            if period in days:
                lsdate = db1['campaign'].find_one({'cid':cid})['member'][period]['lsdate']
            if lsdate == '':
                lsdate = period
        else:
            lsdate = period
        
        member_ls = []
        #members = sc.textFile(src)
        filters = []

        #get filter list
        db = client1[MIGO_MONGO_ETDB]
        col = db['TaMember_{cid}_Subscription'.format(cid=cid)]
        ls = col.find({'Period':int(period), 'ChannelId':dictory[channel], 'SendMode':0})

        for x in ls :
            filters.append(x['MemberId'])
        client1.close()
        logger.add('shopid={shopid} ,cid={cid} et_update_list get Subscription {c}.......'.format(shopid=shopid, cid=cid, c=len(filters)))

        merge_path = "/user/athena/{}/meb_attribute/{}".format(shopid, lsdate)
        if not util_hadoop.PathIsExit(merge_path):
            ls = util_hadoop.GetPath(merge_path.rsplit("/",1)[0])
            lsdate = max(ls).rsplit('/',1)[1]
            merge_path = "/user/athena/{}/meb_attribute/{}".format(shopid, lsdate)
            logger.add('shopid={shopid} ,cid={cid} et_update_list get ta by last date {lsdate}.......'.format(shopid=shopid, cid=cid, lsdate=lsdate))

        SparkContext.setSystemProperty('spark.cores.max', '56')
        sc = SparkContext(appName="et_update_list_{}".format(shopid))

        def mapp(l):
            npt = l.split("\t")[12] if l.split("\t")[12] != 'cindy1' else 'S3'
            return {'memberid': l.split("\t")[1],'tag': l.split("\t")[4], 'channel':[1,0,0], 'npt':npt}
        
        member_ls = sc.textFile(merge_path, 30).filter(lambda l: l.split("\t")[0] == shopid and l.split("\t")[3] == "L7D" and l.split("\t")[1] in filters) \
                .map(lambda l: mapp(l)).collect()

        logger.add('shopid={shopid} ,cid={cid} et_update_list get Result {c}.......'.format(shopid=shopid, cid=cid, c=len(member_ls)))
        member = {'monitor':{'sms':{},'wechat':{},'edm':{}}}
        member['monitor'][channel] = {'start':start, 'end':end, 'members':member_ls}
        raw_count = len(member_ls)
        sc.stop()
        logger.add('shopid={shopid} ,cid={cid} et_update_list Spark Job Done .......'.format(shopid=shopid, cid=cid))
        client = pymongo.MongoClient(MIGO_MONGO_TA_URL, 27017)

        db = client['ET_' + shopid ]
        collectname = '{cid}_{period}_{channel}_{start}_{end}'.format(cid=cid, period=period, channel=channel, start=start, end=end)

        if len(member_ls) > 0:
            if collectname in db.collection_names():
                db[collectname].drop()

            col = db[collectname]
            col.insert(member_ls)
        client.close()
        print 1
    except Exception as e:
        maillist = ['Wendell_Huang@migocorp.com', 'Fergus_Chen@migocorp.com', 'Christine_Huang@migocorp.com', 'James_Liang@migocorp.com', 'Duncan_Du@migocorp.com', 'Kency_Huang@migocorp.com']
        subject = 'ET Error Note'
        From = 'System Mail'
        To = 'RDS'
        contents = 'Product Site, The shop {shopid} , campaignid {cid} on date {date} is failed, parameter {p}\n Error Msg is "{err}"'.format(shopid=shopid, cid=cid, date=period, p=sys.argv, err=str(e))
        sendmail.sendmail(maillist, subject, From, To, contents)
        logger.add('shopid={shopid} ,cid={cid} et_update_list Spark Job Error ...{err}'.format(shopid=shopid, cid=cid, err=str(e)))
        print -1

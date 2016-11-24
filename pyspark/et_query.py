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
import random
import sendmail

DICT_TAG = {str(MIGO_TAG1_N0_W):'N' ,str(MIGO_TAG1_EB_W):'N',str(MIGO_TAG1_S1):'S1', str(MIGO_TAG1_S2):'S2', str(MIGO_TAG1_S3):'S3', str(MIGO_TAG1_E0_W):'E0'}
DICT = {}
CAMPAGIN1 = {'shopid':'', 'cid':'', 'storeid':'', 'type':'O', 'start':'', 'end':'', 'reset':0, 'tag1':0, 'tag2':0 ,'tag3':0, 'exclude_cid':0, 'flag':0, 'top':[], 'member':{}, 'channel':[], 'filter':{} }
chdict = {'0':'sms','1':'wechat','2':'edm'}

FHValue = MIGO_TAG2_N0W_FH + MIGO_TAG2_E0W_FH + MIGO_TAG2_S1_FH + MIGO_TAG2_S2_FH + MIGO_TAG2_S3_FH
FMValue = MIGO_TAG2_N0W_FM + MIGO_TAG2_E0W_FM + MIGO_TAG2_S1_FM + MIGO_TAG2_S2_FM + MIGO_TAG2_S3_FM
FLValue = MIGO_TAG2_N0W_FL + MIGO_TAG2_E0W_FL + MIGO_TAG2_S1_FL + MIGO_TAG2_S2_FL + MIGO_TAG2_S3_FL
D1 = {'H':FHValue, 'M':FMValue, 'L':FLValue}

MHValue = MIGO_TAG3_N0W_MH + MIGO_TAG3_E0W_MH + MIGO_TAG3_S1_MH + MIGO_TAG3_S2_MH + MIGO_TAG3_S3_MH
MMValue = MIGO_TAG3_N0W_MM + MIGO_TAG3_E0W_MM + MIGO_TAG3_S1_MM + MIGO_TAG3_S2_MM + MIGO_TAG3_S3_MM
MLValue = MIGO_TAG3_N0W_ML + MIGO_TAG3_E0W_ML + MIGO_TAG3_S1_ML + MIGO_TAG3_S2_ML + MIGO_TAG3_S3_ML
D2 = {'H':MHValue, 'M':MMValue, 'L':MLValue}

def adj_filter(ls, first, last, shopid, caldate):
    
    start = datetime.datetime.strptime(ls[8][0:10], '%Y-%m-%d').date()
    end = datetime.datetime.strptime(ls[9][0:10], '%Y-%m-%d').date()
    r_s, r_e = first.split(',')
    if ls[0] == shopid:
        if first.count("-1") == 0 and last.count("-1") == 0:
            if (datetime.datetime.strptime(r_s, '%Y%m%d').date() <= start <= datetime.datetime.strptime(r_e, '%Y%m%d').date()) and end < (datetime.datetime.now() - datetime.timedelta(days=int(last))).date():
                return True
            else:
                return False
        elif first.count("-1") == 0:
            if datetime.datetime.strptime(r_s, '%Y%m%d').date() > start or start > datetime.datetime.strptime(r_e, '%Y%m%d').date():
                return False
        elif last.count("-1") == 0:
            if end >= datetime.datetime.strptime(caldate, '%Y%m%d').date() - datetime.timedelta(days=int(last)):
                return False
            else:
                return True
        else:
            return True
    else:
        return False
    return True

def merge1(l, tag1, M, F):

    tag = l[15]
    f = l[3]
    m = l[4]
    F_ls = []
    M_ls = []
    if F != "-1":
        F_ls = F.split(',')

    if M != "-1":
        M_ls = M.split(',')

    if tag1 == '0' and F == "-1" and M == "-1":
        return True
    else:
        if tag1 != '0':
            if int(tag1) & int(tag):
                if F == "-1" and M == "-1":
                    return True
                elif F == "-1":
                    if m in M_ls:
                        return True
                    else:
                        return False
                elif M == "-1":
                    if f in F_ls:
                        return True
                    else:
                        return False
                else:
                    if m in M_ls and f in F_ls:
                        return True
                    else:
                        return False
            else:
                return False
        else:
            if F == "-1" and M == "-1":
                return True
            elif F == "-1":
                if m in M_ls:
                    return True
                else:
                    return False
            elif M == "-1":
                if f in F_ls:
                    return True
                else:
                    return False
            else:
                if m in M_ls and f in F_ls:
                    return True
                else:
                    return False
def channelmap(l):
    memberid = l[0]
    tag = l[1][0]
    edm = 1 if l[1][1] != "" else 0
    sms = 1 if l[1][2] != "" else 0
    wechat = 1 if l[1][3] != "" else 0
    return {'memberid':memberid, 'tag':tag, 'channel':[sms, wechat, edm]}

def mafilter(line, groups, shopid):
    try:
        if line.split('\t')[0] == shopid and line.split('\t')[4] == "@{g}@".format(g=groups):
            return True
        else:
            return False
    except:
        return False

if __name__ == "__main__":
    '''
    usage:python ta_query.py kgtp 20140101 40 00A 20140101 20140105 O 7 1 0
    '''
    shopid = sys.argv[1]
    caldate = sys.argv[2]
    tag1 = sys.argv[3]
    cid = sys.argv[4]
    start = sys.argv[5]
    end = sys.argv[6]
    type = sys.argv[7]
    flag = sys.argv[8]
    reset = sys.argv[9]
    M = sys.argv[10]
    F = sys.argv[11]
    npt = sys.argv[12]
    age = sys.argv[13]
    gender = sys.argv[14]
    first = sys.argv[15]
    last = sys.argv[16]
    groups = sys.argv[17]
    top = sys.argv[18]
    npd = sys.argv[19]

    storeid = shopid
    logger = log.log()
    lsdate = caldate
    UPDATE = True

    try:
        prefix = "hdfs://{hdfsmaster}:8020".format(hdfsmaster=MIGO_HDFS_MASTER)
        merge_path = prefix + "/user/athena/{shop}/meb_attribute/{caldate}".format(shop=shopid, caldate=caldate)
        lsdate = caldate

        logger.add('clientid={clientid}, cid={cid}, et_query is init by {shopid} {caldate} {tag1} {cid} {start} {end} {type} {flag} {reset} {M} {F} {npt} {age} {gender} {first} {last} {groups} {top} {npd}' \
        .format(clientid=shopid, cid=cid, shopid=shopid, caldate=caldate, tag1=tag1, start=start, end=end, type=type, flag=flag ,reset=reset, M=M, F=F, npt=npt, age=age, gender=gender, \
                first=first, last=last, groups=groups, top=top, npd=npd ))

        CONTINUE = True
        MSG = ""

        if not util_hadoop.PathIsExit(merge_path):
            if util_hadoop.PathIsExit(merge_path.rsplit("/",1)[0]):
                ls = util_hadoop.GetPath(merge_path.rsplit("/",1)[0])
                merge_path = "hdfs:" + max(ls) + "/*"
                lsdate = max(ls).rsplit("/",1)[1]
            else:
                CONTINUE = False
                MSG = 'member cube is not exists...'
        else:
            merge_path = merge_path + "/*"

        client = pymongo.MongoClient(MIGO_MONGO_TA_URL, 27017)
        db = client['ET_' + shopid ]
        col = db['campaign']
        up = False 
        if flag == "0":
            pass
        else:
            member = {'monitor':{'sms':{},'wechat':{},'edm':{}}}
            if type.lower() == 'o':
                for x in chdict:
                    member['monitor'][chdict[x]] = {'start':start, 'end':end, 'members':[]}

            if reset == "1":
                cam = col.find({'cid':cid})
                S = datetime.datetime.strptime(start, '%Y%m%d')
                E = datetime.datetime.strptime(end, '%Y%m%d')
                if cam.count() == 1:
                    S = cam[0]['start']
                    E = cam[0]['end']
                CAMPAGIN = copy.deepcopy(CAMPAGIN1)
                CAMPAGIN['shopid'] = shopid
                CAMPAGIN['storeid'] = shopid #storeid default = shopid
                CAMPAGIN['cid'] = cid
                CAMPAGIN['type'] = type
                CAMPAGIN['start'] = S
                CAMPAGIN['end'] = E
                CAMPAGIN['tag1'] = tag1
                CAMPAGIN['flag'] = flag
                CAMPAGIN['F'] = F
                CAMPAGIN['M'] = M
                CAMPAGIN['npt'] = npt
                CAMPAGIN['gender'] = gender
                CAMPAGIN['age'] = age
                CAMPAGIN['first'] = first
                CAMPAGIN['last'] = last
                CAMPAGIN['groups'] = groups
                CAMPAGIN['top'] = top
                CAMPAGIN['npd'] = npd
                CAMPAGIN['member'][caldate] = member

                if type.lower() == 'o':
                    CAMPAGIN['member'][caldate]['lsdate'] = lsdate
                else:
                    CAMPAGIN['member'][caldate]['lsdate'] = ""

                if cam.count() > 0:
                    col.remove({'cid':cid})
                col.insert(CAMPAGIN)
            else:
                logger.add('clientid={clientid}, cid={cid}, update list....testing ....'.format(clientid=shopid, cid=cid))
                if col.find_one({'cid':cid}):
                    all_date = col.find_one({'cid':cid})['member']
                    lastday = max(sorted(all_date.items()))[0]
                    prelsdate = all_date[lastday]['lsdate']
                    if lsdate == prelsdate:
                        CONTINUE = False
                        UPDATE = False
                        MSG = "ta date is not update"
                    else:
                        up = True
                    #col.update( {'cid':cid}, {'$set':{'end':datetime.datetime.strptime(end, '%Y%m%d')} })
        if CONTINUE:
            SparkContext.setSystemProperty('spark.cores.max', '48')
            sc = SparkContext(appName="[ET] et_query by merge source {shopid}_cid:{cid}".format(shopid=shopid, cid=cid))
            merge = sc.textFile(merge_path, 20).filter(lambda l: l.split("\t")[0] == shopid and l.split("\t")[3] == "L7D") \
            .map(lambda l: (l.split("\t")[0], l.split("\t")[1], l.split("\t")[4], l.split("\t")[5], l.split("\t")[6], l.split("\t")[11] \
            ,l.split("\t")[12], l.split("\t")[13], l.split("\t")[15] , l.split("\t")[16] ,l.split("\t")[17], l.split("\t")[18] \
            ,l.split("\t")[19], l.split("\t")[20], l.split("\t")[21], l.split("\t")[7], random.random() \
            )).cache()
            ####    schema    ####
            # shopid, memberid, nes, f, m, pr,
            # npt_hml, npd, first, last, sex, age,
            # email, phone, wechat, tag1, ran
            ####    schema    ####

            ta = merge.filter(lambda l: merge1(l, tag1, M, F))
            logger.add('clientid={clientid}, cid={cid}, get ta is finish....'.format(clientid=shopid, cid=cid))

            if top.count('-1') == 0 or npt.count('-1') == 0:
                npt_s = ta.filter(lambda l: l[6] != "").cache()
                if top.count('-1') == 0:
                    total = npt_s.count()
                    ### jira ECR-47
                    T = 0
                    types, values = top.split(',')
                    if types == '0':
                        T = int(values)
                    else:
                        T = int(float(total) * float(values) / 100)
                    npt_src1 = npt_s.filter(lambda x: x[5] != 'cindy1').sortBy(lambda x: float(x[5]) + float(x[16]), True, 30).zipWithIndex().filter(lambda xi: xi[1] < T).keys().cache()

                elif npt.count('-1') == 0:
                    npt_ls = npt.split(",")
                    npt_src1 = npt_s.filter(lambda l: l[6] in npt_ls).cache()

                if npd.count('-1') == 0:
                    startd, endd = npd.split(',')
                    npt_src = npt_src1.filter(lambda l: l[7] != "" and l[7] != 'cindy1').filter(lambda l: int(startd) <= int(l[7]) <= int(endd))
                else:
                    npt_src = npt_src1
                ta_pool = npt_src    
            else:
                if npd.count('-1') == 0:
                    startd, endd = npd.split(',')
                    npt_src = ta.filter(lambda l: l[7] != "" and l[7] != 'cindy1').filter(lambda l: int(startd) <= int(l[7]) <= int(endd))
                else:
                    npt_src = ta
                ta_pool = npt_src

            logger.add('clientid={clientid}, cid={cid}, ta and npt merge is finish....'.format(clientid=shopid, cid=cid))

            if gender.count('-1') > 0 and age.count('-1') > 0:
                p = ta_pool
            else:
                if gender.count('-1') == 0 and age.count('-1') == 0:
                    p = ta_pool.filter(lambda l: l[11] != "cindy3" and l[11] != "" and l[10] != "cindy3" and l[10] != "") \
                    .filter(lambda l: int(age.split(',')[0]) <= int(l[11]) <= int(age.split(',')[1]) and l[10] == gender)

                elif gender.count('-1') == 0:
                    p = ta_pool.filter(lambda l: l[10] != "cindy3" and l[10] != "").filter(lambda l: l[10] == gender)
                elif age.count('-1') == 0:
                    p = ta_pool.filter(lambda l: l[11] != "cindy3" and l[11] != "").filter(lambda l: int(age.split(',')[0]) <= int(l[11]) <= int(age.split(',')[1]))
                else:
                    p = ta_pool

            logger.add('clientid={clientid}, cid={cid}, member profile is finish....'.format(clientid=shopid, cid=cid))
            
            if first.count("-1") == 0 or last.count("-1") == 0:
                p = p.filter(lambda line: adj_filter(line, first, last, shopid, caldate))

            if groups != '-1':
                gpath = prefix + "/user/athena/{shop}/et_group/groups.txt".format(hdfsmaster=MIGO_HDFS_MASTER, shop=shopid)
                groupslist = sc.textFile(gpath).filter(lambda line: mafilter(line, groups, shopid) ).map(lambda line: line.split('\t')[3].encode('utf-8')).collect()
                member_ls = p.filter(lambda line:line[1].encode('utf-8') in groupslist).map(lambda x: x[1].encode('utf-8')).collect()
            else:
                member_ls = p.map(lambda x: x[1].encode('utf-8')).collect()
            sc.stop()

            logger.add('clientid={clientid}, cid={cid}, total members in this time is {counts}....'.format(clientid=shopid, cid=cid, counts=len(member_ls)))
            raw_count = len(member_ls)
            r = member_ls
            if flag == "0":
                print "#migoCount#\t{count}".format(count=raw_count)
            else:
                if up:
                    col.update({'cid':cid}, {'$set':{'member.{L}.lsdate'.format(L=lastday):lsdate} })
                print r
            logger.add('clientid={clientid}, cid={cid}, Task is done....'.format(clientid=shopid, cid=cid ))
        else:
            if UPDATE:
                print "#migoError#\t{msg}".format(msg=MSG)
            else:
                r = []
                print r
        client.close()
    except Exception as e:
        maillist = MAIL_RECEIVER
        subject = 'ET Error Note'
        From = 'System Mail'
        To = 'RDS'
        contents = 'Product Site, The shop {shopid} , campaignid {cid} , parameter {p}\n Error Msg is "{err}"'.format(shopid=shopid, cid=cid, p=sys.argv, err=str(e))
        sendmail.sendmail(maillist, subject, From, To, contents)
        logger.add('clientid={clientid}, cid={cid}, Task Error...{err}'.format(clientid=shopid, cid=cid, err=str(e) ))
        print "#migoError#\t{err}".format(err=str(e))

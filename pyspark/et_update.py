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



def adj_filter(line, first, last, shopid):
    ls = line.split("\t")
    start = datetime.datetime.strptime(ls[3][0:10], '%Y-%m-%d').date()
    end = datetime.datetime.strptime(ls[4][0:10], '%Y-%m-%d').date()
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
            if end >= (datetime.datetime.now() - datetime.timedelta(days=int(last))).date():
                return False
        else:
            return True
    else:
        return False
    return True

def merge1(l, tag1, M, F):
    tag = l[14]
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
        if line.split('\t')[0] == shopid and line.split('\t')[4] == "@{}@".format(groups):
            return True
        else:
            return False
    except:
        return False

if __name__ == "__main__":

    '''
    Objective: No Use
    old ta
    TANes       kgtp    20140615        L7D     00330   2048    0       0
    usage:python ta_update.py kgtp 20150105 4194304 2 20150105 20150112 O 7 sms

    new ta_done
    kgtp	20141231	L31D	100165	41216	0	0
    kgtp	20141231	L31D	100192	41216	0	0
    '''
    #try:
    shopid = sys.argv[1]
    caldate = sys.argv[2]
    tag1 = sys.argv[3]
    cid = sys.argv[4]
    start = sys.argv[5]
    end = sys.argv[6]
    type = sys.argv[7]
    channel = sys.argv[8]
    M = sys.argv[9]
    F = sys.argv[10]
    npt = sys.argv[11]
    age = sys.argv[12]
    gender = sys.argv[13]
    first = sys.argv[14]
    last = sys.argv[15]
    groups = sys.argv[16]
    top = sys.argv[17]
    npd = sys.argv[18]

    storeid = shopid
    merge_path = "/user/athena/{}/meb_cube/{}".format(shopid, caldate)
    adjust_interval = "/user/athena/{}/adjusted_interval/{}".format(shopid, caldate)

    logger = log.log()
    lsdate = caldate
    adjust_interval_path = adjust_interval + "/*"
    isblank = False

    try:
        logger.add('clientid={clientid}, cid={cid}, et_update is init by {shopid} {caldate} {tag1} {cid} {start} {end} {type} {channel} {M} {F} {npt} {age} {gender} {first} {last} {groups} {top} {npd}' \
            .format(clientid=shopid, cid=cid, shopid=shopid, caldate=caldate, tag1=tag1, start=start, end=end, type=type, channel=channel, M=M, F=F, npt=npt, age=age, gender=gender, \
                first=first, last=last, groups=groups, top=top, npd=npd ))

        client = pymongo.MongoClient(MIGO_MONGO_TA_URL, 27017)
        db = client['ET_' + shopid ]
        col = db['campaign']

        if not util_hadoop.PathIsExit(merge_path):
            ls = util_hadoop.GetPath(merge_path.rsplit("/",1)[0])
            lsdate = max(ls).rsplit("/",1)[1] #the laste hadoop ta date

            merge_path = "/user/athena/{}/meb_cube/{}/*".format(shopid, lsdate)
            all_date = col.find_one({'cid':cid})['member']
            lastday = max(sorted(all_date.items()))[0]
            prelsdate = all_date[lastday]['lsdate'] #laste usage day

            if lsdate != prelsdate:
                ta_parent = "/user/athena/{}/ta_done/{}".format(shopid, lsdate)
                ta_path = ta_parent + "/*"
            else:
                logger.add("clientid={clientid}, cid={cid}, lsdate = prelsdate it won't work".format(clientid=shopid, cid=cid))
                isblank = True
            adjust_interval_path = "/user/athena/{}/adjusted_interval/{}/*".format(shopid, lsdate)

        member_ls = []
        cam = col.find({'cid':cid})

        if not isblank:
            logger.add("clientid={clientid}, cid={cid}, task is running.................".format(clientid=shopid, cid=cid))
            sc = SparkContext(appName="[ET]Update_member_by_{shopid}_cid:{cid}".format(shopid=shopid, cid=cid))

            merge = sc.textFile(merge_path, 20).filter(lambda l: l.split("\t")[0] == shopid and l.split("\t")[2] == "L7D") \
            .map(lambda l: (l.split("\t")[0],l.split("\t")[1],l.split("\t")[3], l.split("\t")[4], l.split("\t")[5], l.split("\t")[6], l.split("\t")[7], l.split("\t")[8], l.split("\t")[9] \
            , l.split("\t")[10], l.split("\t")[11], l.split("\t")[12], l.split("\t")[13], l.split("\t")[14], l.split("\t")[15], random.random())).cache()





            ta = merge.filter(lambda l: merge1(l, tag1, M, F))
            logger.add('clientid={clientid}, cid={cid}, filter tag1, M, F done ....'.format(clientid=shopid, cid=cid))

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
                    npt_src1 = npt_s.sortBy(lambda x: "{}".format(float(x[5]) + float(x[15])), False, 30).zipWithIndex().filter(lambda xi: xi[1] < T).keys().cache()

                elif npt.count('-1') == 0:
                    npt_ls = npt.split(",")
                    npt_src1 = npt_s.filter(lambda l: l[6] in npt_ls).cache()

                if npd.count('-1') == 0:
                    startd, endd = npd.split(',')
                    npt_src = npt_src1.filter(lambda l: int(startd) <= int(l[7]) <= int(endd))
                else:
                    npt_src = npt_src1
                ta_pool = npt_src
            else:
                ta_pool = ta

            logger.add('clientid={clientid}, cid={cid}, ta and npt merge is finish....'.format(clientid=shopid, cid=cid))

            if gender.count('-1') > 0 and age.count('-1') > 0:
                p = ta_pool.map(lambda l: (l[1], (l[2], l[11], l[12], l[13]) ))
            else:
                if gender.count('-1') == 0 and age.count('-1') == 0:
                    p = ta_pool.filter(lambda l: int(age.split(',')[0]) <= int(l[10]) <= int(age.split(',')[1]) and l[9] == gender).map(lambda l: (l[1], (l[2], l[11], l[12], l[13]) ))
                elif gender.count('-1') == 0:
                    p = ta_pool.filter(lambda l: l[9] == gender).map(lambda l: (l[1], (l[2], l[11], l[12], l[13]) ))
                elif age.count('-1') == 0:
                    p = ta_pool.filter(lambda l: int(age.split(',')[0]) <= int(l[10]) <= int(age.split(',')[1])).map(lambda l: (l[1], (l[2], l[11], l[12], l[13]) ))
                else:
                    p = ta_pool.map(lambda l: (l[1], (l[2], l[11], l[12], l[13]) ))

            logger.add('clientid={clientid}, cid={cid}, member profile is finish....'.format(clientid=shopid, cid=cid))

            if first.count("-1") == 0 or last.count("-1") == 0:
                adj = sc.textFile(adjust_interval_path)
                adj_tmp = adj.filter(lambda line:adj_filter(line, first, last, shopid)).map(lambda x : (x.split("\t")[1],1))
                p = p.join(adj_tmp).map(lambda line: (line[0], line[1][0]))

            if groups != '-1':
                gpath = "/user/athena/{}/et_group/groups.txt".format(shopid)
                groupslist = sc.textFile(gpath).filter(lambda line: mafilter(line, groups, shopid) ).map(lambda line: line.split('\t')[3].encode('utf-8')).collect()
                logger.add("clientid={clientid}, cid={cid}, groups{groups} pool is {count}...........".format(clientid=shopid, cid=cid, groups=groups, count=len(groupslist)))
                member_ls = p.filter(lambda line:line[0].encode('utf-8') in groupslist).map(lambda x: channelmap(x)).collect()
            else:
                member_ls = p.map(lambda x: channelmap(x)).collect()
            sc.stop()
            logger.add("clientid={clientid}, cid={cid}, this time ta is {count}....................".format(clientid=shopid, cid=cid, count=len(member_ls)))

        #member = {'monitor':{'sms':{},'wechat':{},'edm':{}},'members':member_ls } ECR-27
        member = {'monitor':{'sms':{},'wechat':{},'edm':{}} }
        if member_ls:
            raw_count = len(member_ls)
        else:
            raw_count = 0

        CAMPAGIN = copy.deepcopy(CAMPAGIN1)
        if cam.count() == 1:
            doc = cam[0]
            if caldate in doc['member']:
                member = doc['member'][caldate]
                #member['members'] = member_ls ECR-27
                member['members'] = []
        
            if type.lower() == 'o':
                pass
            else:
                member['monitor'][channel] = {'start':start ,'end':end, 'members':[]}
                doc['member'][caldate] = member
                doc['member'][caldate]['lsdate'] = lsdate

            col.update({'cid':cid}, doc)
        else:
            pass
        client.close()
        r = []
        for T in member_ls:
            r.append(T['memberid'].encode('utf8'))
        print r
    except Exception as e:
        maillist = ['Wendell_Huang@migocorp.com', 'Fergus_Chen@migocorp.com', 'Michael_Chuang@migocorp.com', 'Christine_Huang@migocorp.com', 'James_Liang@migocorp.com', 'Duncan_Du@migocorp.com', 'Kency_Huang@migocorp.com']
        subject = 'ET Error Note'
        From = 'System Mail'
        To = 'RDS'
        contents = 'The shop {shopid} , campaignid {cid} is failed, parameter {p}\n Error Msg is "{err}"'.format(shopid=shopid, cid=cid, p=sys.argv, err=str(e))
        sendmail.sendmail(maillist, subject, From, To, contents)

        logger.add('clientid={clientid}, cid={cid}, Task Error...{err}'.format(clientid=shopid, cid=cid, err=str(e) ))
        print "#migoError#\t{}".format(str(e))


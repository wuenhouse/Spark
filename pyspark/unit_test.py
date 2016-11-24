#!/usr/bin/env python
#coding=UTF-8
#Objective: testing
#Author: Wendell
#Created Date: 2016.07.07

from pyspark import SparkContext

if __name__ == "__main__":

    '''
    Usage: python /data/migo/athena/lib/util/spark_pre_hazard_rate.py -s meishilin -d 20160508
    '''
    sc = SparkContext(appName="spark_test")
    a = [(1,2,3,4,5), (6,7,8,9,10)]
    rdd1 = sc.parallelize(a)
    print rdd1.collect()
    sc.stop()

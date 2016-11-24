#!/usr/bin/env python
#coding=UTF-8

import os, sys, getopt
import shutil
import itertools
import math

sys.path.append("/data/migo/pandora/lib")
sys.path.append("/data/migo/athena/lib")

from athena_variable import *

import pymongo
import datetime, time
import copy
import subprocess


def GetPath(path):
    ls = subprocess.Popen(["hadoop", "fs", "-ls", path], stdout=subprocess.PIPE)
    path = []
    for x in ls.stdout:
        if x.find("/") > 0:
            path.append("/{}".format(x.split("/",1)[1].strip()))

    return path

def PathIsExit(path):
    cmd = "hadoop fs -test -d {}".format(path)
    r = subprocess.call(cmd, shell=True)
    if r != 0:
        return False
    else:
        return True

def TEST():
    cmd = "cat /home/wendell/test2.dat | wc -l"
    r = os.popen(cmd).read()
    return r.strip()

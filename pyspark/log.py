#!/usr/bin/env python
#coding=UTF-8

import os, sys, getopt
import shutil
import itertools
import math
import pymongo
import datetime, time
import copy
import subprocess
from os import stat
from pwd import getpwuid

class log(object):

    def find_owner(self, filename):
        return getpwuid(stat(filename).st_uid).pw_name

    def __init__(self):
        #self.logtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.logdate = datetime.datetime.now().strftime('%Y%m%d')
        self.filename = "log.txt"
        self.path = "/tmp/log/et/{date}".format(date=self.logdate)
        self.file = "{path}/{filename}".format(path=self.path, date=self.logdate, filename=self.filename )

        if not os.path.exists(self.path):
            os.makedirs(self.path)
            cmd = "chown -R athena:athena /tmp/log/et"
            subprocess.call(cmd, shell=True)

    def add(self, line):
        if os.path.exists(self.file): 
            owner = self.find_owner(self.file)
            if owner != "athena":
                cmd = "chown -R athena:athena {}".format(self.file)
                subprocess.call(cmd, shell=True)

        logtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        fh2 = open(self.file, "a")
        line = "{logtime}::::{line}\n".format(logtime=logtime, line=line)
        fh2.write(line)
        fh2.close()

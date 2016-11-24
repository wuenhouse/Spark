#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *

import luigi

class MergeTag(MigoLuigiHive):
    '''
    Job Task:     Merge TA
    Objective:    TA Member
    Author:       Wendell Huang
    Created Date: 2016.05.24
    Source:       /user/athena/shopid/adjusted_interval/{date}
                  -> shopid, memberid, adj, first, last
                  /user/athena/shopid/hazard_hml/{date}
                  -> storeid, memberid, nes, hml, npt7, pr, order, nptdate, random
                  /user/athena/shopid/nes_tag_week/{date}
                  -> storeid, member_id, nes
    Destination:  /user/athena/shopid/et_merge/{date}
                  -> shop_id, member_id, nes, npt_hml, pr, nptdate, random, first, last
    Usage:        python et_ta_merge.py MergeTag --use-hadoop --keep-temp --src "/user/athena/meishilin/adjusted_interval/20160518;/user/athena/meishilin/hazard_hml/20160518;/user/athena/meishilin/nes_tag_week/20160518" --dest /user/athena/et_merge/20160518 --cal-date 20160518
    Attributes:
    '''
    sep = luigi.Parameter(default=";")

    def start(self):
        super(MergeTag, self).start()

        self.srcPool = self.src.split(self.sep)

    def requires(self):
        if self.use_hadoop:
            return [CleanerHDFS(filepath) for filepath in self.srcPool]
        else:
            return CleanerLocal(self.srcPool)

    def query(self):
        cmd = """
DROP TABLE adjusted_interval;
CREATE EXTERNAL TABLE adjusted_interval
(
shop_id STRING,
member_id STRING,
grp_mean FLOAT,
first STRING,
last STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "{adjpath}";

DROP TABLE hazard_hml;
CREATE EXTERNAL TABLE hazard_hml
(
shop_id STRING,
member_id STRING,
nes FLOAT,
hml FLOAT,
npt7 FLOAT,
pr FLOAT,
order FLOAT,
nptdate STRING,
ran FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "{hazardpath}";

DROP TABLE nes_tag_week;
create external table nes_tag_week
(
shop_id STRING,
member_id STRING,
NES_Tag STRING
)
row format delimited fields terminated by '\t'
location "{nespath}";

DROP TABLE et_merge;
create external table et_merge
(
shop_id STRING,
member_id STRING,
nes STRING,
hml STRING,
pr FLOAT,
nptdate STRING,
ran FLOAT,
first STRING,
last STRING
)
row format delimited fields terminated by '\t'
location "{mergepath}";

insert into table et_merge
select x.shop_id, x.member_id, x.NES_Tag, z.hml, z.pr, z.nptdate, z.ran, y.first, y.last
from nes_tag_week as x
left join adjusted_interval as y on x.shop_id = y.shop_id and x.member_id = y.member_id
left join hazard_hml as z on x.shop_id = z.shop_id and x.member_id = z.member_id
""".format(adjpath=self.srcPool[0], hazardpath=self.srcPool[1], nespath=self.srcPool[2], mergepath=self.dest)
        return cmd

if __name__ == "__main__": 
    luigi.run()

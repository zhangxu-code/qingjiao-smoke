#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-03-06
"""


import unittest
import csv
import ddt
import os
import sys
from loguru import logger
import numpy.testing as npt
sys.path.append(os.getcwd())
from dbimpala.dbengine import dbengine

def csvreader():
    logger.info("read sql csv")
    sqls = []
    csvfiles = os.getenv("sqlcsvfiles")
    csv.field_size_limit(1024 * 1024 * 10)
    logger.info(csvfiles)
    if csvfiles is None:
        return [False,False]
    for file in csvfiles.split(","):
        fr = open("./datainput/db/" + file)
        reader = csv.reader(fr)
        fieldnames = next(reader)
        csvfile = csv.DictReader(fr, fieldnames=fieldnames)
        for row in csvfile:
            sqls.append([row.get("title"), dict(row)])
        fr.close()
    logger.info(sqls)
    return sqls

@ddt.ddt
class DBEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:

        cls.db = dbengine(host=os.getenv("dbhost"), port=int(os.getenv("dbport")))
        cls.create_tabel_gold_case(cls)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.drop_table(cls)
        cls.db.__del__()

    @ddt.data(*csvreader())
    @ddt.unpack
    def test_sqlexec(self, title, datasql):
        """
        [ddt] sql
        :param title:
        :param data:
        :return:
        """
        logger.info(datasql)
        res = self.db.execut_sql(sql=datasql.get("sql"))
        logger.info(res)
        data = []
        for tmp in res:
            data.extend(tmp)
        logger.info(data)
        npt.assert_almost_equal(data, eval(datasql.get("expect")),
                                err_msg="校验result与预期不一致 res:%s expect:%s sql:%s" %
                                        (str(data), str(datasql.get("expect")), datasql.get("sql")))

    def create_tabel_gold_case(self):
        try:
            sql = "create table golda_int(day integer , time integer ,price integer ,c_code integer) TBLPROPERTIES('DS.dataset'='1:goldA_upload_int_100:goldA_upload_int_100')"
            # logger.info("create table "+sql)
            logger.info(self.db.execut_sql(sql))
            sql = "create table goldb_int(day integer , time integer ,price integer ,c_code integer) TBLPROPERTIES('DS.dataset'='1:goldb_upload_int_140:goldb_upload_int_140')"
            logger.info(self.db.execut_sql(sql))
        except Exception as err:
            logger.error(err)


    def drop_table(self):
        sql = "drop table golda_int"
        logger.info("drop table " + sql)
        # logger.info(self.db.execut_sql(sql))
        sql = "drop table goldb_int"
        logger.info("drop table " + sql)

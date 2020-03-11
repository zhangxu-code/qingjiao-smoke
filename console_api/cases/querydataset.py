#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-25
"""

import os
import sys
sys.path.append(os.getcwd())
import unittest
import json
import random
import jsonschema
import warnings
from loguru import logger
from tm.consoleapi import ConsoleAPI


class QeuryDataSet(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logger.info("setup")
        warnings.simplefilter("ignore", ResourceWarning)
        # cls.client = ConsoleAPI(site=conf.get("site"),
        #                        user=conf.get("user"), passwd=conf.get("passwd"))
        cls.client = ConsoleAPI(site=os.getenv("consolesite"),
                                user=os.getenv("consoleuser"), passwd=os.getenv("consolepasswd"))

    def check_schema(self, resp):
        """
        json schema check
        :param resp:
        :return:
        """
        freader = open('./console_api/schema/querydataset.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/querydataset.json')))
        freader.close()
        try:
            jsonschema.validate(resp, schema)
            return True
        except Exception as err:
            logger.error(err)
            return str(err)

    def get_dataserverid(self):
        response = self.client.query_ds()
        try:
            return response.get("data").get("data")[
                random.randint(1, len(response.get("data").get("data"))) - 1].get("id")
        except Exception as err:
            logger.error(err)
            return False

    def get_dataserverid_max(self):
        response = self.client.query_ds()
        try:
            maxid = 1
            for ds in response.get("data").get("data"):
                if ds.get("id") > maxid:
                    maxid = ds.get("id")
            return maxid
        except Exception as err:
            logger.error(err)
            return False

    def test_querydataset_default(self):
        """
        [poc] query dataset query=default
        :return:
        """
        dataserverId = self.get_dataserverid()
        if not dataserverId:
            self.assertTrue(False, msg="get dataserverId failed")
        response = self.client.query_dataset(query="dataServerId=%d" % dataserverId)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                             msg="check len(data) <= pageSize")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curPageNo = response.get("data").get("nextPageNo")
            query = "page=%d&dataServerId=%d" % (response.get("data").get("nextPageNo"), dataserverId)
            response = self.client.query_dataset(query=query)
            logger.info(response)
            if isinstance(self.check_schema(response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                                 msg="check len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageNo"), curPageNo, msg="pageNo = 上次调用的 nextPageNo")
        self.assertLessEqual(response.get("data").get("nextPageNo"), response.get("data").get("totalPages"),
                             msg="查询到最后一页，check nextPageNo <= totalPages")

    def test_querydataset_pagesize20(self):
        """
        [poc] query dataset query pagesize=20
        :return:
        """
        dataserverId = self.get_dataserverid()
        if not dataserverId:
            self.assertTrue(False, msg="get dataserverId failed")
        response = self.client.query_dataset(query="dataServerId=%d&pageSize=20" % dataserverId)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), 20,
                             msg="check len(data) <= pageSize")
        self.assertEqual(response.get("data").get("pageSize"), 20, msg="expect pageSize = 20")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curPageNo = response.get("data").get("nextPageNo")
            query = "page=%d&dataServerId=%d&pageSize=20" % (response.get("data").get("nextPageNo"), dataserverId)
            response = self.client.query_dataset(query=query)
            logger.info(response)
            if isinstance(self.check_schema(response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), 20,
                                 msg="check len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageSize"), 20, msg="expect pageSize = 20")
            self.assertEqual(response.get("data").get("pageNo"), curPageNo, msg="pageNo = 上次调用的 nextPageNo")
        self.assertLessEqual(response.get("data").get("nextPageNo"), response.get("data").get("totalPages"),
                             msg="查询到最后一页，check nextPageNo <= totalPages")

    def test_querydataset_wrongpage(self):
        """
        [exception] query dataset page=-1
        :return:
        """
        dataserverId = self.get_dataserverid()
        if not dataserverId:
            self.assertTrue(False, msg="get dataserverId failed")
        response = self.client.query_dataset(query="dataServerId=%d&page=-1" % dataserverId)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_querydataset_outpage(self):
        """
        [exception] query data page = totalpage  + 1
        :return:
        """
        dataserverId = self.get_dataserverid()
        if not dataserverId:
            self.assertTrue(False, msg="get dataserverId failed")
        response = self.client.query_dataset(query="dataServerId=%d" % dataserverId)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

        response = self.client.query_dataset(query="dataServerId=%d&page=%d" %
                                                   (dataserverId, response.get("data").get("totalPages")+1))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_querydataset_wrongdsid(self):
        """
        [exception] query dataset wrong dataServerId
        :return:
        """
        response = self.client.query_dataset(query="dataServerId=%d" % (99999 + 1))
        logger.info(response)
        self.assertEqual(len(response.get("data").get("data")), 0, msg="expect data = []")

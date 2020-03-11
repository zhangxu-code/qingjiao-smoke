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


class QueryMetaData(unittest.TestCase):
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
        freader = open('./console_api/schema/querymetadata.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/querymetadata.json')))
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

    def get_dataset(self, dataserverid):
        response = self.client.query_dataset(query="dataServerId=%d" % dataserverid)
        logger.info(response)
        try:
            return response.get("data").get("data")[
                random.randint(1, len(response.get("data").get("data"))) - 1].get("id")
        except Exception as err:
            logger.error(err)
            return False

    def test_querymetadata_default(self):
        """
        [poc] query metadata query=default
        :return:
        """
        dataserverId = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        response = self.client.query_metadata(query="dataServerId=%d&dataSetId=%d" % (dataserverId, datasetid))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                             msg="check len(data) <= pageSize")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curpage = response.get("data").get("nextPageNo")
            query = "page=%d&dataServerId=%d&dataSetId=%d" % (response.get("data").get("nextPageNo"), dataserverId, datasetid)
            response = self.client.query_metadata(query=query)
            logger.info(response)
            if isinstance(self.check_schema(response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                                 msg="check len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageNo"), curpage, msg="expect pageNo = 上次调用的 nextPageNo")
        self.assertLessEqual(response.get("data").get("nextPageNo"), response.get("data").get("totalPages"),
                             msg="查询到最后一页，check nextPageNo <= totalPages")

    def test_querymetadata_pagesize20(self):
        """
        [poc] query metadata query pagesize=20
        :return:
        """
        dataserverId = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        response = self.client.query_metadata(query="dataServerId=%d&dataSetId=%d&pageSize=20" % (
            dataserverId, datasetid))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), 20,
                             msg="check len(data) <= pageSize")
        self.assertEqual(response.get("data").get("pageSize"), 20, msg="expect pageSize = 20")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curpage = response.get("data").get("nextPageNo")
            query = "page=%d&dataServerId=%d&dataSetId=%d&pageSize=20" % (
                response.get("data").get("nextPageNo"), dataserverId,datasetid)
            response = self.client.query_metadata(query=query)
            logger.info(response)
            if isinstance(self.check_schema(response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), 20,
                                 msg="check len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageSize"), 20, msg="expect pageSize = 20")
            self.assertEqual(response.get("data").get("pageNo"), curpage, msg="expect pageNo = 上次调用的 nextPageNo")
        self.assertLessEqual(response.get("data").get("nextPageNo"), response.get("data").get("totalPages"),
                             msg="查询到最后一页，check nextPageNo <= totalPages")

    def test_querymetadata_wrongpage(self):
        """
        [exception] query metadata page=-1
        :return:
        """
        dataserverId = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        response = self.client.query_metadata(query="page=-1dataServerId=%d&dataSetId=%d" % (dataserverId, datasetid))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_querymetadata_pageoutmax(self):
        """
        [exception] query metadatq page = totalpage + 1
        :return:
        """
        dataserverId = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        response = self.client.query_metadata(query="dataServerId=%d&dataSetId=%d" % (dataserverId, datasetid))
        logger.info(response)
        response = self.client.query_metadata(query="page=%ddataServerId=%d&dataSetId=%d" % (
                    response.get("data").get("totalPages") + 1, dataserverId, datasetid))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_querymetadata_wrongds(self):
        """
        [exception] query metadata dataServerId not exist
        :return:
        """
        dataserverId = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        response = self.client.query_metadata(query="dataServerId=%d&dataSetId=%d" % (99999, datasetid))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_querymetadata_ds_dataset_notmatch(self):
        """
        [exception] query metadata dataServerId&dataset not match
        :return:
        """
        dataserverId = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        response = self.client.query_metadata(query="dataServerId=%d&dataSetId=%d" % (dataserverId+1, datasetid))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-25
"""

import os
import sys
import unittest
import json
import random
import jsonschema
import warnings
from loguru import logger
sys.path.append(os.getcwd())
from tm.consoleapi import ConsoleAPI


class QueryMetaIdByName(unittest.TestCase):
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
        freader = open('./console_api/schema/querymetaid.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/querymetaid.json')))
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
            randint = random.randint(1, len(response.get("data").get("data"))) - 1
            return response.get("data").get("data")[randint].get("id"), \
                   response.get("data").get("data")[randint].get("name")
        except Exception as err:
            logger.error(err)
            return False

    def get_dataset(self, dataserverid):
        response = self.client.query_dataset(query="dataServerId=%d" % dataserverid)
        logger.info(response)
        try:
            randint = random.randint(1, len(response.get("data").get("data"))) - 1
            return response.get("data").get("data")[randint].get("id"), \
                   response.get("data").get("data")[randint].get("name")
        except Exception as err:
            logger.error(err)
            return False

    def get_metadata_key(self, dataserverid, datasetid):
        response = self.client.query_metadata(query="dataServerId=%d&dataSetId=%d" % (dataserverid, datasetid))
        try:
            randint = random.randint(1, len(response.get("data").get("data"))) - 1
            return response.get("data").get("data")[randint].get("key")
        except Exception as err:
            logger.error(err)
            return False

    def test_getmetaid_byid(self):
        """
        [poc] get metaId by dataServerId & dataSetId & key
        :return:
        """
        dataserverId, dsname = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid, datasetname = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        key = self.get_metadata_key(dataserverid=dataserverId, datasetid=datasetid)
        if key is False:
            self.assertTrue(False, msg="get metadata key failed")

        response = self.client.query_metadata_byname(query="dataServerId=%d&dataSetId=%d&key=%s"
                                                           % (dataserverId, datasetid, key))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_getmetaid_byname(self):
        """
        [poc] get metaId by dsName & dataSetName & key
        :return:
        """
        dataserverId, dsname = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid, datasetname = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        key = self.get_metadata_key(dataserverid=dataserverId, datasetid=datasetid)
        if key is False:
            self.assertTrue(False, msg="get metadata key failed")

        response = self.client.query_metadata_byname(query="dsName=%s&dataSetName=%s&key=%s"
                                                           % (dsname, datasetname, key))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_getmetaid_byname_nomatch(self):
        """
        [all] get metaId by dsName & dataSetName & key not match
        :return:
        """
        dataserverId, dsname = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid, datasetname = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        key = self.get_metadata_key(dataserverid=dataserverId, datasetid=datasetid)
        if key is False:
            self.assertTrue(False, msg="get metadata key failed")

        response = self.client.query_metadata_byname(query="dsName=%sh&dataSetName=%s&key=%s"
                                                           % (dsname, datasetname, key))
        logger.info(response)

        response = self.client.query_metadata_byname(query="dsName=%snomatch&dataSetName=%s&key=%s"
                                                           % (dsname, datasetname, key))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_getmetaid_byid_nomatch(self):
        """
        [all] get metaId by dataServerId & dataSetId & key
        :return:
        """
        dataserverId, dsname = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid, datasetname = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        key = self.get_metadata_key(dataserverid=dataserverId, datasetid=datasetid)
        if key is False:
            self.assertTrue(False, msg="get metadata key failed")
        response = self.client.query_metadata_byname(query="dataServerId=%d33&dataSetId=%d&key=%s"
                                                           % (dataserverId, datasetid, key))
        logger.info(response)
        if response.get("data"):
            self.assertTrue(False, msg="expect data = None")
        #if isinstance(self.check_schema(resp=response), str):
        #    self.assertTrue(False, "jsonschema check failed")



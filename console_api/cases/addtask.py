#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-26
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
from console_api.cases.util import util

class addTask(unittest.TestCase):
    code = """
import privpy as pp
data = pp.ss("data")
pp.reveal(data, "result")
"""
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
        freader = open('./console_api/schema/addtask.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/addtask.json')))
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
            return response.get("data").get("data")[randint].get("key"), \
                   response.get("data").get("data")[randint].get("metaId")
        except Exception as err:
            logger.error(err)
            return False

    def test_addtask_ok(self):
        """
        [poc]  add task
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }],
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")

    def test_addtask_wrong_varname(self):
        """
        [poc] add task wrong varName = data@
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data@"
            }],
            "taskResultVOList": [{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "FORMAT_ERROR", msg="expect subCode = Null")

    def test_addtask_wrong_varname1(self):
        """
        [poc] add task wrong varName = data data
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data data"
            }],
            "taskResultVOList": [{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "FORMAT_ERROR", msg="expect subCode = Null")

    def test_addtask_wrong_varname2(self):
        """
        [poc] add task wrong varName = data&data
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data&data"
            }],
            "taskResultVOList": [{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "FORMAT_ERROR", msg="expect subCode = Null")

    def test_addtask_wrong_resultvarname(self):
        """
        [poc] add task wrong resultvarName = result&
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_data"
            }],
            "taskResultVOList": [{
                "resultDest": dataserverId,
                "resultVarName": "result&"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "FORMAT_ERROR", msg="expect subCode = Null")

    def test_addtask_wrong_resultvarname1(self):
        """
        [poc] add task wrong resultvarName = result result
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_data"
            }],
            "taskResultVOList": [{
                "resultDest": dataserverId,
                "resultVarName": "result result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "FORMAT_ERROR", msg="expect subCode = Null")

    def test_addtask_nocode(self):
        """
        [exception]  add task key code not exist
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            #"code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }],
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")

    def test_addtask_code_none(self):
        """
        [exception]  add task key code = none
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": None,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }],
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")

    def test_addtask_noname(self):
        """
        [exception]  add task key name not exist
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            #"name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }],
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")

    def test_addtask_name_none(self):
        """
        [exception]  add task key name = none
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": None,
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }],
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")

    def test_addtask_result_none(self):
        """
        [exception]  add task key taskResultVOList = none
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }],
            "taskResultVOList": None
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")

    def test_addtask_noresult(self):
        """
        [exception]  add task key taskResultVOList not exist
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data_"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")

    def test_addtask_nodatasource(self):
        """
        [exception]  add task key taskDataSourceVOList not exist
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        #self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")

    def test_addtask_datasource_none(self):
        """
        [exception]  add task key taskDataSourceVOList = none
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
        jsbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": None,
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(jsbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        #self.assertEqual(response.get("subCode"), "PARAM_GLOBAL0005", msg="expect subCode = Null")


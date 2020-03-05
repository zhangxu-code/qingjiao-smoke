#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-27
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

class ModifyTask(unittest.TestCase):
    code = """
import privpy as pp
data = pp.ss("data")
pp.reveal(data, "result")
"""
    taskbody = None
    taskid = None
    @classmethod
    def setUpClass(cls) -> None:
        logger.info("setup")
        warnings.simplefilter("ignore", ResourceWarning)
        # cls.client = ConsoleAPI(site=conf.get("site"),
        #                        user=conf.get("user"), passwd=conf.get("passwd"))
        cls.client = ConsoleAPI(site=os.getenv("consolesite"),
                                user=os.getenv("consoleuser"), passwd=os.getenv("consolepasswd"))
        cls.addtask(cls)

    def check_schema(self, resp):
        """
        json schema check
        :param resp:
        :return:
        """
        freader = open('./console_api/schema/modifytask.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/modifytask.json')))
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

    def addtask(self):
        """
        [poc]  add task
        :return:
        """
        dataserverId, dsname = self.get_dataserverid(self)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        datasetid, datasetname = self.get_dataset(self, dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        key, metaid = self.get_metadata_key(self, dataserverid=dataserverId, datasetid=datasetid)
        if key is False:
            self.assertTrue(False, msg="get metadata key failed")
        self.taskbody = {
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
        response = self.client.add_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        #if isinstance(self.check_schema(self, resp=response), str):
        #    self.assertTrue(False, "jsonschema check failed")
        self.taskid = response.get("data").get("id")

    def test_modify_task_name(self):
        """
        [poc] modify task change name
        :return:
        """
        lastname = self.taskbody["name"]
        self.taskbody["name"] = "newname"
        self.taskbody["id"] = self.taskid
        response  =self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")
        response = self.client.get_task(taskid=self.taskid)
        logger.info(response)
        self.assertEqual(response.get("data").get("name"), "newname",
                         msg="expect name = newname")
        self.taskbody["name"] = lastname

    def test_modify_task_code(self):
        """
        [poc] modify task change name
        :return:
        """
        latestcode = self.taskbody["code"]
        self.taskbody["code"] = "import os\n" + latestcode
        self.taskbody["id"] = self.taskid
        response = self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")
        response = self.client.get_task(taskid=self.taskid)
        logger.info(response)
        self.assertEqual(response.get("data").get("code"), "import os\n" + latestcode,
                         msg="expect code = %s" % ("import os\n" + latestcode))
        self.taskbody["code"] = latestcode

    def test_modify_task_datasouce(self):
        """
        [poc] modify task change name
        :return:
        """
        latestdatasource = self.taskbody["taskDataSourceVOList"]
        self.taskbody["taskDataSourceVOList"] = None
        self.taskbody["id"] = self.taskid
        response = self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")
        response = self.client.get_task(taskid=self.taskid)
        logger.info(response)
        self.assertEqual(response.get("data").get("taskDataSourceVOList"), None,
                         msg="expect taskDataSourceVOList = None")
        self.taskbody["taskDataSourceVOList"] = latestdatasource

    def test_modify_task_result(self):
        """
        [poc] modify task change name
        :return:
        """
        latestdatasource = self.taskbody["taskResultVOList"]
        self.taskbody["taskResultVOList"] = None
        self.taskbody["id"] = self.taskid
        response = self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")
        response = self.client.get_task(taskid=self.taskid)
        logger.info(response)
        self.assertEqual(response.get("data").get("taskResultVOList"), None,
                         msg="expect taskResultVOList = None")
        self.taskbody["taskResultVOList"] = latestdatasource

    def test_modify_task_noname(self):
        """
        [all] modify task change name
        :return:
        """
        lastname = self.taskbody["name"]
        self.taskbody.pop("name")
        self.taskbody["id"] = self.taskid
        logger.info(self.taskbody)
        response = self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        #if isinstance(self.check_schema(resp=response), str):
        #    self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="expect code = 1")
        self.assertIsInstance(response.get("subCode"), str, msg="expect subCode = Str")
        #self.assertEqual(response.get("subCode"), str, msg="expect subCode = Null")

        self.taskbody["name"] = lastname

    def test_modify_task_nocode(self):
        """
        [all] modify task change name
        :return:
        """
        lastcode = self.taskbody["code"]
        self.taskbody.pop("code")
        self.taskbody["id"] = self.taskid
        logger.info(self.taskbody)
        response = self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        #if isinstance(self.check_schema(resp=response), str):
        #    self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="expect code = 1")
        self.assertIsInstance(response.get("subCode"), str, msg="expect subCode = Str")
        #self.assertEqual(response.get("subCode"), str, msg="expect subCode = Null")
        self.taskbody["code"] = lastcode

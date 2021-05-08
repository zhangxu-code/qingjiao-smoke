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
from console_api.cases.util import util

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

    def addtask(self):
        """
        [test]  add task
        :return:
        """
        dataserverId, dsname = util.getdsid(client=self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(client=self.client)
        self.taskbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data"
            }],
            "taskResultVOList":[{
                "resultDest": dataserverId,
                "resultVarName": "result"
            }]
        }
        response = self.client.add_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
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
        self.assertEqual(response.get("data").get("taskDataSourceVOList"), [],
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
        self.assertEqual(response.get("data").get("taskResultVOList"), [],
                         msg="expect taskResultVOList = None")
        self.taskbody["taskResultVOList"] = latestdatasource

    def test_modify_task_noname(self):
        """
        [exception] modify task change name
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
        [exception] modify task change name
        :return:
        """
        lastcode = self.taskbody["code"]
        self.taskbody.pop("code")
        self.taskbody["id"] = self.taskid
        logger.info(self.taskbody)
        response = self.client.modify_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 1")
        self.assertIsInstance(response.get("subCode"), str, msg="expect subCode = Str")
        self.taskbody["code"] = lastcode

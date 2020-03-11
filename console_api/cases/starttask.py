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
import time
import json
import random
import jsonschema
import warnings
from loguru import logger
sys.path.append(os.getcwd())
from tm.consoleapi import ConsoleAPI
from  console_api.cases.util import util


class StartTask(unittest.TestCase):
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
        #cls.addtask(cls)

    def check_schema(self, resp):
        """
        json schema check
        :param resp:
        :return:
        """
        freader = open('./console_api/schema/starttask.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/starttask.json')))
        freader.close()
        try:
            jsonschema.validate(resp, schema)
            return True
        except Exception as err:
            logger.error(err)
            return str(err)

    def addtask(self, data=None):
        """
        [poc]  add task
        :return:
        """
        dataserverId, dsname = util.getdsid(self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metadata key failed")
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
        if data:
            self.taskbody["taskDataSourceVOList"][0]["varName"] = data
        response = self.client.add_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        return response.get("data").get("id")

    def test_starttask_notrun(self):
        """
        [poc] start task 未执行任务
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")

    def test_starttask_finished(self):
        """
        [poc] start task 完成任务
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")
        response = self.client.get_task(taskid=taskid)
        while response.get("data").get("queueStatus") < 6:
            response = self.client.get_task(taskid=taskid)
            logger.info(response)
            time.sleep(10)
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")

    def test_starttask_failed(self):
        """
        [poc] start task 失败任务
        :return:
        """
        taskid = self.addtask(data="data_")
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")
        response = self.client.get_task(taskid=taskid)
        while response.get("data").get("queueStatus") < 6:
            response = self.client.get_task(taskid=taskid)
            time.sleep(10)
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), None, msg="expect subCode = Null")

    def test_starttask_notexist(self):
        """
        [poc] start task 不存在任务
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid+9999)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="expect subCode = TM_TASK_NOT_EXIST")

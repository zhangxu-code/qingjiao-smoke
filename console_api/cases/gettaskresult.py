#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-03-02
"""


import os
import sys
import unittest
import json
import random
import jsonschema
import warnings
import yaml
from loguru import logger
sys.path.append(os.getcwd())
from tm.consoleapi import ConsoleAPI
from console_api.cases.util import util

class GetTaskResult(unittest.TestCase):
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
        cls.client = ConsoleAPI(site=os.getenv("consolesite"),
                                user=os.getenv("consoleuser"), passwd=os.getenv("consolepasswd"))

    def check_schema(self, resp):
        """
        json schema check
        :param resp:
        :return:
        """
        freader = open('./console_api/schema/gettaskresult.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/gettaskresult.json')))
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
        dataserverId, dsid = util.getdsid(self.client)
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        metaid = util.getmetaId(self.client)
        if metaid is False:
            self.assertTrue(False, msg="get metaid failed")
        self.taskbody = {
            "code": self.code,
            "name": "test",
            "taskDataSourceVOList": [{
                "metaId": metaid,
                "varName": "data"
            }],
            "taskResultVOList": [{
                "resultDest": dsid,
                "resultVarName": "result"
            }]
        }
        if data:
            self.taskbody["taskDataSourceVOList"]["varName"] = data
        response = self.client.add_task(job_data=json.dumps(self.taskbody))
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        return response.get("data").get("id")

    def checktaskstatus(self, taskid, expectstatus):
        logger.info("check task status")
        response = self.client.get_task(taskid=taskid)
        while response.get("data").get("queueStatus") < expectstatus:
            response = self.client.get_task(taskid=taskid)

    def test_gettaskresult_finished(self):
        """
        [none] get task result status = finished
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="start task expect code = 0")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        response = self.client.get_task_result(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

    def test_gettaskresult_running(self):
        """
        [none] get task result status = running
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="start task expect code = 0")
        response = self.client.get_task_result(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="expect code = 1")
        self.checktaskstatus(taskid=taskid, expectstatus=6)

    def test_gettaskresult_failed(self):
        """
        [none]
        :return:
        """

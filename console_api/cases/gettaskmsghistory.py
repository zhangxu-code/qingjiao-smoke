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
import time
from loguru import logger
sys.path.append(os.getcwd())
from tm.consoleapi import ConsoleAPI
from console_api.cases.util import util

class GetExecMsgHistory(unittest.TestCase):
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
        freader = open('./console_api/schema/gethistorymsg.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/gethistorymsg.json')))
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
        response  = self.client.get_task(taskid=taskid)
        while response.get("data").get("queueStatus") < expectstatus:
            response = self.client.get_task(taskid=taskid)

    def test_gethistorymsg_running(self):
        """
        [poc] get history execMsg task running
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        time.sleep(10)
        response = self.client.get_history_msg(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")

    def test_gethistorymsg_finished(self):
        """
        [poc] get history execMsg task finished
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        response = self.client.get_history_msg(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")

    def test_gethistorymsg_waiited(self):
        """
        [exception] get history execMsg task waited
        :return:
        """
        taskid = self.addtask()
        response = self.client.get_history_msg(taskid=taskid)
        logger.info(response)
        #if isinstance(self.check_schema(resp=response), str):
        #    self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertEqual(response.get("data"), [], msg="expect data = []")

    def test_gethistorymsg_tasknotexist(self):
        """
        [exception] get history execMsg task not exist
        :return:
        """
        taskid = self.addtask()
        response = self.client.get_history_msg(taskid=taskid + 9999)
        logger.info(response)
        #if isinstance(self.check_schema(resp=response), str):
        #    self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="expect code = 1")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="expect subCode = TM_TASK_NOT_EXIST")

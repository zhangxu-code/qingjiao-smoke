#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-03-03
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


class GetTaskLogByID(unittest.TestCase):
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
        freader = open('./console_api/schema/getrolelogbytaskid.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/getrolelogbytaskid.json')))
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

    def test_getlog_wait(self):
        """
        [poc] get task log by taskid ,task = wait
        :return:
        """
        taskid = self.addtask()
        response = self.client.get_role_log_bytaskid(query="role=tm&taskId=%d" % taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 0")
        self.assertEqual(response.get("subCode"), "TRACK_LOGS_NOT_EXIST", msg="expect data = []")

    def test_getlog_running(self):
        """
        [poc] get task log by taskid ,task = running
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.checktaskstatus(taskid=taskid, expectstatus=4)
        response = self.client.get_role_log_bytaskid(query="role=tm&taskId=%d" % taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.assertNotEqual(response.get("data"), [], msg="expect data = []")

    def test_getlog_finished(self):
        """
        [poc] get task log by taskid ,task = finished
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        response = self.client.get_task_roles(query="taskId=%d" % taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        for role in response.get("data"):
            response = self.client.get_role_log_bytaskid(query="role=%s&taskId=%d" % (role, taskid))
            logger.info(response)
            if isinstance(self.check_schema(resp=response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertEqual(response.get("code"), 0, msg="expect code = 0")
            self.assertNotEqual(response.get("data"), [], msg="expect data = []")

    def test_getlog_taskid_notexist(self):
        """
        [exception] get task log by taskid ,taskid not exist
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        response = self.client.get_task_roles(query="taskId=%d" % taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        for role in response.get("data"):
            response = self.client.get_role_log_bytaskid(query="role=%s&taskId=%d" % (role, taskid + 9999))
            logger.info(response)
            self.assertEqual(response.get("code"), 1, msg="expect code = 1")
            self.assertEqual(response.get("subCode"), "TRACK_LOGS_NOT_EXIST", msg="expect subCode = TM_TASK_NOT_EXIST")

    def test_getlog_role_noexist(self):
        """
        [exception] get task log by taskid ,role not exist
        :return:
        """
        taskid = self.addtask()
        response = self.client.get_role_log_bytaskid(query="role=hahi&taskId=%d" % taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code = 1")

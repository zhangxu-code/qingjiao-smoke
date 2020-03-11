#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-03-01
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


class StopTask(unittest.TestCase):
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
        freader = open('./console_api/schema/stoptask.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/stoptask.json')))
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

    def test_stoptask_idnoexist(self):
        """
        [exception] stop task by taskid = not exist
        :return:
        """
        taskid = self.addtask()
        stopbody = {
            "taskId": taskid + 9999
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")

        self.assertEqual(response.get("code"), 1, msg="stop task expect code = 1")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="stop task expect subCode=TM_TASK_NOT_EXIST")

    def test_stoptask_requestid_notexist(self):
        """
        [exception] stop task by taskid = not exist
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        stopbody = {
            "requestId": "hahahaa" + requestid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="stop task expect code = 1")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="stop task expect subCode=TM_TASK_NOT_EXIST")
        self.checktaskstatus(taskid=taskid, expectstatus=6)

    def test_stoptask_byrequestid_running(self):
        """
        [poc] stop task by requestId
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        stopbody = {
            "requestId": requestid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="stop task expect code = 0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_stoptask_byid_running(self):
        """
        [poc] stop task by id
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        stopbody = {
            "taskId": taskid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="stop task expect code = 0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_stoptask_byid_finished(self):
        """
        [poc] stop task by id
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        stopbody = {
            "taskId": taskid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="stop task expect code = 0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_stoptask_byrequestid_finished(self):
        """
        [poc] stop task by requestId
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        stopbody = {
            "requestId": requestid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="stop task expect code = 0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_stoptask_byid_requestid(self):
        """
        [poc] stop task by requstId & taskid
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        stopbody = {
            "requestId": requestid,
            "taskId": taskid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="stop task expect code = 0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_stoptask_byid_requestid_notmatch(self):
        """
        [exception] stop task by requstId & taskid not match
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        stopbody = {
            "requestId": requestid,
            "taskId": taskid - 1
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="stop task expect code = 0")
        self.assertIsInstance(response.get("subCode"), str, msg="expect subCode = None")

    def test_stoptask_byid_requestid_notmatch(self):
        """
        [exception] stop task by requstId & taskid not match
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect start job success")
        requestid = response.get("data")
        stopbody = {
            "requestId": "hahaha" + requestid,
            "taskId": taskid
        }
        response = self.client.stop_task(data=json.dumps(stopbody))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 1, msg="stop task expect code = 0")
        self.assertIsInstance(response.get("subCode"), str, msg="expect subCode = TM_TASK_NOT_EXIST")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="expect subCode = TM_TASK_NOT_EXIST")

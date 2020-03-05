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


class GetTaskInfo(unittest.TestCase):
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
        freader = open('./console_api/schema/gettaskinfo.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/gettaskinfo.json')))
        freader.close()
        try:
            jsonschema.validate(resp, schema)
            return True
        except Exception as err:
            logger.error(err)
            return str(err)

    def get_dataserverid(self):
        response = self.client.query_ds()
        logger.info(response)
        try:
            randint = random.randint(1, len(response.get("data").get("data"))) - 1
            return response.get("data").get("data")[randint].get("id"), \
                   response.get("data").get("data")[randint].get("dsId")
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
        logger.info(response)
        try:
            randint = random.randint(1, len(response.get("data").get("data"))) - 1
            return response.get("data").get("data")[randint].get("key"), \
                   response.get("data").get("data")[randint].get("metaId")
        except Exception as err:
            logger.error(err)
            return False

    def get_metaid(self):
        fr = open("./console_api/conf.yaml")
        conf = yaml.load(fr)
        fr.close()
        try:
            response = self.client.query_metadata_byname(query="dsName=%s&dataSetName=%s&key=%s" % (
                conf.get("dataServer"), conf.get("dataSet"), conf.get("key")))
            logger.info(response)
            self.assertEqual(response.get("code"), 0, msg="expect code = 0")
            return response.get("data").get("metaId")
        except Exception as err:
            logger.error(err)
            return False

    def addtask(self, data=None):
        """
        [poc]  add task
        :return:
        """

        dataserverId, dsid = self.get_dataserverid()
        if dataserverId is False:
            self.assertTrue(False, msg="get dataserverId failed")
        '''
        datasetid, datasetname = self.get_dataset(dataserverId)
        if datasetid is False:
            self.assertTrue(False, msg="get dataset failed")
        key, metaid = self.get_metadata_key(dataserverid=dataserverId, datasetid=datasetid)
        if key is False:
            self.assertTrue(False, msg="get metadata key failed")
        '''
        metaid = self.get_metaid()
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

    def test_gettask_wait(self):
        """
        [poc] get taskinfo status = wait
        :return:
        """
        taskid = self.addtask()
        response = self.client.get_task(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code=0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_gettask_running(self):
        """
        [poc] get taskinfo status = running
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        response = self.client.get_task(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code=0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")
        self.checktaskstatus(taskid=taskid, expectstatus=6)

    def test_gettask_finished(self):
        """
        [poc] get taskinfo status = running
        :return:
        """
        taskid = self.addtask()
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="expect code = 0")
        self.checktaskstatus(taskid=taskid, expectstatus=6)
        response = self.client.get_task(taskid=taskid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("code"), 0, msg="expect code=0")
        self.assertIsNone(response.get("subCode"), msg="expect subCode = None")

    def test_gettask_notexist(self):
        """
        [exception] get taskinfo taskid not exist
        :return:
        """
        taskid = self.addtask()
        response = self.client.get_task(taskid=taskid+9999)
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code=0")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="expect subCode = None")

    def test_gettask_id0(self):
        """
        [exception] get taskinfo taskid = 0
        :return:
        """
        response = self.client.get_task(taskid=0)
        logger.info(response)
        self.assertEqual(response.get("code"), 1, msg="expect code=0")
        self.assertEqual(response.get("subCode"), "TM_TASK_NOT_EXIST", msg="expect subCode = None")

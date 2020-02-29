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

class queryTask(unittest.TestCase):
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
        freader = open('./console_api/schema/querytask.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/querytask.json')))
        freader.close()
        try:
            jsonschema.validate(resp, schema)
            return True
        except Exception as err:
            logger.error(err)
            return str(err)

    def test_querytask_default(self):
        """
        [poc] query task pageSize=default
        :return:
        """

        logger.info("query task pageSize = default")
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                             msg="expect len(data) <= pageSize")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curpage = response.get("data").get("nextPageNo")
            response = self.client.query_task(query="page=%d" % curpage)
            if isinstance(self.check_schema(resp=response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                                 msg="expect len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageNo"), curpage, msg="预期当前pageNo = 上次调用的nextPageNo")
        self.assertEqual(response.get("data").get("pageNo"), response.get("data").get("totalPages"), msg="预期当前pageNo =  totlaPages")

    def test_querytask_pagesize20(self):
        """
        [poc] query task pageSize=20
        :return:
        """

        logger.info("query task pageSize = default")
        response = self.client.query_task(query="pageSize=20")
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), 20,
                             msg="expect len(data) <= pageSize")
        self.assertEqual(response.get("data").get("pageSize"), 20, msg="expect pageSize = 20")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curpage = response.get("data").get("nextPageNo")
            response = self.client.query_task(query="page=%d&pageSize=20" % curpage)
            if isinstance(self.check_schema(resp=response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertEqual(response.get("data").get("pageSize"), 20, msg="expect pageSize = 20")
            self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                                 msg="expect len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageNo"), curpage, msg="预期当前pageNo = 上次调用的nextPageNo")
        self.assertEqual(response.get("data").get("pageNo"), response.get("data").get("totalPages"), msg="预期当前pageNo =  totlaPages")

    def test_querytask_pagesize50(self):
        """
        [poc] query task pageSize=50
        :return:
        """

        logger.info("query task pageSize = default")
        response = self.client.query_task(query="pageSize=50")
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), 50,
                             msg="expect len(data) <= pageSize")
        self.assertEqual(response.get("data").get("pageSize"), 50, msg="expect pageSize = 50")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            curpage = response.get("data").get("nextPageNo")
            response = self.client.query_task(query="page=%d&pageSize=50" % curpage)
            if isinstance(self.check_schema(resp=response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertEqual(response.get("data").get("pageSize"), 50, msg="expect pageSize = 50")
            self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                                 msg="expect len(data) <= pageSize")
            self.assertEqual(response.get("data").get("pageNo"), curpage, msg="预期当前pageNo = 上次调用的nextPageNo")
        self.assertEqual(response.get("data").get("pageNo"), response.get("data").get("totalPages"),
                         msg="预期当前pageNo =  totlaPages")

    def test_querytask_byid(self):
        """
        [poc] querytask by taskid
        :return:
        """
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        id = response.get("data").get("data")[0]["id"]
        response = self.client.query_task(query="id=%d" % id)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("totalRows"), 1, msg="expect total rows = 1")
        self.assertEqual(response.get("data").get("data")[0].get("id"), id, msg="epxect id=%d" % id)

    def test_querytask_byrequestid(self):
        """
        [poc] querytask by requestid
        :return:
        """
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        requestid = response.get("data").get("data")[0]["requestId"]
        response = self.client.query_task(query="requestId=%s" % requestid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("totalRows"), 1, msg="expect total rows = 1")
        self.assertEqual(response.get("data").get("data")[0].get("requestId"), requestid, msg="epxect requestId=%s" % requestid)

    def test_querytask_byname(self):
        """
        [poc] querytask by name
        :return:
        """
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        name = response.get("data").get("data")[0]["name"]
        response = self.client.query_task(query="name=%s" % name)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("totalRows"), 1, msg="expect total rows = 1")
        self.assertEqual(response.get("data").get("data")[0].get("name"), name,
                         msg="epxect name=%s" % name)

    def test_querytask_byid_noexist(self):
        """
        [all] querytask by taskid not exist
        :return:
        """
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        id = response.get("data").get("data")[0]["id"]
        response = self.client.query_task(query="id=99%d" % id)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("totalRows"), 0, msg="expect total rows = 0")

    def test_querytask_byrequestid_notexist(self):
        """
        [all] querytask by requestid not exist
        :return:
        """
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        requestid = response.get("data").get("data")[0]["requestId"]
        response = self.client.query_task(query="requestId=hahahaha%s" % requestid)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("totalRows"), 0, msg="expect total rows = 0")

    def test_querytask_byname_notexist(self):
        """
        [all] querytask by name not exist
        :return:
        """
        response = self.client.query_task()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        name = response.get("data").get("data")[0]["name"]
        response = self.client.query_task(query="name=hihihihi%s" % name)
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("totalRows"), 0, msg="expect total rows = 0")


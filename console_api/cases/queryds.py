#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketestbak
Author  zhenghongguang
Date    2020-02-20
"""

import os
import sys
sys.path.append(os.getcwd())
import unittest
import json
import jsonschema
import warnings
from loguru import logger
from tm.consoleapi import ConsoleAPI


class QueryDS(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        logger.info("setup")
        warnings.simplefilter("ignore", ResourceWarning)
        #cls.client = ConsoleAPI(site=conf.get("site"),
        #                        user=conf.get("user"), passwd=conf.get("passwd"))
        cls.client = ConsoleAPI(site=os.getenv("consolesite"),
                                user=os.getenv("consoleuser"), passwd=os.getenv("consolepasswd"))

    def check_schema(self, resp):
        """
        json schema check
        :param resp:
        :return:
        """
        freader = open('./console_api/schema/queryds.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/queryds.json')))
        freader.close()
        try:
            jsonschema.validate(resp, schema)
            return True
        except Exception as err:
            logger.error(err)
            return str(err)

    def test_queryds_default(self):
        """
        [poc] queryds pagesize==default
        :return:
        """
        logger.info("query ds default")
        response = self.client.query_ds(query=None)
        logger.info(response)
        if isinstance(self.check_schema(response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                             msg="check len(data) <= pageSize")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            query = "page=%d" % response.get("data").get("nextPageNo")
            response = self.client.query_ds(query=query)
            logger.info(response)
            if isinstance(self.check_schema(response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), response.get("data").get("pageSize"),
                                 msg="check len(data) <= pageSize")
        self.assertLessEqual(response.get("data").get("nextPageNo"), response.get("data").get("totalPages"),
                             msg="查询到最后一页，check nextPageNo <= totalPages")

    def test_queryds_pagesize20(self):
        """
        [poc] queryds pagesize=20
        :return:
        """
        logger.info("query ds pagesize 20")
        response = self.client.query_ds(query="pageSize=20")
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        self.assertEqual(response.get("data").get("pageSize"), 20,msg="expect pageSize = 20")
        self.assertLessEqual(len(response.get("data").get("data")), 20,
                             msg="check len(data) <= pageSize")
        while response.get("data").get("pageNo") < response.get("data").get("totalPages"):
            query = "page=%d&pageSize=20" % response.get("data").get("nextPageNo")
            response = self.client.query_ds(query=query)
            logger.info(response)
            if isinstance(self.check_schema(response), str):
                self.assertTrue(False, "jsonschema check failed")
            self.assertLessEqual(len(response.get("data").get("data")), 20,
                                 msg="check len(data) <= pageSize")
            self.assertLessEqual(len(response.get("data").get("data")), 20,
                                 msg="check len(data) <= pageSize")
        self.assertLessEqual(response.get("data").get("nextPageNo"), response.get("data").get("totalPages"),
                         msg="查询到最后一页，check nextPageNo <= totalPages")

    def test_queryds_page_wrong(self):
        """
        [all] queryds page = -1
        :return:
        """
        logger.info("query ds page = -1")
        response = self.client.query_ds(query="page=-1")
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")


    def test_queryds_page_outmax(self):
        """
        [all] queryds page = totalpage + 1
        :return:
        """
        logger.info("query ds page = totalpage + 1")
        response = self.client.query_ds()
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")
        response = self.client.query_ds(query="page=%d" % (response.get("data").get("totalPages") + 1))
        logger.info(response)
        if isinstance(self.check_schema(resp=response), str):
            self.assertTrue(False, "jsonschema check failed")



if __name__ == '__main__':
    #os.environ["consolesite"] = "console-dev.tsingj.local"
    #os.environ["consoleuser"] = "smokelibrary"
    #os.environ["consolepasswd"] = "1234qwer"
    unittest.main()

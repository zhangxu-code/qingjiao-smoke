#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-29
"""

import unittest
import os
import sys
import json
import csv
import time
from loguru import logger
import jsonschema
import warnings
sys.path.append(os.getcwd())
import ddt
from tm.consoleapi import ConsoleAPI


def jobcsv():
    logger.info("get job.csv")
    jobs = []
    csvfiles = os.getenv("csvfiles")
    csv.field_size_limit(1024 * 1024 * 10)
    logger.info(csvfiles)
    for file in csvfiles.split(","):
        fr = open("./datainput/tm/"+file)
        reader = csv.reader(fr)
        fieldnames = next(reader)
        csvfile = csv.DictReader(fr, fieldnames=fieldnames)
        #next(csvfile)
        for row in csvfile:
            logger.info(dict(row))
            jobs.append([row.get("title"), dict(row)])
        fr.close()
    logger.info(jobs)
    return jobs


@ddt.ddt
class job(unittest.TestCase):
    namespace = None
    @classmethod
    def setUpClass(cls) -> None:
        logger.info("setup")
        warnings.simplefilter("ignore", ResourceWarning)
        # cls.client = ConsoleAPI(site=conf.get("site"),
        #                        user=conf.get("user"), passwd=conf.get("passwd"))
        cls.client = ConsoleAPI(site=os.getenv("consolesite"),
                                user=os.getenv("consoleuser"), passwd=os.getenv("consolepasswd"))
        cls.namespace = os.getenv("namespace")

    def check_schema(self, resp):
        """
        json schema check
        :param resp:
        :return:
        """
        freader = open('./console_api/schema/addtask.json')
        schema = json.loads(
            freader.read(os.path.getsize('./console_api/schema/addtask.json')))
        freader.close()
        try:
            jsonschema.validate(resp, schema)
            return True
        except Exception as err:
            logger.error(err)
            return str(err)

    def getmetaid(self, datasource):
        logger.info(datasource)
        datasource_metaid = []
        try:
            for data in datasource:
                tmp = {}
                tmp["varName"] = data.get("varName")
                if "${namespace}" in data.get("dsId"):
                    ds = data.get("dsId").replace("${namespace}", self.namespace)
                else:
                    ds = data.get("dsId")
                logger.info(ds)
                logger.info(data)
                response = self.client.query_metadata_byname(query="dsName=%s&dataSetName=%s&key=%s"
                                            % (ds, data.get("dataSet"), data.get("key")))
                logger.info(response)
                tmp["metaId"] = response.get("data").get("metaId")
                datasource_metaid.append(tmp)
        except Exception as err:
            logger.error(err)
            return False
        return datasource_metaid

    def getdsid(self,dsname):
        logger.info("get dsid")
        if "${namespace}" in dsname:
            dsname =dsname.replace("${namespace}", self.namespace)
        page = 0
        try:
            while 1:
                response = self.client.query_ds(query="page=%d" % page)
                for ds in response.get("data").get("data"):
                    if ds.get("name") == dsname:
                        return ds.get("dsId")
                if page == response.get("data").get("totalPages"):
                    return False
                page = page + 1
        except Exception as err:
            logger.info(err)

    def jobdata_json(self, jsonfile):
        fr = open("./datainput/tm/jobjson/10M.json")
        data = json.load(fr)
        fr.close()
        logger.info(data)
        body = {
            "name": data.get("name"),
            "code": data.get("code"),
            "taskResultVOList": data.get("taskResultVOList")
        }
        datasource = self.getmetaid(data.get("taskDataSourceVOList"))

        for result in data.get("taskResultVOList"):
            if "${namespace}" in result.get("resultDest"):
                result["resultDest"] = result.get("resultDest").replace("${namespace}", self.namespace)
        body["taskDataSourceVOList"] = datasource
        return body

    def jobdata_dict(self, data):
        datasource_metaid = self.getmetaid(data.get("datasource"))
        logger.info(datasource_metaid)
        self.assertIsInstance(datasource_metaid, list, msg="get metaid failed")
        result_dict = data.get("result")
        for tmpres in result_dict:
            dsid = self.getdsid(tmpres.get("resultDest"))
            self.assertIsInstance(dsid, str, msg="get dsid failed")
            tmpres["resultDest"] = dsid
        logger.info(result_dict)

        jobbody = {
            "name": data.get("title"),
            "taskDataSourceVOList": datasource_metaid,
            "taskResultVOList": result_dict,
            "code": data.get("code")
        }
        return jobbody

    @ddt.data(*jobcsv())
    @ddt.unpack
    def test_jobrun(self, title, data): #title, key, datasource, result, code, timeout, expect):
        """
        [ddt] ddt驱动任务测试
        :param data
        :return:
        """
        logger.info(data)
        logger.info("start job timeout=%d" % int(data.get("timeout")))
        #logger.info(title)

        if data.get("json"):
            jobbody = self.jobdata_json(jsonfile=data.get("json"))
        else:
            jobbody = self.jobdata_dict(data=data)
        logger.info(jobbody)
        response = self.client.add_task(job_data=json.dumps(jobbody))
        logger.info(response)
        if isinstance(self.check_schema(response), str):
            self.assertTrue(False, "jsonschema check failed")
        taskid = response.get("data").get("id")
        response = self.client.start_task(jobid=taskid)
        logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="start job failed")
        timecount = 0
        while timecount < int(data.get("timeout")):
            response = self.client.get_task(taskid=taskid)
            logger.info(response)
            if response.get("data").get("queueStatus") < 6:
                time.sleep(10)
                timecount = timecount + 30
                continue
            else:
                self.assertEqual(response.get("data").get("queueStatus"), 6, msg="expect task success")
                break
        #response = self.client.delete_task(jobid=taskid)
        #logger.info(response)
        self.assertEqual(response.get("code"), 0, msg="delete job expect code = 0")

"""
if __name__ == '__main__':
    os.environ["csvfiles"] = "heartbeat_ali_metaid.csv"
    os.environ["consolesite"] = "console-dev.tsingj.com"
    os.environ["consoleuser"] = "heartbeat"
    os.environ["consolepasswd"] = "qwer1234"
    print('hhhhhhh\n')
    unittest.main()
"""

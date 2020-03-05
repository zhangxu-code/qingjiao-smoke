# -*- coding: utf-8 -*-
"""
Project smoketestbak
Author  zhenghongguang
Date    2020-02-18
"""

import os
import requests
import json
from loguru import logger
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from util.redis_producer import redis_producer

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class ConsoleAPI:
    """
    console API
    """

    site = None
    user = None
    passwd = None
    token = None

    def __init__(self, site="console.tsingj.local", user="smoketest", passwd="qwer1234"):
        self.site = site
        self.user = user
        self.passwd = passwd
        if "http" in self.site:
            url = "%s/api/api-sso/token/simpleLogin" % self.site
        else:
            url = "https://%s/api/api-sso/token/simpleLogin" % self.site
        data = "username=%s&password=%s" % (self.user, self.passwd)
        logger.info(url)
        logger.info(data)
        req = requests.post(url=url, params=data, verify=False)
        # print(req.text)
        logger.info(req.text)
        self.token = req.json().get("data").get("access_token")
        self.MQ = redis_producer()

    def produces(self, api, time=None, length=None, isOK=True):
        return True
        tmp = {"api": api}
        if isOK is False:
            tmp["isOK"] = False
        else:
            tmp["isOK"] = True
            tmp["time"] = time
        try:
            self.MQ.producer(json.dumps(tmp))
        except Exception as err:
            logger.error(err)

    def add_task(self, token=None, job_data=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token,
                "Content-Type":"application/json"}
        try:
            logger.info(url)
            logger.info(head)
            logger.info(job_data)
            req = requests.post(url=url, data=job_data, headers=head, verify=False)
            self.produces(
                "POST/api/api-tm/task/v1",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("POST/api/api-tm/task/v1", isOK=False)
            return False

    def modify_task(self, token=None, job_data=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token,
                "Content-Type":"application/json"}
        try:
            logger.info(url)
            logger.info(job_data)
            req = requests.put(url=url, data=job_data, headers=head, verify=False)
            self.produces(
                "PUT/api/api-tm/task/v1",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("PUT/api/api-tm/task/v1", isOK=False)
            return False

    def delete_task(self, token=None, jobid=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.delete(
                url="%s/%d" % (url, jobid), headers=head, verify=False
            )
            self.produces(
                "DELETE/api/api-tm/v1/task",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("DELETE/api/api-tm/v1/task", isOK=False)
            return False

    def start_task(self, token=None, jobid=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task/start" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task/start" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.put(url="%s/%d" % (url, jobid), headers=head, verify=False)
            self.produces(
                "PUT/api/api-tm/v1/task/start",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("PUT/api/api-tm/v1/task/start", isOK=False)
            return False

    def stop_task(self, token=None, data=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task/stop" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task/stop" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token,
                "Content-Type":"application/json"}
        try:
            req = requests.put(url=url, data=data, headers=head, verify=False)
            self.produces(
                "PUT/api/api-tm/v1/task/stop",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("PUT/api/api-tm/v1/task/stop", isOK=False)
            return False

    def stop_task_bystatus(self, token=None, status=None):
        if "http" in self.site:
            url = "%s/api/api-tm/task/v1/stopByStatus" % self.site
        else:
            url = "https://%s/api/api-tm/task/v1/stopByStatus" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.put(url="%s/%d" % (url, status), headers=head, verify=False)
            self.produces(
                "PUT/api/api-tm/task/v1/stopByStatus",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("PUT/api/api-tm/task/v1/stopByStatus", isOK=False)
            return False

    def get_running_task(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/task/v1/getExecutorTasks" % self.site
        else:
            url = "https://%s/api/api-tm/task/v1/getExecutorTasks" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url=url, data=query, headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/task/v1/getExecutorTasks",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/task/v1/getExecutorTasks", isOK=False)
            return False

    def query_task(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            if query:
                url = "%s?%s" % (url, query)
            else:
                url = url
            logger.info(url)
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/task/v1",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/task/v1", isOK=False)
            return False

    def get_task(self, token=None, taskid=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url="%s/%d" % (url, taskid), headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/task/taskid",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/task/taskid", isOK=False)
            return False

    def get_task_result(self, token=None, taskid=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/taskResult" % self.site
        else:
            url = "https://%s/api/api-tm/v1/taskResult" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url="%s/%d" % (url, taskid), headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/taskResult",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/taskResult", isOK=False)
            return False

    def get_task_msg(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task/getExecMsg" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task/getExecMsg" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url="%s?%s" % (url, query), headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/task/getExecMsg",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/task/getExecMsg", isOK=False)
            return False

    def get_history_msg(self, token=None, taskid=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task/getHistoryExecMsg" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task/getHistoryExecMsg" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            logger.info(url)
            req = requests.get(url="%s/%d" % (url, taskid), headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/task/getHistoryExecMsg",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/task/getHistoryExecMsg", isOK=False)

    def get_role_log_byrequestid(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-track/track/getLogsByRequestIdAndRole" % self.site
        else:
            url = "https://%s/api/api-track/track/getLogsByRequestIdAndRole" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url="%s?%d" % (url, query), headers=head, verify=False)
            self.produces(
                "GET/api/api-track/track/getLogsByRequestIdAndRole",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(
                "GET/api/api-track/track/getLogsByRequestIdAndRole", isOK=False
            )

    def get_role_log_bytaskid(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-track/track/getLogsByTaskIdAndRole" % self.site
        else:
            url = "https://%s/api/api-track/track/getLogsByTaskIdAndRole" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url="%s?%s" % (url, query), headers=head, verify=False)
            self.produces(
                "GET/api/api-track/track/getLogsByTaskIdAndRole",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-track/track/getLogsByTaskIdAndRole", isOK=False)

    def get_task_roles(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/task/getTaskRoles" % self.site
        else:
            url = "https://%s/api/api-tm/v1/task/getTaskRoles" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            req = requests.get(url="%s?%s" % (url, query), headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/task/getTaskRoles",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/task/getTaskRoles", isOK=False)

    def query_ds(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/dataServer" % self.site
        else:
            url = "https://%s/api/api-tm/v1/dataServer" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            if query:
                url = "%s?%s" % (url, query)
            else:
                url = url
            logger.info(url)
            logger.info(head)
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/dataServer",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/dataServer", isOK=False)

    def query_dataset(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/dataSet" % self.site
        else:
            url = "https://%s/api/api-tm/v1/dataSet" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            if query:
                url = "%s?%s" % (url, query)
            else:
                url = url
            logger.info(url)
            logger.info(head)
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/dataSource",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/dataSource", isOK=False)

    def query_metadata(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/dataSourceMetadata" % self.site
        else:
            url = "https://%s/api/api-tm/v1/dataSourceMetadata" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            if query:
                url = "%s?%s" % (url, query)
            else:
                url = url
            logger.info(url)
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/dataSourceMetadata",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/dataSourceMetadata", isOK=False)

    def query_metadata_byname(self, token=None, query=None):
        if "http" in self.site:
            url = "%s/api/api-tm/v1/dataSourceMetadata/findDataSourceMetadataPara" % self.site
        else:
            url = "https://%s/api/api-tm/v1/dataSourceMetadata/findDataSourceMetadataPara" % self.site
        if not token:
            token = self.token
        head = {"Authorization": "bearer %s" % token}
        try:
            if query:
                url = "%s?%s" % (url, query)
            else:
                url = url
            logger.info(url)
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(
                "GET/api/api-tm/v1/dataSourceMetadata/findDataSourceMetadataPara",
                time=req.elapsed.total_seconds() * 1000,
                isOK=True,
            )
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces("GET/api/api-tm/v1/dataSourceMetadata/findDataSourceMetadataPara", isOK=False)


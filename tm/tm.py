# -*- coding: UTF-8 -*-
import time
import re
import requests
import json
#import logging
from util.redis_producer import redis_producer
from loguru import logger
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


'''
默认参数 token == None的情况下，使用 __init__初始化的获得token
否则使用 传入的token参数，可用于测试异常token情况下，接口反应
'''
class tmJob:
    token  = ''
    site   = ''
    user   = ''
    passwd = ''
    MQ     = None
    def __init__(self,site='',user='',passwd=''):
        self.site =   site
        self.user =   user
        self.passwd = passwd
        #logger = logging.getLogger()
        #logger.setLevel(logging.INFO)
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        data = "username=%s&password=%s" % (self.user, self.passwd)
        req = requests.post(url=url, params=data, verify=False)
        #print(req.text)
        logger.info(req.text)
        self.token = req.json().get("data").get("access_token")
        self.MQ = redis_producer()

    def produces(self,api,time=None,length=None,isOK=True):
        tmp = {}
        tmp["api"] = api
        if isOK == False:
            tmp["isOK"] = False
        else:
            tmp["isOK"] = True
            tmp["time"] = time
            #tmp["length"] = length
        self.MQ.producer(json.dumps(tmp))

    def login(self):
        logger.info("get access token")
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        data = "username=%s&password=%s" % (self.user, self.passwd)
        req = requests.post(url=url, params=data, verify=False)
        # print(req.text)
        logger.info(req.text)
        self.token = req.json().get("data").get("access_token")

    def job_start(self,jobid,token = None):
        logger.info("job start "+str(jobid))
        url = 'https://%s/api/api-tm/task/startTask/%s' % (self.site, str(jobid))
        if token ==None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        try:
            req = requests.put(url=url,headers = head,verify=False)
            self.produces(api="/api/api-tm/task/startTask",time=req.elapsed.total_seconds()*1000,isOK=True)
            logger.info(req.text)
            return  req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/startTask", isOK=False)
            logger.error(err)

    def job_getinfo(self,jobid,token = None):
        logger.info("get job "+str(jobid))
        url = 'https://%s/api/api-tm/task/%s' % (self.site, str(jobid))
        if token ==None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        try:
            req = requests.get(url=url,headers = head,verify=False)
            self.produces(api="GET/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="GET/api/api-tm/task", isOK=False)
            logger.error(err)

    def job_getconfig(self,jobid,token = None):
        url = 'https://%s/api/api-tm/task/getTaskSendConfig/%s'%(self.site,str(jobid))
        if token ==None:
            token = self.token
        head = {
            "Authorization":"bearer %s"%(self.token)
        }
        try:
            req = requests.get(url=url,headers = head,verify=False)
            self.produces(api="/api/api-tm/task/getTaskSendConfig", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/getTaskSendConfig", isOK=False)
            logger.error(err)

    def job_create(self,key,datasource = [],result=[],code='',token=None):
        logger.info('create job '+key)
        url = "https://%s/api/api-tm/task"%(self.site)
        body = {
            "name":"autotest_%s" %(key),
            "code": code, #re.sub(var_re,"'pp\.ss\(\"%s\"\)'"%(varname),code),
            "taskDataSourceVOList":datasource,
            "taskResultVOList":result
        }
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token),
            "Content-Type": "application/json"
        }
        try:
            req = requests.post(url=url,headers = head,data=json.dumps(body),verify = False)
            self.produces(api="POST/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="POST/api/api-tm/task",  isOK=False)
            logger.error(err)

    def job_create_body(self,body,token=None):
        url = "https://%s/api/api-tm/task" % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token),
            "Content-Type": "application/json"
        }
        try:
            req = requests.post(url=url, headers=head, data=json.dumps(body), verify=False)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="POST/api/api-tm/task",  isOK=False)

    def modify_job(self,body,token=None):
        url = "https://%s/api/api-tm/task" % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token),
            "Content-Type": "application/json"
        }
        logger.info(json.dumps(body))
        try:
            req = requests.put(url=url, headers=head, data=json.dumps(body), verify=False)
            self.produces(api="PUT/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="PUT/api/api-tm/task", isOK=False)
            logger.error(err)

    def list_job(self,page = 1,token = None):
        url = 'https://%s/api/api-tm/task' % (self.site)
        if token ==None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        data = "page=%d"%(page)
        try:
            req = requests.get(url=url,headers = head,params=data,verify = False)
            self.produces(api="GET(query)/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="GET(query)/api/api-tm/task", isOK=False)
            logger.error(err)

    def query_job(self,query,token = None):
        url = 'https://%s/api/api-tm/task' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        #data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="GET(query)/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="GET(query)/api/api-tm/task", isOK=False)


    def query_running_job(self,query,token = None):
        url = 'https://%s/api/api-tm/task/getExecutorTasks' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        #data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="GET(query)/api/api-tm/task/getExecutorTasks", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="GET(query)/api/api-tm/task/getExecutorTasks",isOK=False)

    def get_job_result(self,jobid,token = None):
        url = 'https://%s/api/api-tm/task/getTaskResult/%s' % (self.site, str(jobid))
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        try:
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(api="GET/api/api-tm/task/getTaskResult", time=req.elapsed.total_seconds() * 1000,isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="GET/api/api-tm/task/getTaskResult", isOK=False)

    def del_jobid(self,jobid,token = None):
        logger.info("job delete " + str(jobid))
        url = 'https://%s/api/api-tm/task/%s' % (self.site, str(jobid))
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        try:
            req = requests.delete(url=url, headers=head, verify=False)
            self.produces(api="DELETE/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="DELETE/api/api-tm/task", isOK=False)
            logger.error(err)

    def kill_job(self,jobid,requestId,token=None):
        logger.info("job kill " + str(jobid))
        url = 'https://%s/api/api-tm/task/killServer/%s/%s' % (self.site, str(jobid),requestId)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        try:
            req = requests.put(url=url,headers=head,verify=False)
            self.produces(api="/api/api-tm/task/killServer", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/killServer", isOK=False)
            logger.error(err)

    def kill_job_bystatus(self,status,token=None):
        logger.info("job kill " + str(status))
        url = 'https://%s/api/api-tm/task/killServers/%s' % (self.site, str(status))
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        try:
            req = requests.put(url=url, headers=head, verify=False)
            self.produces(api="/api/api-tm/task/killServers", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/killServers", isOK=False)
            logger.error(err)

    def task_roles(self,query,token = None):
        url = 'https://%s/api/api-tm/task/getTaskRoles' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        # data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="/api/api-tm/task/getTaskRoles", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/getTaskRoles",  isOK=False)
            logger.error(err)

    def getLogs_task(self,query,token=None):
        url = 'https://%s/api/api-track/track/getLogsByRequestIdAndRole' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="/api/api-track/track/getLogsByRequestIdAndRole", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="/api/api-track/track/getLogsByRequestIdAndRole", isOK=False)

    def dataServer(self,query='',token=None):
        url = 'https://%s/api/api-tm/dataServer' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        # data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="/api/api-tm/dataServer", time=req.elapsed.total_seconds() * 1000,isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/dataServer",  isOK=False)
            logger.error(err)

    def dataSource(self,query='',token=None):
        url = 'https://%s/api/api-tm/dataSource' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        # data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="/api/api-tm/dataSource", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/dataSource", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.error(err)

    def dataSourceMeatadata(self,query='',token=None):
        url = 'https://%s/api/api-tm/dataSourceMetadata' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        # data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, params=query, verify=False)
            self.produces(api="/api/api-tm/dataSourceMetadata", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/dataSourceMetadata",  isOK=False)
            logger.error(err)

    def job_historySendMsg(self,taskId,token=None):
        url = 'https://%s/api/api-tm/task/getHistorySendMsg/%d' % (self.site,taskId)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        # data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(api="/api/api-tm/task/getHistorySendMsg", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/getHistorySendMsg",isOK=False)
            logger.error(err)

    def job_schedule(self,taskId,requestId,index,token=None):
        url = 'https://%s/api/api-tm/task/getTaskSchedule/%d/%s/%d' % (self.site, taskId,requestId,index)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        # data = "page=%d" % (page)
        try:
            req = requests.get(url=url, headers=head, verify=False)
            self.produces(api="/api/api-tm/task/getTaskSchedule", time=req.elapsed.total_seconds() * 1000, isOK=True)
            logger.info(req.text)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-tm/task/getTaskSchedule", isOK=False)
            logger.error(err)

class tm_data:
    def __init__(self):
        logger.info("init")

    def login(self):
        logger.info(logger)

    def listds(self):
        url = 'https://%s/api/api-tm/dataServer' % (self.conf.get("site"))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        while 1:
            req = requests.delete(url=url, headers=head, verify=False)
            '''
            if req.json().get("subCode") == 'GLOBAL0004':
                time.sleep(1)
                self.login()
                head["Authorization"] = "bearer %s" % (self.token)
                continue
            '''
            logger.info(req.text)
            return req.json()


if __name__ == '__main__':
    print('hello')

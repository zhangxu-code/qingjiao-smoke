# -*- coding: UTF-8 -*-

import re
import requests
import HTMLReport
import json
import logging
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


'''
默认参数 token == None的情况下，使用 __init__初始化的获得token
否则使用 传入的token参数，可用于测试异常token情况下，接口反应
'''
class tmJob:
    token = ''
    site = ''
    user = ''
    passwd = ''
    def __init__(self,site='',user='',passwd=''):
        self.site =   site
        self.user =   user
        self.passwd = passwd
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        data = "username=%s&password=%s" % (self.user, self.passwd)
        req = requests.post(url=url, params=data, verify=False)
        #print(req.text)
        self.token = req.json().get("data").get("access_token")


    def job_start(self,jobid,token = None):
        logging.info("job start "+str(jobid))
        url = 'https://%s/api/api-tm/task/startTask/%s' % (self.site, str(jobid))
        if token ==None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        req = requests.put(url=url,headers = head,verify=False)
        logging.info(req.text)
        return  req.json()
    def job_getinfo(self,jobid,token = None):
        logging.info("get job "+str(jobid))
        url = 'https://%s/api/api-tm/task/%s' % (self.site, str(jobid))
        if token ==None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        req = requests.get(url=url,headers = head,verify=False)
        logging.debug(req.text)
        return req.json()

    def job_getconfig(self,jobid,token = None):
        url = 'https://%s/api/api-tm/task/getTaskSendConfig/%s'%(self.site,str(jobid))
        if token ==None:
            token = self.token
        head = {
            "Authorization":"bearer %s"%(self.token)
        }
        req = requests.get(url=url,headers = head,verify=False)
        logging.info(req.text)
        return req.json()

    def job_create(self,key,datasource = [],result=[],code='',token=None):
        logging.info('create job '+key)
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
        logging.debug(json.dumps(body))
        logging.debug(json.dumps(head))
        req = requests.post(url=url,headers = head,data=json.dumps(body),verify = False)
        logging.debug(req.text)
        return req.json()

    def list_job(self,page = 1,token = None):
        url = 'https://%s/api/api-tm/task' % (self.site)
        if token ==None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        data = "page=%d"%(page)
        req = requests.get(url=url,headers = head,params=data,verify = False)
        return req.json()

    def get_job_result(self,jobid,token = None):
        url = 'https://%s/api/api-tm/task/getTaskResult/%s' % (self.site, str(jobid))
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (self.token)
        }
        req = requests.get(url=url, headers=head, verify=False)
        logging.info(req.text)
        return req.json()

if __name__ == '__main__':
    print('hello')

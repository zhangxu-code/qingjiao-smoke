import time
import os
import sys
import re
#import logger
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import yaml
import json
import numpy
from loguru import logger

#logger = logger.getLogger()
#logger.setLevel(logger.INFO)


class TaskRunnerAPI:
    conf = None
    token = None
    def __init__(self):
        logger.info("runner init")
        fr = open('./taskrunner/conf.yml')
        self.conf = yaml.load(fr)
        logger.info(self.conf)
        fr.close()

    def run(self,code):
        if not self.login():
            logger.error("login failed")
            return False
        aliveds = self.find_allalive_ds()
        if aliveds == [] or aliveds == False:
            logger.error("no alive ds")
            return False
        datasource = self.find_metadata(aliveds)
        if not datasource:
            logger.error("no data")
            return False

        logger.info("running job")
        #[{""resultVarName"":""result"",""resultDest"":""-1""}]
        var_result = self.code_reveal(code=code)
        result = []
        for var in var_result:
            tmp = {
                "resultVarName":var,
                "resultDest" : datasource.get("dataServerId")
            }
            result.append(tmp)
        datasource_l = []
        datasource["varName"] = "data"
        datasource_l.append(datasource)
        return  self.job_pipline(datasource=datasource_l,result=result,code=self.sub_debug_reveal(code=code))

    def code_reveal(self,code):
        re_reveal = re.compile(r'''\.['debug_reveal|reveal'\([a-zA-Z_0-9\ ]+,[\ ]+['"]([a-zA-Z_\-0-9]+)['"]''')
        reveal = re.findall(re_reveal,code)
        return  reveal

    def sub_debug_reveal(self,code):
        re_reveal = re.compile(r'\.debug_reveal\(')
        return re.sub(re_reveal,".reveal(",code)

    def job_pipline(self,datasource,result,code):
        logger.info("start job")
        jobid = self.job_create(datasource=datasource,result=result,code=code)
        if jobid == False:
            return False
        if not self.job_start(jobid=jobid):
            return False
        if not  self.job_status(jobid=jobid):
            return  False
        result =  self.job_result(jobid=jobid)
        if len(result.keys()) != 0:
            self.job_delete(jobid=jobid)
        return result

    def job_create(self,datasource,result,code):
        body = {
            "name":"privpy-libarary",
            "code":code,
            "taskDataSourceVOList":datasource,
            "taskResultVOList":result
        }
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        logger.info(body)
        try:
            url = "https://%s/api/api-tm/task" %(self.conf.get("site"))
            while 1:
                req = requests.post(url=url,headers=head,data=json.dumps(body),verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    logger.info(req.text)
                    time.sleep(1)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                if req.json().get("code") != 0:
                    logger.error("create job failed :%s"%(req.text))
                    return False
                return req.json().get("data").get("id")
        except Exception as err:
            logger.error(err)
            logger.error(req.text)
            return False
    def job_start(self,jobid):
        logger.info("start job:%s"%(str(jobid)))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }

        try:
            url = "https://%s/api/api-tm/task/startTask/%s"%(self.conf.get("site"),str(jobid))
            while 1:
                req = requests.put(url=url,headers=head,verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                if req.json().get("code") != 0:
                    logger.error("start job failed:%s"%(req.text))
                    return False
                else:
                    return True
        except Exception as err:
            logger.error(err)
        return True

    def job_delete(self,jobid):
        logger.info("delete job :%s" % (str(jobid)))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        url = 'https://%s/api/api-tm/task/%s' % (self.conf.get("site"), str(jobid))
        try:
            while 1:
                req = requests.delete(url=url, headers=head, verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                else:
                    return True

        except Exception as err:
            logger.error(err)
            logger.error(req.text)
            return False

    def job_status(self,jobid):
        logger.info("check job status:%s"%(str(jobid)))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        url = 'https://%s/api/api-tm/task/%s' % (self.conf.get("site"), str(jobid))
        tryCount = 0
        try:
            while tryCount < 100:
                req = requests.get(url=url,headers=head,verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                if req.json().get("data").get("queueStatus") == 6 or req.json().get("data").get("queueStatus") == 7:
                    logger.info("job finished")
                    return True
                logger.info("job:%s is running ...."%(str(jobid)))
                time.sleep(3)
                tryCount =  tryCount + 1

        except Exception as err:
            logger.error(err)
            logger.error(req.text)
            return False

    def job_result(self,jobid):
        logger.info("get job result")
        url =  "https://%s/api/api-tm/task/getTaskResult/%s" %(self.conf.get("site"),str(jobid))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        result_ditc = {}
        try:
            while 1 :
                req = requests.get(url=url,headers=head,verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                for res in req.json().get("data"):
                    tmp = {}
                    #logger.info(type(eval(res.get("result"))))
                    tmp['val'] = numpy.array(eval(res.get("result")))
                    #logger.info(type(tmp['val']))
                    result_ditc[res.get("resultVarName")] = tmp
                return result_ditc
        except Exception as err:
            logger.error(err)
            logger.error(req.text)
            return result_ditc

    def login(self):
        logger.info("login :%s"%(self.conf.get("site")))
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.conf.get("site"))
        data = "username=%s&password=%s" % (self.conf.get("user"), self.conf.get("passwd"))
        try:
            req = requests.post(url=url, params=data, verify=False)
            # print(req.text)
            self.token = req.json().get("data").get("access_token")
        except Exception as err:
            logger.error(err)
            return False
        return  True


    def find_allalive_ds(self):
        logger.info("find ds")
        url  = 'https://%s/api/api-tm/dataServer'%(self.conf.get("site"))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        alive_ds = []
        page = 1
        try:
            while 1:
                req = requests.get(url='%s?page=%s'%(url,str(page)),headers=head,verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                if len(req.json().get("data").get("data")) == 0:
                    logger.error("ds is empty")
                    return False
                for ds in req.json().get("data").get("data"):
                    if ds.get("status") == '1':
                        alive_ds.append(ds.get("id"))
                if page == req.json().get("data").get("nextPageNo"):
                    return  alive_ds
                page = page + 1
        except Exception as err:
            logger.error(req.text)
            logger.error(err)
            return False

    def find_metadata(self,aliveds = []):
        datasource = {}
        logger.info("find metadata")
        url = 'https://%s/api/api-tm/dataSourceMetadata' %(self.conf.get("site"))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        page = 1
        try:
            while 1:
                req = requests.get(url='%s?page=%s'%(url,str(page)),headers=head,verify=False)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                if len(req.json().get("data").get("data")) == 0:
                    logger.info("data is empty")
                    return False
                for data in req.json().get("data").get("data"):
                    if data.get("dsId")  in aliveds:
                        datasource["dataSourceId"] = req.json().get("data").get("data")[0].get("dataSetId")
                        datasource["dataSourceMetadataId"] = req.json().get("data").get("data")[0].get("id")
                        datasource["dataServerId"] = req.json().get("data").get("data")[0].get("dsId")
                        return  datasource
                if page == req.json().get("data").get("nextPageNo"):
                    return False
                page = page + 1
        except Exception as err:
            logger.error(err)
            logger.error(req.text)
            return False


if __name__ == '__main__':
    runner = TaskRunnerAPI()
    code= '''import privpy as pp
import os
import sys
sys.path.append(os.getcwd() + '/privpy_lib')
import pnumpy as pnp
s1 = pnp.eye(4)
pp.debug_reveal(s1, 'result1')
    '''
    #print(runner.sub_debug_reveal(code))
    #print(runner.code_reveal(code=code))
    #print(runner.run(code=code))

    res = '[[1.0,0.0,0.0,0.0],[0.0,1.0,0.0,0.0],[0.0,0.0,1.0,0.0],[0.0,0.0,0.0,1.0]]'
    print(res.split(','))
    print(eval(res))

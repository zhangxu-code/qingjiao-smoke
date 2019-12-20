import time
import os
import sys
sys.path.append(os.getcwd())
import re
#import logger
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import yaml
import json
import numpy as np
import numpy.testing as npt
from loguru import logger
from util.redis_producer import redis_producer
#from multiprocessing import Lock
import threading
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
        self.MQ = redis_producer()

    def produces(self,api,time=None,isOK=True):
        tmp = {}
        tmp["api"] = api
        if isOK == False:
            tmp["isOK"] = False
        else:
            tmp["isOK"] = True
            tmp["time"] = time
        self.MQ.producer(json.dumps(tmp))

    def run(self,code,name=None):
        logger.info(threading.current_thread().name)
        #return True
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
        return  self.job_pipline(datasource=[] ,result=result,code=self.sub_debug_reveal(code=code),name=name)

    def code_reveal(self,code):
        print(code.split('\n'))
        re_reveal = re.compile(r'''^[a-zA-Z_\-0-9\ \=]+\.(?:debug_reveal|reveal)\([a-zA-Z_0-9\ \[\]\-\.\(\),=]+['"]([a-zA-Z_\-0-9]+)['"]''')
        reveal = []
        for line in code.split('\n'):
            tmp = re.search(re_reveal,line)
            if tmp:
                print(line)
                reveal.append(tmp.group(1))
        return  reveal

    def sub_debug_reveal(self,code):
        re_reveal = re.compile(r'\.debug_reveal\(')
        return re.sub(re_reveal,".reveal(",code)

    def job_pipline(self,datasource,result,code,name=None):
        logger.info("start job")
        jobid = self.job_create(datasource=datasource,result=result,code=code,name=name)
        if jobid == False:
            return False
        if not self.job_start(jobid=jobid):
            return False
        if not  self.job_status(jobid=jobid):
            return  False
        result =  self.job_result(jobid=jobid)
        #if len(result.keys()) != 0:
        #    self.job_delete(jobid=jobid)
        return result

    def job_create(self,datasource,result,code,name=None):
        if name == None:
            jobname = 'privpy-libarary'
        else:
            jobname = 'privpy-libarary-'+name
        body = {
            "name":jobname,
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
                self.produces(api="POST/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
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
            self.produces(api="POST/api/api-tm/task", isOK=False)
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
                self.produces(api="PUT/api/api-tm/task/startTask", time=req.elapsed.total_seconds() * 1000, isOK=True)
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
            self.produces(api="PUT/api/api-tm/task/startTask", isOK=False)
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
                self.produces(api="DELETE/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
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
            self.produces(api="DELETE/api/api-tm/task",  isOK=False)
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
            while tryCount < 300:
                req = requests.get(url=url,headers=head,verify=False)
                self.produces(api="GET/api/api-tm/task", time=req.elapsed.total_seconds() * 1000, isOK=True)
                logger.info(req.text)
                if req.json().get("subCode") == 'GLOBAL0004':
                    time.sleep(1)
                    logger.info(req.text)
                    self.login()
                    head["Authorization"] = "bearer %s" % (self.token)
                    continue
                if req.json().get("data").get("queueStatus") == 6 or req.json().get("data").get("queueStatus") == 7  or req.json().get("data").get("queueStatus") == 8:
                    logger.info("job finished")
                    return True
                logger.info("job:%s is running ...."%(str(jobid)))
                time.sleep(6)
                tryCount =  tryCount + 1

        except Exception as err:
            self.produces(api="GET/api/api-tm/task", isOK=False)
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
            get_result = True
            while 1:
                get_result = True
                req = requests.get(url=url,headers=head,verify=False)
                self.produces(api="GET/api/api-tm/task/getTaskResult", time=req.elapsed.total_seconds() * 1000, isOK=True)
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
                    if res.get("result") == None:
                        get_result = False
                    try:
                        if isinstance((eval(res.get("result"))),list):
                            logger.info("list result")
                            tmp['val'] = np.array(eval(res.get("result")),dtype=float)
                            logger.info(tmp['val'].dtype)
                        else:
                            tmp['val'] = eval(res.get("result"))
                        result_ditc[res.get("resultVarName")] = tmp
                    except Exception as err:
                        logger.info(err)
                        tmp['val'] = res.get("result")
                        result_ditc[res.get("resultVarName")] = tmp
                if get_result == False:
                    time.sleep(3)
                    continue
                return result_ditc
        except Exception as err:
            self.produces(api="GET/api/api-tm/task/getTaskResult",  isOK=False)
            logger.error(err)
            logger.error(req.text)
            return result_ditc

    def login(self):
        threadname = threading.current_thread().name
        logger.info("login :%s"%(self.conf.get("site")))
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.conf.get("site"))
        if 'multiRun' in threadname:
            username = self.conf.get("user")+str((int(threadname.split('_')[-1])+1)%(int(self.conf.get("thread"))))
        else:
            username = self.conf.get("user")
        data = "username=%s&password=%s" % (username, self.conf.get("passwd"))
        logger.info(data)
        try:
            req = requests.post(url=url, params=data, verify=False)
            # print(req.text)
            self.token = req.json().get("data").get("access_token")
            self.produces(api="POST/api/api-sso/token/simpleLogin", time=req.elapsed.total_seconds() * 1000, isOK=True)
        except Exception as err:
            logger.error(err)
            self.produces(api="POST/api/api-sso/token/simpleLogin",  isOK=False)
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
                self.produces(api="GET/api/api-tm/dataServer", time=req.elapsed.total_seconds() * 1000, isOK=True)
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
                    if ds.get("status") == '1' and ds.get("isCache") == 0:
                        alive_ds.append(ds.get("id"))
                if page == req.json().get("data").get("nextPageNo"):
                    return  alive_ds
                page = page + 1
        except Exception as err:
            self.produces(api="GET/api/api-tm/dataServer", isOK=False)
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
                self.produces(api="GET/api/api-tm/dataSourceMetadata", time=req.elapsed.total_seconds() * 1000, isOK=True)
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
            self.produces(api="GET/api/api-tm/dataSourceMetadata", isOK=True)
            logger.error(err)
            logger.error(req.text)
            return False


if __name__ == '__main__':
    runner = TaskRunnerAPI()
    code= '''
import privpy as pp
import os
import sys
sys.path.append(os.getcwd() + '/privpy_lib')
import pnumpy as pnp
import numpy as np

a = pp.farr([1, 2])
b = pp.farr([1, 2, 3])
c = pp.farr([3, 4])
d = pp.farr([1, 3])

s1 = pnp.array_equal(a, a)
s2 = pnp.array_equal(a, b)       
s3 = pnp.array_equal(a, c)  
s4 = pnp.array_equal(a, d) 

pp.debug_reveal(s1, 's1')
pp.debug_reveal(s2, 's2')
pp.debug_reveal(s3, 's3')
pp.debug_reveal(s4, 's4')
    '''
    #print(runner.sub_debug_reveal(code))
    #print(runner.code_reveal(code=code))
    res = (runner.run(code=code))
    print(res)
    a = np.array([1, 2])
    b = np.array([1, 2, 3])
    c = np.array([3, 4])
    d = np.array([1, 3])

    #res = test_util.run_task(code)
    s1 = res['s1']['val']
    s2 = res['s2']['val']
    s3 = res['s3']['val']
    s4 = res['s4']['val']

    npt.assert_array_almost_equal(s1, np.array_equal(a, a))
    npt.assert_array_almost_equal(s2, np.array_equal(a, b))
    npt.assert_array_almost_equal(s3, np.array_equal(a, c))
    npt.assert_array_almost_equal(s4, np.array_equal(a, d))
    print('assert ok')
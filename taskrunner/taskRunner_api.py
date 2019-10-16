import time
import os
import sys
import re
import logging
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import yaml
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TaskRunnerAPI:
    conf = None
    token = None
    def __init__(self):
        logging.info("runner init")
        fr = open('conf.yml')
        self.conf = yaml.load(fr)
        fr.close()

    def run(self,code):
        if not self.login():
            logging.error("login failed")
            return False
        aliveds = self.find_allalive_ds()
        if aliveds == [] or aliveds == False:
            logging.error("no alive ds")
            return False
        datasource = self.find_metadata(aliveds)
        if not datasource:
            logging.error("no data")
            return False

        logging.info("running job")
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
        return  self.job_pipline(datasource=datasource_l,result=result,code=code)

    def code_reveal(self,code):
        re_reveal = re.compile(r'''\.debug_reveal|reveal\([a-zA-Z_0-9\ ]+,[\ ]+['"]([a-zA-Z_\-0-9]+)['"]''')
        reveal = re.findall(re_reveal,code)
        return  reveal


    def job_pipline(self,datasource,result,code):
        logging.info("start job")
        jobid = self.job_create(datasource=datasource,result=result,code=code)
        if jobid == False:
            return False
        if not self.job_start(jobid=jobid):
            return False
        if not  self.job_status(jobid=jobid):
            return  False
        return self.job_result(jobid=jobid)

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
        logging.info(body)
        try:
            url = "https://%s/api/api-tm/task" %(self.conf.get("site"))
            req = requests.post(url=url,headers=head,data=json.dumps(body),verify=False)
            if req.json().get("code") != 0:
                logging.error("create job failed :%s"%(req.text))
                return False
            return req.json().get("data").get("id")
        except Exception as err:
            logging.error(err)
            logging.error(req.text)
            return False
    def job_start(self,jobid):
        logging.info("start job:%s"%(str(jobid)))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }

        try:
            url = "https://%s/api/api-tm/task/startTask/%s"%(self.conf.get("site"),str(jobid))
            req = requests.put(url=url,headers=head,verify=False)
            if req.json().get("code") != 0:
                logging.error("start job failed:%s"%(req.text))
                return False
        except Exception as err:
            logging.error(err)
        return True

    def job_status(self,jobid):
        logging.info("check job status:%s"%(str(jobid)))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        url = 'https://%s/api/api-tm/task/%s' % (self.conf.get("site"), str(jobid))
        try:
            while 1:
                req = requests.get(url=url,headers=head,verify=False)
                if req.json().get("data").get("queueStatus") == 6 or req.json().get("data").get("queueStatus") == 7:
                    logging.info("job finished")
                    return True
                logging.info("job:%s is running ...."%(str(jobid)))
                time.sleep(3)

        except Exception as err:
            logging.error(err)
            logging.error(req.text)
            return False

    def job_result(self,jobid):
        logging.info("get job result")
        url =  "https://%s/api/api-tm/task/getTaskResult/%s" %(self.conf.get("site"),str(jobid))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        result_ditc = {}
        try:
            req = requests.get(url=url,headers=head,verify=False)
            for res in req.json().get("data"):
                result_ditc[res.get("resultVarName")] = res.get("result")
            return result_ditc
        except Exception as err:
            logging.error(err)
            logging.error(req.text)
            return False

    def login(self):
        logging.info("login :%s"%(self.conf.get("site")))
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.conf.get("site"))
        data = "username=%s&password=%s" % (self.conf.get("user"), self.conf.get("passwd"))
        try:
            req = requests.post(url=url, params=data, verify=False)
            # print(req.text)
            self.token = req.json().get("data").get("access_token")
        except Exception as err:
            logging.error(err)
            return False
        return  True


    def find_allalive_ds(self):
        logging.info("find ds")
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
                if len(req.json().get("data").get("data")) == 0:
                    logging.error("ds is empty")
                    return False
                for ds in req.json().get("data").get("data"):
                    if ds.get("status") == '1':
                        alive_ds.append(ds.get("id"))
                if page == req.json().get("data").get("nextPageNo"):
                    return  alive_ds
                page = page + 1
        except Exception as err:
            logging.error(req.text)
            logging.error(err)
            return False

    def find_metadata(self,aliveds = []):
        datasource = {}
        logging.info("find metadata")
        url = 'https://%s/api/api-tm/dataSourceMetadata' %(self.conf.get("site"))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        page = 1
        try:
            while 1:
                req = requests.get(url='%s?page=%s'%(url,str(page)),headers=head,verify=False)
                if len(req.json().get("data").get("data")) == 0:
                    logging.info("data is empty")
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
            logging.error(err)
            logging.error(req.text)
            return False

    def  check_dsalive(self,dsid):
        logging.info("check ds is alive")
        url = "https://%s/api/api-tm/dataServer/%s" %(self.conf.get("site"),str(dsid))
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        try:
            req = requests.get(url=url,verify=False,headers = head)
            logging.info(req.text)
            if req.json().get("data").get("status") == 1:
                logging.info("ds:%s alive"%(str(dsid)))
                return True
            else:
                return False
        except Exception as err:
            logging.error(err)
            logging.error(req.text)

if __name__ == '__main__':
    runner = TaskRunnerAPI()
    code= '''import privpy as pp
import os
import sys
sys.path.append(os.getcwd() + '/privpy_lib')
import pnumpy as pnp
s1 = pnp.eye(4)
pp.reveal(s1, 'result1')
    '''
    #print(runner.code_reveal(code=code))
    print(runner.run(code=code))
import unittest
from tm.tm import tmJob
import logging
import time
import ddt
import csv
import re
import os
import json
import numpy.testing as npt
#env = None
def get_job_csv(env='master',key=None):
    #global env
    logging.info(env)
    value_rows = []
    csvfiles = os.listdir('./datainput/tm/')
    csvtype = re.compile(r'_([a-zA-Z]+)_([a-zA-Z]+)\.csv')
    fw = open('./datainput/tm/jobs.csv','w')
    writer = csv.writer(fw)
    fw.write('title,datasource,result,code\n')
    for csv_name in csvfiles:
        print(csv_name)
        if not re.search(csvtype,csv_name):
            logging.info('skip '+csv_name)
            continue
        csvfind = re.search(csvtype,csv_name)
        if key == None:
            local_csv_name = csvfind.group(2)
            expect_csv_name = env
        else:
            local_csv_name = csvfind.group(1)+'_'+csvfind.group(2)
            expect_csv_name = key+'_'+env

        if local_csv_name != expect_csv_name:
            logging.info('skip '+csv_name)
            continue
        logging.info("read job info "+csv_name)
        fr = open('./datainput/tm/'+csv_name)
        csvfile = csv.reader(fr)
        next(csvfile)
        # csvfile = csv.DictReader(fr)
        for row in csvfile:
            writer.writerow(row)
        fr.close()
    fw.close()

def job_csv():
    value_rows = []
    fr = open('./datainput/tm/jobs.csv')
    csvfile = csv.reader(fr)
    next(csvfile)
    # csvfile = csv.DictReader(fr)
    for row in csvfile:
        value_rows.append((row))
    fr.close()
    logging.info(len(value_rows))
    return value_rows

@ddt.ddt
class tmTestcases(unittest.TestCase):
    _tm = None
    @classmethod
    def setUpClass(cls) -> None:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        global conf_args
        global site
        tmp_site = site
        global user
        tmp_user = user
        global passwd
        tmp_passwd = passwd
        logging.info("%s : %s : %s "%(tmp_site,tmp_user,tmp_passwd))
        cls._tm = tmJob(site = tmp_site ,user = tmp_user, passwd=tmp_passwd)
    @classmethod
    def tearDownClass(cls) -> None:
        None
    def jobstartcase(self):
        jobid_l = conf_args.get("jobid").split(',')
        for jobid in jobid_l:
            ret = self._tm.job_start(jobid)
            self.assertEqual(ret.get("code"),0)
            self.assertIsInstance(ret.get("data"),str)
    def jobstart_pipline_case(self):
        # 由用户指定输入jobid
        jobid_l = conf_args.get("jobid").split(',')
        logging.info("start job %s"%(conf_args.get("jobid")))
        for jobid in jobid_l:
            ret = self._tm.job_start(jobid)
            self.assertEqual(ret.get("code"),0)
            self.assertIsInstance(ret.get("data"),str)
            finished = False
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=jobid)
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)

    def jobgetConfig_case(self):
        jobid_L = conf_args.get("jobid").split(',')
        for jobid in jobid_L:
            ret = self._tm.job_getconfig(jobid)
            self.assertEquals(ret.get("code"),0)

    #创建任务的结果校验
    def createjob_ret_check(self,key,ret,code,datasource,result):
        logging.info(ret)
        self.assertEquals(ret.get("code"), 0)
        self.assertIsInstance(ret.get("data").get("id"), int)
        self.assertEquals(ret.get("data").get("name"),
                          "autotest_%s" % (key))
        self.assertEquals(ret.get("data").get("code"), code)
        datasource_ret = []
        for data in ret.get("data").get("taskDataSourceVOList"):
            tmp = {}
            tmp["dataSourceId"] = data.get("dataSourceId")
            tmp["dataSourceMetadataId"] = data.get("dataSourceMetadataId")
            tmp["varName"] = data.get("varName")
            tmp["dataServerId"] = data.get("dataServerId")
            datasource_ret.append(tmp)
        result_ret = []
        for res in ret.get("data").get("taskResultVOList"):
            tmp = {}
            tmp["resultVarName"] = res.get("resultVarName")
            tmp["resultDest"]    = res.get("resultDest")
            result_ret.append(tmp)
        print(datasource)
        print(datasource_ret)
        logging.debug(result)
        logging.debug(result_ret)
        self.assertEquals(datasource_ret,datasource)
        self.assertEquals(result_ret,result)

    def createjob_pipline(self,key,datasource,result,code,timeout = None,expect=None):
        ret = self._tm.job_create(key=key, result=result,
                                  datasource=datasource,
                                  code=code)
        self.createjob_ret_check(key=key, ret=ret, code=code, datasource=datasource, result=result)
        ret_start = self._tm.job_start(ret.get("data").get("id"))
        self.assertEqual(ret_start.get("code"), 0)
        self.assertIsInstance(ret_start.get("data"), str)
        finished = False
        # get jobinfo and check job status
        trycount = 0
        while not finished:
            jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
            if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                finished = True
            time.sleep(3)
            if timeout != None:
                trycount = trycount+1
                if trycount* 3 >= int(timeout):
                    self.assertTrue(False,"job not finished ,timeout:%s"%(str(timeout)))
        self.assertEquals(jobinfo.get("code"), 0)
        self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
        jobresult = self._tm.get_job_result(jobid=ret.get("data").get("id"))
        if expect != '' and expect != None:
            self.check_result(jobresult.get("data"),expect_res=json.loads(expect))
        if 'heartbeat' in key:
            self._tm.del_jobid(jobid=ret.get("data").get("id"))

    def check_result(self,result,expect_res):
        for res in result:
            npt.assert_almost_equal(eval(res.get("result")),expect_res.get(res.get("resultVarName")),decimal=8)

    @ddt.data(*job_csv())
    @ddt.unpack
    def test_jobrun(self,title,datasource,result,code,timeout=None,expect=None):
        self.createjob_pipline(key=title,datasource=json.loads(datasource),result=json.loads(result),code=code,timeout=timeout,expect=expect)


    def jobcreate_multiplication_pipline(self):
        logging.info("create job")
        #create job
        jobCount = 1
        datasource = [{
            "dataServeId":324,
            "dataSourceId":523,
            "dataSourceMetadataId":766,
            "varName":"data"
        }]
        result = [{
            "resultDest":"324",
            "resultVarName":"result"
        }]
        var_re = re.compile('pp\.ss\(\"[\w]+\"\)')
        varname = 'data'
        code = "import privpy as pp\ndd=pp.ss(\"%s\")\nresult = dd*dd \npp.reveal(result,\"result\")" %(varname)
        index = 0
        while index < jobCount:
            ret = self._tm.job_create(key='multiplication',result=result,
                                datasource=datasource,
                                code=code)
            #check create job response
            self.createjob_ret_check(key='multiplication',ret=ret,code=code,datasource=datasource,result=result)
            #start job and check code
            ret_start = self._tm.job_start(ret.get("data").get("id"))
            self.assertEqual(ret_start.get("code"), 0)
            self.assertIsInstance(ret_start.get("data"), str)
            finished = False
            #get jobinfo and check job status
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
            index = index + 1
    def jobcreate_addition_pipline(self):
        logging.info("create job")
        #create job
        jobCount = 1
        datasource = [{
            "dataServeId": 324,
            "dataSourceId": 523,
            "dataSourceMetadataId": 766,
            "varName": "data"
        }]
        result = [{
            "resultDest": "324",
            "resultVarName": "result"
        }]
        var_re = re.compile('pp\.ss\(\"[\w]+\"\)')
        varname = 'data'
        code = "import privpy as pp\ndd=pp.ss(\"%s\")\nresult = dd + dd \npp.reveal(result,\"result\")" %(varname)
        index = 0
        while index < jobCount:
            ret = self._tm.job_create(key='addition',result=result,
                                datasource=datasource,
                                code=code)
            #check create job response
            self.createjob_ret_check(key='addition',ret=ret,code=code,datasource=datasource,result=result)
            #start job and check code
            ret_start = self._tm.job_start(ret.get("data").get("id"))
            self.assertEqual(ret_start.get("code"), 0)
            self.assertIsInstance(ret_start.get("data"), str)
            finished = False
            #get jobinfo and check job status
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
            index = index + 1
    def jobcreate_comparison_pipline(self):
        logging.info("create job")
        #create job
        jobCount = 1
        datasource = [{
            "dataServeId": 324,
            "dataSourceId": 523,
            "dataSourceMetadataId": 766,
            "varName": "data"
        }]
        result = [{
            "resultDest": "324",
            "resultVarName": "result"
        }]
        varname = 'data'
        code = "import privpy as pp\ndd=pp.ss(\"%s\")\nresult = dd < dd \npp.reveal(result,\"result\")" %(varname)
        index = 0
        while index < jobCount:
            ret = self._tm.job_create(key='comparison',result=result,
                                datasource=datasource,
                                code=code)
            #check create job response
            self.createjob_ret_check(key='comparison',ret=ret,code=code,datasource=datasource,result=result)
            #start job and check code
            ret_start = self._tm.job_start(ret.get("data").get("id"))
            self.assertEqual(ret_start.get("code"), 0)
            self.assertIsInstance(ret_start.get("data"), str)
            finished = False
            #get jobinfo and check job status
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
            index = index + 1
    def listjob_check(self,ret,curpage):
        if ret.get("data").get("totalPages") > curpage:
            self.assertEquals(ret.get("data").get("nextPageNo"), curpage + 1)
        self.assertEquals(ret.get("data").get("previousPageNo"), curpage if curpage == 1 else curpage + 1)
        self.assertEquals(ret.get("code"), 0)
        self.assertIsInstance(ret.get("data"), dict)
        self.assertIsInstance(ret.get("data").get("pageNo"), int)
        self.assertIsInstance(ret.get("data").get("pageSize"), int)
        self.assertIsInstance(ret.get("data").get("totalRows"), int)
        self.assertIsInstance(ret.get("data").get("data"), list)
    def listjob_case(self):
        curpage = 1
        hasmorejob = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret,curpage=curpage)
            if ret.get("data").get("totalPages") == curpage:
                hasmorejob == False
            curpage =  curpage + 1
    def listjob_case_wrongtoken(self):
        ret = self._tm.list_job(token="123456")
        self.assertEquals(ret.get("code"),1)
        self.assertNotEquals(ret.get("msg"),"成功")
    def jobstartbylist_pipline_case(self):
        # list job，然后对第一个已完成的job 重新开始，如没有已完成job assert(false)
        curpage = 1
        hasmorejob = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 6:
                    ret_j = self._tm.job_start(job.get("id"))
                    self.assertEqual(ret_j.get("code"), 0)
                    self.assertIsInstance(ret_j.get("data"), str)
                    finished = False
                    while not finished:
                        jobinfo = self._tm.job_getinfo(jobid=job.get("id"))
                        if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                            finished = True
                        time.sleep(1)
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
                    hasmorejob = False
                    break
    # 获取job result，三种状态的job类型
    def jobgetresult_job_success_pipline_case(self):
        curpage = 1
        hasmorejob = True
        nosuccess = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 6:
                    jobinfo = self._tm.get_job_result(jobid=job.get("id"))
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("msg"), "成功")
                    self.assertIsInstance(jobinfo.get("data"),dict)
                    hasmorejob = False
                    nosuccess = False
                    break

        self.assertFalse(nosuccess)

    def jobgetresult_job_failed_pipline_case(self):
        curpage = 1
        hasmorejob = True
        nofailed = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 7:
                    jobinfo = self._tm.get_job_result(jobid=job.get("id"))
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("msg"), "成功")
                    self.assertIsInstance(jobinfo.get("data"),dict)
                    hasmorejob = False
                    nofailed = False
                    break
        self.assertFalse(nofailed)

    def jobgetresult_job_notrun_pipline_case(self):
        curpage = 1
        hasmorejob = True
        norun = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 0:
                    jobinfo = self._tm.get_job_result(jobid=job.get("id"))
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("msg"), "成功")
                    self.assertIsInstance(jobinfo.get("data"),dict)
                    hasmorejob = False
                    norun = False
                    break
        self.assertFalse(norun)


def tm_init(insite,inuser,inpasswd,inenv = 'master'):
    global site
    site = insite
    global user
    user = inuser
    global passwd
    passwd = inpasswd
    logging.info("%s : %s :%s "%(site,user,passwd))
    global env
    env = inenv
    logging.info(env)

if __name__ == '__main__':
    #tm_init('1','2','3','master')
    #get_job_csv(key='heartbeat')

    jobs =  (job_csv())
    for job in jobs:
        print(type(job[1]))
        #print(job[1])
        #print(json.loads(job[1]))
        #print(json.loads(job[2]))
        print(job[5])
        print(type(job[5]))
        #print(r'%s'%(job[3]))

    #fw = open('test.txt', 'w')
    #runner = unittest.TextTestRunner(stream=fw, verbosity=2)
    #tmTestcases()
    #suit = unittest.TestSuite()
    #suit.addTest(tmTestcases(testname=None,inenv='master'))
    #suit.addTest(tmTestcases('test_jobrun'))
    #suit1 = unittest.TestLoader().loadTestsFromTestCase(tmTestcases)
    #logging.info(suit1)
    #suit1.addTest("test_jobrun")
    #suit = unittest.TestSuite()
    #suit.addTest(tmTestcases("test_jobrun"))
    #print(suit)
    #runner.run(suit1)

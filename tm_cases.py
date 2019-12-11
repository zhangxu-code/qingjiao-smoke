import unittest
from tm.tm import tmJob
import logging
import time
import ddt
import csv
import re
import os
import HTMLReport
import json
import numpy.testing as npt
#env = None

def job_csv():
    csv.field_size_limit(1024*1024*10)
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
        self.assertEquals(ret.get("code"), 0,msg="createjob:校验 response.code failed jobid:%d  %s-%s"%(ret.get("data").get("id"),ret.get("msg"),str(ret.get("subCode"))))
        self.assertIsInstance(ret.get("data").get("id"), int,msg="createjob: id类型错误 id:%s type:%s"
                                                                 %(str(ret.get("data").get("id")),str(type(ret.get("data").get("id")))))
        self.assertEquals(ret.get("data").get("name"),"autotest_%s" % (key),
                          msg="createjob: 校验jobname错误,cur:%s expect:autotest_%s  jobid:%d"%(ret.get("data").get("name"),key,ret.get("data").get("id")))
        self.assertEquals(ret.get("data").get("code"), code,msg="createjob: 校验code与提交任务的code不一致,code:%s expect:%s jobid:%d"
                                                                %(ret.get("data").get("code"),code,ret.get("data").get("id")))
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
        self.assertEquals(datasource_ret,datasource,msg="createjob: 校验 taskDataSourceVOList与预期不符 datasource:%s expect:%s jobid:%d"
                                                        ""%(json.dumps(datasource_ret),json.dumps(datasource),ret.get("data").get("id")))

        self.assertEquals(result_ret,result,msg="creaetjob: 校验 taskResultVOList 与预期不符， resultDest：%s expect:%s jobid:%d"
                                                %(json.dumps(result_ret),json.dumps(result),ret.get("data").get("id")) )

    def createjob_pipline(self,key,datasource,result,code,timeout = None,expect=None):
        ret = self._tm.job_create(key=key, result=result,
                                  datasource=datasource,
                                  code=code)
        self.createjob_ret_check(key=key, ret=ret, code=code, datasource=datasource, result=result)
        jobid  = ret.get("data").get("id")
        ret_start = self._tm.job_start(ret.get("data").get("id"))
        self.assertEqual(ret_start.get("code"), 0,
                         msg="startjob: 启动任务失败  code:%d msg:%s jobid:%d"%(ret_start.get("code"),ret_start.get("msg"),jobid))
        self.assertIsInstance(ret_start.get("data"), str,
                              msg="startjob: 校验data 类型失败 data:%s type:%s  jobid:%d"%(str(ret_start.get("data")),str(type(ret_start.get("data"))),jobid) )
        requestid = ret_start.get("data")
        finished = False
        # get jobinfo and check job status
        trycount = 0
        while not finished:
            jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
            if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                finished = True
            time.sleep(3)
            if timeout != None and timeout != '':
                trycount = trycount+1
                if trycount* 3 >= int(timeout):
                    self.assertTrue(False,msg="任务长时间未执行完成 ,timeout:%s jobid:%d-requestid:%s"%(str(timeout),jobid,requestid))
        self.assertEquals(jobinfo.get("code"), 0,
                          msg="getjobinfo: response.code 校验失败 code:%d msg:%s jobid:%d,requestId:%s"%(jobinfo.get("code"),jobinfo.get("msg"),jobid,requestid))
        self.assertEquals(jobinfo.get("data").get("queueStatus"), 6,
                          msg="getjobinfo 任务执行失败: queueStatus:%d msg:%s  jobid:%d requestId:%s"%
                              (jobinfo.get("data").get("queueStatus"),str(jobinfo.get("msg")),jobid,requestid))
        jobresult = self._tm.get_job_result(jobid=ret.get("data").get("id"))
        if expect != '' and expect != None:
            print(expect)
            json.loads(expect)
            self.check_result(jobresult.get("data"),expect_res=json.loads(expect),jobid=jobid,requestId=requestid)
        if 'heartbeat' in key:
            self._tm.del_jobid(jobid=ret.get("data").get("id"))

    def check_result(self,result,expect_res,jobid=0,requestId=''):
        for res in result:
            npt.assert_almost_equal(eval(res.get("result")),(expect_res.get(res.get("resultVarName"))),decimal=4,
                                    err_msg="jobreslut: 校验任务结果与预期结果不一致：curresult: expect: jobid:%d requestId:%s"
                                            %(jobid,requestId))

    @ddt.data(*job_csv())
    @ddt.unpack
    def test_jobrun(self,title,datasource,result,code,timeout=None,expect=None):
        self.createjob_pipline(key=title,datasource=json.loads(datasource),result=json.loads(result),code=code,timeout=timeout,expect=expect)

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
    '''
    jobs =  (job_csv())
    for job in jobs:
        print(type(job[1]))
        #print(job[1])
        #print(json.loads(job[1]))
        #print(json.loads(job[2]))
        print(job[5])
        print(type(job[5]))
        #print(r'%s'%(job[3]))
    '''
    # master  GIS : [{""dataSourceId"":714,""dataSourceMetadataId"":1156,""dataServerId"":1,""varName"":""dinning_info""}]
    # dev GIS : [{""dataSourceId"":3196,""dataSourceMetadataId"":6939,""dataServerId"":70,""varName"":""dinning_info""}]
    tm_init(insite="console.tsingj.local",inuser="smoketest",inpasswd="qwer1234")
    unittest.main()

    suit1 = unittest.TestLoader().loadTestsFromTestCase(tmTestcases)
    '''
    suit = unittest.TestSuite(suit1)
    #suit.addTest(dbengineCases("agg_gold_case"))
    runner.run(suit)
    '''
    # suit.addTest(dbengineCases("agg_case"))
    runner = HTMLReport.TestRunner(report_file_name='test',
                                   output_path='./',
                                   description='login test suite',
                                   thread_count=1,
                                   thread_start_wait=3,
                                   sequential_execution=False,
                                   lang='cn')
    runner.run(suit1)

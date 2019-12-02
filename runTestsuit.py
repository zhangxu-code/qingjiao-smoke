#!/user/bin/env python3
import unittest
import HTMLReport
import json
import datetime
import pytz
import argparse
import time
import yaml
import os
import sys
import re
from xmlrunnerR import xmlrunner
from es2csv.es2csv import es2csv
from tm_cases import tmTestcases
from tm_cases import tm_init
from dbengine_cases import dbengineCases
from dbengine_cases import db_init
from login_cases import loginTestcases
from login_cases import login_init
from alarm.alarm import post_alarm

import logging

def cur_utc_time():
    utc_tz = pytz.timezone('UTC')
    curtime = datetime.datetime.now(tz=utc_tz)
    return (curtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

def downlog(begintime,endtime,filename = 'log.csv'):
    es = es2csv(host="10.18.0.18",filename=filename)
    es.query_time(begintime=begintime, endtime=endtime)
    scrollid = es.scroll()
    print(scrollid)
    es.search(scroll_id=scrollid)

def login_suit():
    loginsuit = unittest.TestSuite()
    loginsuit.addTest(loginTestcases("logincase"))
    return loginsuit


def inspection_suit():
    inspectionsuit = unittest.TestSuite()
    inspectionsuit.addTest(tmTestcases("jobcreate_multiplication_pipline"))
    inspectionsuit.addTest(tmTestcases("jobcreate_addition_pipline"))
    inspectionsuit.addTest(tmTestcases("jobcreate_comparison_pipline"))
    inspectionsuit.addTest(tmTestcases("listjob_case"))
    inspectionsuit.addTest(tmTestcases("listjob_case_wrongtoken"))
    inspectionsuit.addTest(loginTestcases("logincase_ok"))
    inspectionsuit.addTest(loginTestcases("logincase_wrong_user"))
    inspectionsuit.addTest(loginTestcases("logincase_wrong_passwd"))
    return inspectionsuit

def tm_smoke_suit():
    tmsmokesuit = unittest.TestLoader().loadTestsFromTestCase(tmTestcases)
    #tmsmokesuit.addTest(loginTestcases("logincase_ok"))
    logging.info("get tm smoke cases")
    return tmsmokesuit

def db_smoke_suit():
    #dbsomkesuit = unittest.TestLoader().loadTestsFromTestCase(dbengineCases)
    dbsomkesuit = unittest.TestSuite()
    dbsomkesuit.addTest(dbengineCases("describe_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_filter_gold_case"))

    return dbsomkesuit

def library_smoke_suit():
    basePath = os.getcwd()+'/privpy_library/privpy_lib/tests/'
    array_creation = unittest.defaultTestLoader.discover(basePath+'/test_pnumpy', pattern='test_array_creation.py', top_level_dir=None)
    math_function  = unittest.defaultTestLoader.discover(basePath + '/test_pnumpy', pattern='test_math_function.py',top_level_dir=None)
    logic_function = unittest.defaultTestLoader.discover(basePath + '/test_pnumpy', pattern='test_logic_function.py',top_level_dir=None)
    case_path = (basePath + '/test_basic_operations')
    dis = unittest.TestLoader()
    basicOperations = dis.discover(start_dir=case_path, pattern="test*.py", top_level_dir=None)
    return unittest.TestSuite((array_creation,math_function,logic_function,basicOperations))
def library_conf(env):
    fr = open("conf.yml")
    conf = yaml.load(fr)
    fr.close()
    fw = open('./taskrunner/conf.yml','w')
    fw.write("user: %s\n"%(conf.get(env).get("user")))
    fw.write("passwd: %s\n" % (conf.get(env).get("passwd")))
    fw.write("site: %s\n" % (conf.get(env).get("site")))
    fw.close()

def init(site=None,user=None,passwd=None,dbhost=None,dbport=None):

    tm_init(insite=site, inuser=user, inpasswd=passwd)
    db_init(ihost=dbhost, iport=dbport)
    login_init(insite=site, inuser=user, inpasswd=passwd)

def runsmoke(key=None,env='dev',timestr= None):
    logging.info("running smoke test suite")
    if timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    begintime = cur_utc_time()
    suit = unittest.TestSuite((tm_smoke_suit(), db_smoke_suit(),library_smoke_suit()))
    #suit = unittest.TestSuite((tm_smoke_suit()))
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (key, timestr))
    runner.run(suit)
    endtime = cur_utc_time()
    if env in ['master','ali']:
        if post_alarm("privpy-%s-%s" % (key, timestr), env=env):
            downlog(begintime=begintime,endtime=endtime,filename="./privpy-%s-%s/log.csv" % (key, timestr))

def runlocal(key=None,env='dev',timestr= None):
    logging.info("running smoke test suite")
    if timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    #begintime = cur_utc_time()
    suit = unittest.TestSuite((tm_smoke_suit(), db_smoke_suit(), library_smoke_suit()))
    # suit = unittest.TestSuite((tm_smoke_suit()))
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (key, timestr))
    runner.run(suit)
    #endtime = cur_utc_time()

def runregression(key=None,env='dev',timestr = None):
    logging.info("running regression test suite")
    if timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    begintime = cur_utc_time()
    suit = unittest.TestSuite((tm_smoke_suit(), db_smoke_suit()))
    #suit = unittest.TestSuite((tm_smoke_suit()))
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (key, timestr))
    runner.run(suit)
    endtime = cur_utc_time()
    if env in ['master','ali']:
        if post_alarm("privpy-%s-%s" % (key, timestr), env=env):
            downlog(begintime=begintime,endtime=endtime,filename="./privpy-%s-%s/log.csv" % (key, timestr))

def runheartbeat(key=None,env='dev',timestr= None):
    logging.info('running heartbeat test')
    if timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    begintime = cur_utc_time()
    if env == 'ali':
        suit = unittest.TestSuite(tm_smoke_suit())
    else:
        suit = unittest.TestSuite((tm_smoke_suit(),db_smoke_suit()))
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (key, timestr))
    runner.run(suit)
    endtime = cur_utc_time()
    if env in ['ali','master']:
        if post_alarm("privpy-%s-%s" % (key, timestr), env=env):
            downlog(begintime=begintime,endtime=endtime,filename="./privpy-%s-%s/log.csv" % (key, timestr))

def rundb(key=None,env='dev',timestr= None):
    logging.info('running db test')
    if timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    suit = db_smoke_suit()
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (key, timestr))
    runner.run(suit)

if __name__ == '__main__':
    #python3 runTestsuit.py --site=debugsaas-inspection.tsingj.local --user=user --passwd=123456 --key=inspection
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    sys.path.append(os.getcwd())
    parser = argparse.ArgumentParser()
    #parser.description("run job test")
    parser.add_argument("--env",help="environment,like: env192 ",type=str)
    parser.add_argument("--key",help="create jobs, key for data & code exa: 10M_double、1M_double、sql_stage")
    parser.add_argument("--Num",help="create Num jobs,default = 1",default=1)
    parser.add_argument("--time",help="report-${time}",type=str,default=None)
    parser.add_argument("--thread",help="Multithreading",type=int,default=1)

    parser.add_argument("--site",help="中台地址",type=str,default=None)
    parser.add_argument("--user",help="user",type=str,default=None)
    parser.add_argument("--passwd",help="passwd" ,type=str,default=None)
    conf_args = vars(parser.parse_args())
    run = HTMLReport.TestRunner(title="test",report_file_name="test.html")
    run.run(db_smoke_suit())
    raise 1
    #while 1:
    #   time.sleep(1)
    if conf_args.get("site") != None and conf_args.get("user") != None and conf_args.get("passwd") != None:
        init(site=conf_args.get('site'), user=conf_args.get('user'), passwd=conf_args.get('passwd'),
             dbhost=conf_args.get('dbhost'), dbport=conf_args.get('dbport'))
        fw = open('./taskrunner/conf.yml', 'w')
        fw.write("user: %s\n" % (conf_args.get("user")))
        fw.write("passwd: %s\n" % (conf_args.get("passwd")))
        fw.write("site: %s\n" % (conf_args.get("site")))
        fw.close()
        runlocal(key=conf_args.get("key"),env=conf_args.get("env"),timestr=conf_args.get("time"))
    else:
        fr = open('conf.yml')
        all_conf = yaml.load(fr)
        fr.close()
        conf = all_conf.get(conf_args.get("env")+'-'+conf_args.get("key"))
        init(site=conf.get('site'),user=conf.get('user'),passwd=conf.get('passwd'),
             dbhost=conf.get('dbhost'),dbport=conf.get('dbport'))
        library_conf(env=conf_args.get("env")+'-'+conf_args.get("key"))
        if conf_args.get("key") == 'smoke':
            runsmoke(key=conf_args.get('key'),env=conf_args.get('env'),timestr=conf_args.get('time'))
        if conf_args.get("key") == 'heartbeat':
            runheartbeat(key=conf_args.get('key'), env=conf_args.get('env'), timestr=conf_args.get('time'))

        if conf_args.get("key") == 'db':
            rundb(key=conf_args.get('key'),timestr=conf_args.get('time'),env=conf_args.get('env'))

        if conf_args.get("key") == 'regression':
            runregression(key=conf_args.get("key"),timestr=conf_args.get("time"),env=conf_args.get("env"))

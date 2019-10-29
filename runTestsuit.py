#!/user/bin/env python3
import unittest
import HTMLReport
import json
import argparse
import time
import yaml
import os
import sys
import re
import xmlrunner

from tm_cases import tmTestcases,get_job_csv
from tm_cases import tm_init
from dbengine_cases import dbengineCases
from dbengine_cases import db_init
from login_cases import loginTestcases
from login_cases import login_init
from alarm.alarm import post_alarm

import logging


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
    dbsomkesuit = unittest.TestLoader().loadTestsFromTestCase(dbengineCases)
    #dbsomkesuit = unittest.TestSuite()
    dbsomkesuit.addTest(dbengineCases("describe_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_filter_gold_case"))

    return dbsomkesuit

def createdata(length=1000):
    fw = open("data.csv",'w')
    for i in range(length):
        begin = i + 1
        for j in range(length):
            fw.write(str(begin))
            begin =  begin + 1
            fw.write(',')
        fw.write('\n')

if __name__ == '__main__':
    #python3 runTestsuit.py --site=debugsaas-inspection.tsingj.local --user=user --passwd=123456 --key=inspection
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    #parser.description("run job test")
    parser.add_argument("--env",help="environment,like: env192 ",type=str)
    parser.add_argument("--key",help="create jobs, key for data & code exa: 10M_double、1M_double、sql_stage")
    parser.add_argument("--Num",help="create Num jobs,default = 1",default=1)
    parser.add_argument("--time",help="report-${time}",type=str,default=None)
    parser.add_argument("--thread",help="Multithreading",type=int,default=1)
    args = vars(parser.parse_args())
    conf_args = args
    fr = open('conf.yml')
    all_conf = yaml.load(fr)
    fr.close()
    conf = all_conf.get(conf_args.get("env")+'-'+conf_args.get("key"))
    if conf_args.get("time") == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    else:
        timestr = conf_args.get("time")
    #get_job_csv(env=conf_args.get('env'),key=conf_args.get("key"))
    tm_init(insite=conf.get("site"),inuser=conf.get("user"),inpasswd=conf.get("passwd"),inenv=conf.get("csvfile"))
    db_init(ihost=conf.get("dbhost"),iport=conf.get("dbport"))
    login_init(insite=conf.get("site"),inuser=conf.get("user"),inpasswd=conf.get("passwd"))
    if conf_args.get("key") == 'smoke':
        logging.info("run smoke test suit")
        dbsuit = db_smoke_suit()
        tmsuit = tm_smoke_suit()
        suit = unittest.TestSuite((tmsuit,dbsuit))
    if conf_args.get("key") == 'heartbeat':
        suit = tm_smoke_suit()
    if conf_args.get("key") == 'db':
        logging.info("run db test  suit")
        suit = db_smoke_suit()

    if conf_args.get("key") in ['smoke','heartbeat']:
        runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s"%(conf_args.get("key"),timestr))
        runner.run(suit)
        #if conf_args.get("key") == 'heartbeat':
        post_alarm("privpy-%s-%s"%(conf_args.get("key"),timestr),env=conf_args.get("env"))
    else:
        runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (conf_args.get("key"), timestr))
        runner.run(suit)
        '''runner = HTMLReport.TestRunner(report_file_name='test',
                                       output_path='./',
                                       description='login test suite',
                                       thread_count=1,
                                       thread_start_wait=3,
                                       sequential_execution=False,
                                       lang='cn')
        runner.run(suit)
        '''
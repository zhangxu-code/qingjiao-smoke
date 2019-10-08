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

from tm_cases import tmTestcases
from tm_cases import tm_init
from dbengine_cases import dbengineCases
from dbengine_cases import db_init
from login_cases import loginTestcases
from login_cases import login_init

import logging


def login_suit():
    loginsuit = unittest.TestSuite()
    loginsuit.addTest(loginTestcases("logincase"))
    return loginsuit

def tm_suit():
    tmsuit = unittest.TestSuite()
    #tmsuit.addTest(tmTestcases("jobstart_pipline_case"))
    #tmsuit.addTest(tmTestcases("jobgetConfig_case"))
    tmsuit.addTest(tmTestcases("jobcreate_pipline"))
    return tmsuit

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
    smokesuit = unittest.TestSuite()
    smokesuit.addTest(loginTestcases("logincase_ok"))
    smokesuit.addTest(tmTestcases("jobcreate_cnnface_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_matrix_factor_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_logistic_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_query_gold_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_lstm_hsi_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_nn_mnist_pipline"))

    smokesuit.addTest(tmTestcases("jobcreate_100M_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_10M_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_1M_pipline"))
    smokesuit.addTest(tmTestcases("jobcreate_500M_pipline"))
    return smokesuit

def db_smoke_suit():
    dbsomkesuit = unittest.TestLoader().loadTestsFromTestCase(dbengineCases)
    #dbsomkesuit = unittest.TestSuite()
    dbsomkesuit.addTest(dbengineCases("describe_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_filter_gold_case"))
    #dbsomkesuit.addTest(dbengineCases("agg_gold_case"))
    #dbsomkesuit.addTest(dbengineCases("agg_ccode_gold_case"))
    #dbsomkesuit.addTest(dbengineCases("agg_limit_gold_case"))
    #dbsomkesuit.addTest(dbengineCases("agg_filter_gold_case"))
    dbsomkesuit.addTest(dbengineCases("sum_gold_price_case"))
    dbsomkesuit.addTest(dbengineCases("sum_gold_price_filter_case"))
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
    parser.add_argument("--site",help="env site",type=str)
    parser.add_argument("--user",help="user",type=str)
    parser.add_argument("--passwd",help="password",type=str)
    parser.add_argument("--dbhost",help="impala host",type=str)
    parser.add_argument("--dbport",help="impala port",type=int)
    parser.add_argument("--jobid",help="jobid,start jobs,split by ',' ",type=str)
    parser.add_argument("--key",help="create jobs, key for data & code exa: 10M_double、1M_double、sql_stage")
    parser.add_argument("--Num",help="create Num jobs,default = 1",default=1)
    args = vars(parser.parse_args())
    print(args)
    global conf_args
    conf_args = args
    '''
    createdata(100)
    '''
    #suit = login_suit()
    print(conf_args.get("key"))
    print(type(conf_args.get("key")))
    fr = open('conf.yml')
    all_conf = yaml.load(fr)
    fr.close()
    conf = all_conf.get(conf_args.get("env"))

    tm_init(insite=conf.get("site"),inuser=conf.get("user"),inpasswd=conf.get("passwd"))
    db_init(ihost=conf.get("dbhost"),iport=conf.get("dbport"))
    login_init(insite=conf.get("site"),inuser=conf.get("user"),inpasswd=conf.get("passwd"))
    if conf_args.get("key") == 'smoke':
        logging.info("run smoke test suit")
        dbsuit = db_smoke_suit()
        tmsuit = tm_smoke_suit()
        suit = unittest.TestSuite((tmsuit,dbsuit))

    else:
        suit  = inspection_suit()
    if conf_args.get("key") == 'smoke':
        runner = xmlrunner.XMLTestRunner(output="test.xml")
        runner.run(suit)
    else:
        runner = HTMLReport.TestRunner(report_file_name='test',
                                       output_path='./',
                                       description='login test suite',
                                       thread_count=1,
                                       thread_start_wait=3,
                                       sequential_execution=False,
                                       lang='cn')
        runner.run(suit)

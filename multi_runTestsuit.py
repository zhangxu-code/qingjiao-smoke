import os
import sys
from concurrent.futures import ProcessPoolExecutor
import time
import unittest
import logging
import yaml
from xmlrunnerR import xmlrunner
from absl import flags,app
'''
from tm_cases import tmTestcases,get_job_csv
from tm_cases import tm_init
from dbengine_cases import dbengineCases
from dbengine_cases import db_init
from login_cases import loginTestcases
from login_cases import login_init
from alarm.alarm import post_alarm
'''

FLAGS = flags.FLAGS
flags.DEFINE_string("env",'master','environment,default:master')
flags.DEFINE_string("timestr",None,help="report-mutli-${time}")
flags.DEFINE_integer("cycle",default=1,help='all case run N times')
'''
def tm_suit():
    tmsmokesuit = unittest.TestLoader().loadTestsFromTestCase(tmTestcases)
    #tmsmokesuit.addTest(loginTestcases("logincase_ok"))
    logging.info("get tm smoke cases")
    return tmsmokesuit

def db_suit():
    dbsomkesuit = unittest.TestLoader().loadTestsFromTestCase(dbengineCases)
    #dbsomkesuit = unittest.TestSuite()
    dbsomkesuit.addTest(dbengineCases("describe_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_gold_case"))
    dbsomkesuit.addTest(dbengineCases("hdfs_filter_gold_case"))

    return dbsomkesuit

'''
def process_executor(suite,executor,timestr):
    task = []
    for tmp in suite:
        task.append(executor.submit(runsuit,tmp,timestr))
    return task


def library_suit():
    pattern = "test*.py"
    case_path = os.getcwd() + '/privpy_library/privpy_lib/tests'
    discover = (unittest.defaultTestLoader.discover(case_path, pattern=pattern, top_level_dir=None))
    return discover

def library_smoke_suit(executor,timestr):
    task = []
    basePath = os.getcwd()+'/privpy_library/privpy_lib/tests'
    case_path = (basePath+'/test_pnumpy')
    array_creation = unittest.defaultTestLoader.discover(start_dir=case_path, pattern="test*.py", top_level_dir=None)
    task.extend(process_executor(array_creation,executor,timestr))

    print(array_creation.countTestCases())
    math_function  = unittest.defaultTestLoader.discover(basePath + '/test_pai', pattern="test*.py",top_level_dir=None)
    task.extend(process_executor(math_function, executor, timestr))
    print(math_function.countTestCases())

    pandas = unittest.defaultTestLoader.discover(basePath + '/test_ppandas', pattern="test*.py",top_level_dir=None)
    task.extend(process_executor(pandas, executor, timestr))
    print(pandas.countTestCases())

    psql = unittest.defaultTestLoader.discover(basePath + '/test_psql', pattern="test*.py", top_level_dir=None)
    task.extend(process_executor(psql, executor, timestr))
    print(psql.countTestCases())

    ptorch = unittest.defaultTestLoader.discover(basePath + '/test_ptorch', pattern="test*.py", top_level_dir=None)
    task.extend(process_executor(ptorch, executor, timestr))
    print(ptorch.countTestCases())
    return task

def library_conf(env):
    fr = open("conf.yml")
    conf = yaml.load(fr)
    fr.close()
    fw = open('./taskrunner/conf.yml','w')
    fw.write("user: %s\n"%(conf.get(env).get("user")))
    fw.write("passwd: %s\n" % (conf.get(env).get("passwd")))
    fw.write("site: %s\n" % (conf.get(env).get("site")))
    fw.close()

'''
def init(site=None,user=None,passwd=None,dbhost=None,dbport=None):

    tm_init(insite=site, inuser=user, inpasswd=passwd)
    db_init(ihost=dbhost, iport=dbport)
    login_init(insite=site, inuser=user, inpasswd=passwd)

'''
def runsuit(cases,output):
    logging.info("run suite")
    runner = xmlrunner.XMLTestRunner(output=output)
    runner.run(cases)

def conf(env):
    fr = open("conf.yml")
    conf = yaml.load(fr)
    fr.close()
    fw = open('./taskrunner/conf.yml','w')
    fw.write("user: %s\n"%(conf.get(env).get("user")))
    fw.write("passwd: %s\n" % (conf.get(env).get("passwd")))
    fw.write("site: %s\n" % (conf.get(env).get("site")))
    fw.close()


def main(argv):
    del argv
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if FLAGS.timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    else:
        timestr = FLAGS.timestr
    conf(env='%s-library' % (FLAGS.env))
    sys.path.append(os.getcwd())
    #suite = unittest.TestSuite((db_suit(),tm_suit(),library_suit()))

    basePath = os.getcwd() + '/privpy_library/privpy_lib/tests'
    case_path = (basePath + '/test_pnumpy')
    pattern = 'test*.py'
    discover = unittest.defaultTestLoader.discover(case_path, pattern=pattern, top_level_dir=None)
    print(discover.countTestCases())

    executor = ProcessPoolExecutor(max_workers=10)

    task = library_smoke_suit(executor,'privpy-library-'+timestr)

    while len(task) != 0:
        #print(task)
        logging.info("not finished %d"%(len(task)))
        for tmp in task:
            if tmp.done():
                print(tmp.result())
                task.remove(tmp)

        time.sleep(10)
    logging.info("all process is done")

    #runner = xmlrunner.XMLTestRunner(output="privpy-library-%s" % (timestr))
    #runner.run(discover)

if __name__ == '__main__':
    app.run(main)
import os
import sys
from absl import flags,app
import unittest
import re
from taskrunner.taskRunner_api import TaskRunnerAPI
from xmlrunnerR import xmlrunner
import yaml
#import logger
import time
from loguru import logger
logger.add('./logs/log-{time}.log')
logger.add(sys.stderr,format="{time} {level} {message}",filter="./logs/log")
logger.add(sys.stdout,format="{time} {level} {message}",filter="./logs/log")



FLAGS = flags.FLAGS
flags.DEFINE_string('package', "pnumpy", 'the package to test')
flags.DEFINE_string('module', "", 'the module to test, default is all module in a package')
flags.DEFINE_string("env","master",'environment,default:master')
flags.DEFINE_string("timestr",None,help="report-library-${time}")

def conf(env):
    fr = open("conf.yml")
    conf = yaml.load(fr)
    fr.close()
    fw = open('./taskrunner/conf.yml','w')
    fw.write("user: %s\n"%(conf.get(env).get("user")))
    fw.write("passwd: %s\n" % (conf.get(env).get("passwd")))
    fw.write("site: %s\n" % (conf.get(env).get("site")))
    fw.write("queue: %s"%(conf.get("timestr")))
    fw.close()

def loadAll_suite():
    basePath = os.getcwd() + '/privpy_library/privpy_lib/tests'
    alldirs = os.listdir(basePath)
    case_dir = re.compile(r'^test_[a-zA-Z0-9_]+$')
    cases = [] #unittest.TestSuite()
    for tmpdir in alldirs:
        #logger.info(tmpdir)
        if re.search(case_dir,tmpdir):
            logger.info(tmpdir)
            if "test_ppandas" == tmpdir:
                logger.info("skip "+tmpdir)
                continue
            try:
                dis = unittest.TestLoader()
                cases.append( dis.discover(start_dir=basePath+"/"+tmpdir, pattern="test*.py", top_level_dir=None))
                #cases = unittest.TestSuite(())
            except Exception as err:
                logger.error(err)

    return unittest.TestSuite(cases)


def main(argv):
    del argv
    logger.info("%s run %s:%s time:%s"%(FLAGS.env,FLAGS.package,FLAGS.module,FLAGS.timestr))
    conf(env='%s-library'%(FLAGS.env))
    if FLAGS.timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    else:
        timestr = FLAGS.timestr
    sys.path.append(os.getcwd())
    case_path = os.getcwd() + '/privpy_library/privpy_lib/tests/test_' + FLAGS.package
    if FLAGS.package == "all":
        discover = loadAll_suite()
    else:
        if FLAGS.module == "":
            pattern = "test*.py"
        else:
            pattern = "test_" +  FLAGS.module +".py"
        print(case_path)
        print(pattern)
        discover = unittest.defaultTestLoader.discover(case_path, pattern=pattern, top_level_dir=None)
    print(type(discover))
    print(discover.countTestCases())


    runner = xmlrunner.XMLTestRunner(output="privpy-library-%s" % (timestr))
    runner.run(discover)

if __name__ == "__main__":
    app.run(main)

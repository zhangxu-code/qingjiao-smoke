import os
import sys
from absl import flags,app
import unittest
from taskrunner.taskRunner_api import TaskRunnerAPI
import xmlrunner
import yaml
import logging
import time
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

def main(argv):
    del argv

    conf(env='%s-library'%(FLAGS.env))
    if FLAGS.timestr == None:
        timestr = time.strftime("%Y%m%d%H%M%S")
    else:
        timestr = FLAGS.timestr
    sys.path.append(os.getcwd())
    case_path = os.getcwd() + '/privpy_library/privpy_lib/tests/test_' + FLAGS.package
    if FLAGS.module == "":
        pattern = "test*.py"
    else:
        pattern = "test_" +  FLAGS.module +".py"
    discover = unittest.defaultTestLoader.discover(case_path, pattern=pattern, top_level_dir=None)
    #runner = unittest.TextTestRunner(stream=None, descriptions=None, verbosity=2)
    runner = xmlrunner.XMLTestRunner(output="privpy-library-%s" % (timestr))
    runner.run(discover)

if __name__ == "__main__":
    app.run(main)
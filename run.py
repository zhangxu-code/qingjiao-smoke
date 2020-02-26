#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketestbak
Author  zhenghongguang
Date    2020-02-22
"""

import os
from xmlrunnerR import xmlrunner
from loguru import logger
import unittest
import time
import json
from absl import flags,app
from finder import caseFinder

FLAGS = flags.FLAGS
flags.DEFINE_string(name="args",default='{"site":"console-dev.tsingj.local","user":"smoketest","passwd":"qwer1234","key":"test"}',help="json args")

def main(argv):
    # caseFinder.find_bypath(os.getcwd() + '/console_api')
    if FLAGS.args:
        conf = json.loads(FLAGS.args)
        os.environ["consolesite"] = conf.get("site")
        os.environ["consoleuser"] = conf.get("user")
        os.environ["consolepasswd"] = conf.get("passwd")
        key = conf.get("key")
        timestr = time.strftime("%Y%m%d%H%M%S")
    finder = caseFinder.casefinder()
    suite = finder.findcases_bypath(path=os.getcwd() + '/console_api', key='test')
    #logger.info(finder.find_bypath(path=os.getcwd() + '/console_api'))
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (key, timestr))
    runner.run(suite)

if __name__ == '__main__':
    app.run(main)


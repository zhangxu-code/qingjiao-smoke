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
flags.DEFINE_string(name="args",
                    default=None,
                    help="json args")
flags.DEFINE_string(name="json", default=None, help="read env from json")
flags.DEFINE_string(name="timestr", default=None, help=None)
flags.DEFINE_string(name="type", default="test", help=None)

def main(argv):
    # caseFinder.find_bypath(os.getcwd() + '/console_api')
    if FLAGS.args:
        conf = json.loads(FLAGS.args)
        os.environ["consolesite"] = conf.get("site")
        os.environ["consoleuser"] = conf.get("user")
        os.environ["consolepasswd"] = conf.get("passwd")
        os.environ["csvfiles"] = conf.get("csvfiles")
        os.environ["namespace"] = conf.get("namespace")
        os.environ["path"] = conf.get("path")
        if conf.get("key"):
            os.environ["key"] = conf.get("key")
    elif FLAGS.json:
        fr = open(FLAGS.json)
        conf = json.load(fr)
        fr.close()
        for key in conf.keys():
            os.environ[key] = conf.get(key)
        '''
        os.environ["consolesite"] = conf.get("site")
        os.environ["consoleuser"] = conf.get("user")
        os.environ["consolepasswd"] = conf.get("passwd")
        os.environ["csvfiles"] = conf.get("csvfiles")
        os.environ["namespace"] = conf.get("namespace")
        os.environ["path"] = conf.get("path")
        if conf.get("key"):
            os.environ["key"] = conf.get("key")
        '''

    print(os.environ)
    if FLAGS.timestr:
        timestr = FLAGS.timestr
    else:
        timestr = time.strftime("%Y%m%d%H%M%S")
    finder = caseFinder.casefinder()
    suit_list = []
    for path in os.environ["path"].split(','):
        suit_list.append(finder.findcases_bypath(path=os.getcwd() + "/" + path, key=os.getenv("key")))
    #logger.info(finder.find_bypath(path=os.getcwd() + '/console_api'))
    suite = unittest.TestSuite(suit_list)
    logger.info(suite.countTestCases())
    logger.info(suite)
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (FLAGS.type, timestr))
    runner.run(suite)

if __name__ == '__main__':
    app.run(main)


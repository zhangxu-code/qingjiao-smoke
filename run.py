#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project integration
Author  zhenghongguang
Date    2020-03-09
"""


import os
import unittest
import time
import json
from loguru import logger
from absl import flags, app
from finder import caseFinder
from xmlrunnerR import xmlrunner

FLAGS = flags.FLAGS
flags.DEFINE_string(name="args",
                    default=None,
                    help="json args")
flags.DEFINE_string(name="json", default=None, help="read env from json")
flags.DEFINE_string(name="timestr", default=None, help=None)
flags.DEFINE_string(name="type", default="test", help=None)

def main(argv):
    """
    main
    :param argv:
    :return:
    """
    if FLAGS.args:
        conf = json.loads(FLAGS.args)
        for key in conf.keys():
            os.environ[key] = conf.get(key)
    elif FLAGS.json:
        reader = open(FLAGS.json)
        conf = json.load(reader)
        reader.close()
        for key in conf.keys():
            os.environ[key] = conf.get(key)
    print(os.environ)
    if FLAGS.timestr:
        timestr = FLAGS.timestr
    else:
        timestr = time.strftime("%Y%m%d%H%M%S")
    finder = caseFinder.casefinder()
    suit_list = []
    for path in os.environ["path"].split(','):
        suit_list.append(finder.findcases_bypath(path=
                                                 os.getcwd() + "/" + path, key=os.getenv("key")))
    #logger.info(finder.find_bypath(path=os.getcwd() + '/console_api'))
    suite = unittest.TestSuite(suit_list)
    logger.info(suite.countTestCases())
    logger.info(suite)
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (FLAGS.type, timestr))
    runner.run(suite)


if __name__ == '__main__':
    app.run(main)

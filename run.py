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
                    default='{"site":"console-dev.tsingj.com","user":"heartbeat","passwd":"qwer1234","path":"job","csvfiles":"heartbeat_ali_metaid.csv"}',
                    help="json args")
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
        key = conf.get("key")
    if FLAGS.timestr:
        timestr = FLAGS.timestr
    else:
        timestr = time.strftime("%Y%m%d%H%M%S")

    finder = caseFinder.casefinder()
    suite = finder.findcases_bypath(path=os.getcwd() + "/" + json.loads(FLAGS.args).get("path"), key=json.loads(FLAGS.args).get("key"))
    #logger.info(finder.find_bypath(path=os.getcwd() + '/console_api'))
    logger.info(suite)
    runner = xmlrunner.XMLTestRunner(output="privpy-%s-%s" % (json.loads(FLAGS.args).get("type"), timestr))
    runner.run(suite)

if __name__ == '__main__':
    app.run(main)


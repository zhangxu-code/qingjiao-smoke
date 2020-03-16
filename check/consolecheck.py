#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-03-10
"""

import os
import time
import json
import requests
from loguru import logger
from absl import flags, app

FLAGS = flags.FLAGS
flags.DEFINE_string(name="args",
                    default=None,
                    help="json args")
flags.DEFINE_string(name="json", default=None, help="read env from json")

def check_login(site, user, passwd):
    """
    check console login
    :return:
    """
    logger.info("check login")
    try_count = 0
    while try_count < 90:
        try:
            logger.info("username=%s&password=%s" % (user, passwd))
            response = requests.post(url="%s/api/api-sso/token/simpleLogin" % site,
                                    params="username=%s&password=%s" % (user, passwd),
                                    verify=False)
            logger.info(response.text)
            token = response.json().get("data").get("access_token")
            if token:
                return token
        except Exception as err:
            logger.error(err)
        try_count = try_count + 1
        logger.info(try_count)
        time.sleep(10)
    return False

def check_task(token, site):
    """
    try start task 1
    :param token:
    :return:
    """
    logger.info("check task run")
    head = {"Authorization": "bearer %s" % token,"Content-Type":"application/json"}
    try_count = 0


    jsbody = {
        "code": '''
import privpy as pp
import os
import sys
sys.path.append(os.getcwd() + '/privpy_lib')

import pnumpy as pnp
s1 = pnp.zeros_like(pp.iarr([[0, 1, 2],
       [3, 4, 5]]))
    ''',
        "name": "checktest"
    }
    response = requests.post(url="%s/api/api-tm/v1/task" % site,
                             data=json.dumps(jsbody),
                             headers=head,
                             verify=False)
    logger.info(response.text)
    taskid = response.json().get("data").get("id")
    response = requests.put(url="%s/api/api-tm/v1/task/start/%d" % (site, taskid),
                            headers=head,
                            verify=False)
    logger.info(response.text)

    while try_count < 90:
        try:
            response = requests.get(url="%s/api/api-tm/v1/task/%d" % (site, taskid),
                                    headers=head,
                                    verify=False)
            logger.info(response.text)
            if response.json().get("data").get("queueStatus") == 6:
                return True
            elif response.json().get("data").get("queueStatus") != 2 and \
                response.json().get("data").get("queueStatus") != 4:

                response = requests.put(url="%s/api/api-tm/v1/task/start/%d" % (site, taskid),
                                        headers=head,
                                        verify=False)
                logger.info(response.text)
        except Exception as err:
            logger.error(err)
            return False
        try_count = try_count + 1
        time.sleep(10)
    return False

def main(argv):
    """
    main
    :param argv:
    :return:
    """
    if FLAGS.args:
        conf = json.loads(FLAGS.args)
        os.environ["consolesite"] = conf.get("consolesite")
        os.environ["consoleuser"] = conf.get("consoleuser")
        os.environ["consolepasswd"] = conf.get("consolepasswd")
    elif FLAGS.json:
        reader = open(FLAGS.json)
        conf = json.load(reader)
        reader.close()
        for key in conf.keys():
            os.environ[key] = conf.get(key)

    token = check_login(site=os.getenv("consolesite"),
                        user=os.getenv("consoleuser"),
                        passwd=os.getenv("consolepasswd"))
    if token is False:
        exit(1)
    if check_task(token=token, site=os.getenv("consolesite")) is False:
        exit(1)

if __name__ == '__main__':
    app.run(main)

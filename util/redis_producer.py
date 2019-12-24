import os
import sys
import redis
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import json
import yaml
import time

class redis_producer:

    def __init__(self):
        try:
            fr = open("./util/conf.yml")
            self.conf = yaml.load(fr)
            fr.close()
            self.queue = self.conf.get("queue")
            self.r = redis.Redis(host=self.conf.get("host"),port=self.conf.get("port"),password=self.conf.get("passwd"))
        except Exception as err:
            logger.error(err)

    def producer(self,msg):
        try:
            logger.info(msg)
            self.r.lpush(self.queue,(msg))
        except Exception as err:
            logger.error(err)

    def pub_performace(self,msg):
        if isinstance(msg,dict):
            if "runtime" not in msg.keys():
                msg["runtime"] = self.queue
            pubmsg = json.dumps(msg)
        else:
            pubmsg = msg
        logger.info("pub %s" % (pubmsg))
        self.r.publish("performance",pubmsg)

def fun():
    produc = redis_producer()
    produc.producer("hello")

if __name__ == '__main__':
    task = []
    logger.info("begin")
    '''
    executor = ThreadPoolExecutor(max_workers=50)
    for i in range(50):
        executor.submit(fun)

    while task != []:
        for tmp in task:
            if tmp.done():
                task.remove(tmp)
    '''
    logger.info("end")
    pub = redis_producer()
    while 1:
        pub.pub_performace(msg = {"name":"test","time":100})
        time.sleep(3)
    #produc = redis_producer()
    #produc.producer("hello")

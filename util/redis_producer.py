import os
import sys
import redis
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import json
import yaml

class redis_producer:

    def __init__(self):
        try:
            fr = open("./util/conf.yml")
            conf = yaml.load(fr)
            fr.close()
            self.queue = conf.get("queue")
            self.r = redis.Redis(host=conf.get("host"),port=conf.get("port"),password=conf.get("passwd"))
        except Exception as err:
            logger.error(err)

    def producer(self,msg):
        try:
            logger.info(msg)
            self.r.lpush(self.queue,(msg))
        except Exception as err:
            logger.error(err)

def fun():
    produc = redis_producer()
    produc.producer("hello")

if __name__ == '__main__':
    task = []
    logger.info("begin")
    executor = ThreadPoolExecutor(max_workers=50)
    for i in range(50):
        executor.submit(fun)

    while task != []:
        for tmp in task:
            if tmp.done():
                task.remove(tmp)
    logger.info("end")
    #produc = redis_producer()
    #produc.producer("hello")

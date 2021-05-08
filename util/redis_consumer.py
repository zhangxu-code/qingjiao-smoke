import os
import sys
import redis
import json
from loguru import logger
import yaml
import numpy
import math

class redis_consumer:
    data     = {}
    data_err = {}
    queue = None
    def __init__(self):
        fr = open('./util/conf.yml')
        conf = yaml.load(fr)
        fr.close()
        logger.info(conf)
        self.queue = conf.get("queue")
        self.r = redis.Redis(host=conf.get("host"),port=conf.get("port"),password=conf.get("passwd"))

    def delete(self):
        self.r.delete(self.queue)

    def consumer(self):
        length = (self.r.llen(self.queue))
        index = 0
        count = 0
        while index < length:
            if index + 100 < length:
                next = index + 100
            else:
                next = length
            data = self.r.lrange(self.queue,index,next)
            for tmp in data:
                tmp_js = json.loads(bytes.decode(tmp))
                if tmp_js.get("isOK"):
                    if tmp_js.get("api") in self.data.keys():
                        self.data[tmp_js.get("api")].append(tmp_js.get("time"))
                    else:
                        self.data[tmp_js.get("api")] = [tmp_js.get("time")]
                else:
                    if tmp_js.get("api") in self.data_err.keys():
                        self.data_err[tmp_js.get("api")] += 1
                    else:
                        self.data_err[tmp_js.get("api")] = 1
            #logger.info(data)
            index = next+1
            count += len(data)
        logger.info(count)
        logger.info(length)

    def _find(self,arr,data):
        max = None
        for tmp in arr:
            if tmp < data:
                if max == None:
                    max = tmp
                elif max < tmp:
                    max = tmp
        return  max

    def X_Line(self,arr):
        arr.sort()
        length = len(arr)
        Line_90 = arr[math.ceil(length*90/100)-1]
        Line_95 = arr[math.ceil(length * 95 / 100) - 1]
        Line_99 = arr[math.ceil(length * 99 / 100) - 1]
        return Line_90,Line_95,Line_99

    def analysis(self):
        Aggregated_report = {}
        for key in self.data.keys():
            Aggregated_report[key] = {}
            Aggregated_report[key]["count"]   = len(self.data.get(key))
            Line_90,Line_95,Line_99 = self.X_Line(self.data.get(key))
            Aggregated_report[key]["90%(ms)"] = Line_90
            Aggregated_report[key]["95%(ms)"] = Line_95
            Aggregated_report[key]["99%(ms)"] = Line_99
            Aggregated_report[key]["max(ms)"] = max(self.data.get(key))
            Aggregated_report[key]["error(%)"] = -1
        for key in self.data_err.keys():
            if key not in self.data.keys():
                Aggregated_report[key]["error(%)"] = 100
                Aggregated_report[key]["90%(ms)"] = -1
                Aggregated_report[key]["95%(ms)"] = -1
                Aggregated_report[key]["99%(ms)"] = -1
                Aggregated_report[key]["max(ms)"] = -1
            else:
                Aggregated_report[key]["error(%)"] = self.data_err.get(key)*100.0/(self.data_err.get(key)+len(self.data.get(key)))

        return Aggregated_report

if __name__ == '__main__':
    consumer = redis_consumer()
    consumer.consumer()
    print(consumer.analysis())
    #consumer.delete("library")
import requests
import csv
import os
import time
import datetime
import pytz
from loguru import logger
from retrying import retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import uuid
import json

class es2csv:
    host = ''
    port = 9200
    user = 'admin'
    passwd = 'qwer1234'
    query = ''
    def __init__(self,host,port=9200,user='admin',passwd='qwer1234'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
    def query_time(self,begintime,endtime):
        self.query = {
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match": {
                                "kubernetes.namespace_name": "privpy-master"
                              }
                            },
                            {
                              "range": {
                                "@timestamp": {
                                  "gte": begintime,
                                  "lte": endtime
                                }
                              }
                            }
                          ]
                        }
                      },
                     "sort": [
                            {
                                "@timestamp": {
                                    "order": "desc"
                                }
                            }
                        ]
                    }
    def write_data_2_csv(self,hits):
        fw = open('log.csv','a+')
        csvwriter = csv.writer(fw)
        for log in hits:
            row = []
            row.append(log.get("_id"))
            row.append(log.get("_source").get("time"))
            row.append(log.get("_source").get("stream"))
            row.append(log.get("_source").get("container_name"))
            row.append(log.get("_source").get("log"))
            csvwriter.writerow(row)
    @retry(stop_max_attempt_number=3)
    def scroll(self):
        head = {
            "Content-Type": "application/json"
        }
        try:
            #https://admin:qwer1234@10.18.0.18:9200/_search
            scroll_id = str(uuid.uuid1())
            url = 'https://%s:%s@%s:%d/k8s-*/_search?scroll=10m'%(self.user,self.passwd,self.host,int(self.port))
            req = requests.post(url=url,headers=head,verify = False,data=json.dumps(self.query))
            #print(req.text)
            self.write_data_2_csv(req.json().get("hits").get("hits"))
            return req.json().get("_scroll_id")
        except Exception as err:
            logger.error(err)
            logger.error(req.text)
            raise IOError("requests failed:%s :%s"%(url,str(err)))

    @retry(stop_max_attempt_number=3)
    def search(self,scroll_id):
        url = 'https://%s:%s@%s:%d/_search/scroll'%(self.user,self.passwd,self.host,int(self.port))
        body = {
            "scroll" : "10m",
            "scroll_id":scroll_id
        }
        head = {
            "Content-Type": "application/json"
        }
        while 1:
            try:
                req = requests.get(url=url,params=body,verify=False)

                if len(req.json().get("hits").get("hits")) == 0:
                    logger.info("download success")
                    return  True
                self.write_data_2_csv(req.json().get("hits").get("hits"))
            except Exception as err:
                logger.info(req.text)
                logger.error(err)
                raise  IOError("requests failed:%s :%s"%(url,str(err)))

if __name__ == '__main__':
    es = es2csv(host="10.18.0.18")
    utc_tz = pytz.timezone('UTC')
    curtime = datetime.datetime.now(tz=utc_tz)
    print(curtime)
    print(curtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

    es.query_time(begintime="2019-11-06T08:37:05.285552Z",endtime=curtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
    scrollid = es.scroll()
    print(scrollid)
    es.search(scroll_id=scrollid)


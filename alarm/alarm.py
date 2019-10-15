import os
import sys
import json
import  requests
import  xml.etree.ElementTree as ET
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def readjunitxml(xmlpath):
    logging.info("parser "+xmlpath)
    tree = ET.ElementTree(file=xmlpath)
    testsuit_dict = {}
    testsuit = tree.getroot()
    #print(testsuit.tag)
    #print(testsuit.attrib)
    tmp_root = {}
    tmp_root["errors"]   = int(testsuit.attrib["errors"])
    tmp_root["failures"] = int(testsuit.attrib["failures"])
    tmp_root["tests"]    = int(testsuit.attrib["tests"])
    tmp_root["name"]     = testsuit.attrib["name"]
    tmp_root["time"]     = float(testsuit.attrib["time"])
    testsuit_dict["root"] = tmp_root
    testsuit_dict["cases"] = []
    for case in testsuit:
        tmp_case = {}
        if case.tag != "testcase":
            continue
        tmp_case["name"] = case.attrib["name"]
        tmp_case["time"] = case.attrib["time"]
        if len(case) == 0:
            tmp_case["status"] = True
        else:
            tmp_case["status"] = False
            tmp_case["msg"] = case[0].attrib.get("message")
            tmp_case["error"] = (case[0].text)
        testsuit_dict["cases"].append(tmp_case)
    return testsuit_dict

def listxmlfile_dir(dir):
    files = os.listdir(dir)
    report = {}
    report["testsuit"] = []
    tmproot = {
        "errors":0,
        "failures":0,
        "tests":0,
        "time":0,
        "name":'smoketest'
    }
    for xmlfile in files:
        #print(os.path.splitext(xmlfile))
        if os.path.splitext(xmlfile)[1] == ".xml":
            xmlreport =  readjunitxml(dir+"/"+xmlfile)
            #print(xmlreport.get("root"))
            tmproot["errors"] = tmproot["errors"] + xmlreport.get("root").get("errors")
            tmproot["failures"] = tmproot["failures"] + xmlreport.get("root").get("failures")
            tmproot["tests"] = tmproot["tests"] + xmlreport.get("root").get("tests")
            tmproot["time"] = tmproot["time"] + xmlreport.get("root").get("time")
            report["testsuit"].append(xmlreport)
    report["root"] = tmproot
    return report

def postalarm(msg=None):
    head = {
        "Content-Type":"application/json",
        "charset":"utf-8"
    }
    body = {
        "moduleId":"1",
        "username":"qa",
        "desc":msg
    }
    logging.info(json.dumps(body))
    logging.info(json.dumps(head))
    try:
        req = requests.post(url="http://talert-dev.tsingj.local/api/warning",headers=head,data=json.dumps(body))
        logging.info("post alarm")
        logging.info(req.text)
    except Exception as err:
        logging.info("failed")
        logging.error(err)

def post_alarm(xmlpath):
    report = listxmlfile_dir(dir=xmlpath)
    #print(report)

    if report.get("root").get("errors") + report.get("root").get("failures") != 0:
        logging.error("run heartbeat failed ,post alarm")
        msg = ''
        for suit in report.get("testsuit"):
            for case in suit.get("cases"):
                if case.get("status") == False:
                    msg = msg +case.get("name")+":"+ case.get("msg")+";"
        logging.error(msg)
        postalarm(msg=msg)

if __name__ == '__main__':
    post_alarm(xmlpath='../privpy-heartbeat-20191015140230')
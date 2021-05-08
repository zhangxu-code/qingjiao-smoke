import os
import sys
import re
import  xml.etree.ElementTree as ET
import logging

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
            tmp_case["msg"]   = case[0].attrib.get("message")
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
            report["testsuit"].append(xmlreport.get("root"))
    report["root"] = tmproot
    return report

def listreport_path(path,last_read_time = 0):
    re_reportdir = re.compile(r'report-([0-9]{14})')
    reportdir = os.listdir(path)
    for tmpdir in reportdir:
        if re.search(re_reportdir,tmpdir):
            if int(re.search(re_reportdir,tmpdir).group(1)) > last_read_time:
                report = listxmlfile_dir(path+"/"+tmpdir)
                print(report.get("root"))
                print(report.get("testsuit"))




if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    #print(readjunitxml("./report/report-20191009143943/TEST-dbengine_cases.dbengineCases-20191009143946.xml"))
    listreport_path("../report")

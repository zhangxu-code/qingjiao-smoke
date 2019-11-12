import os
import sys
import json
import re
import requests
import logging
from prettytable import PrettyTable
from prettytable import from_html
from readxml import listxmlfile_dir
import datetime
import time

from absl import flags,app
FLAGS = flags.FLAGS
flags.DEFINE_string("report",None,"report dir")
flags.DEFINE_string("buildid",None,"jenkins Build ID")

BASE_URL = "http://wiki.tsingj.local/rest/api/content"
VIEW_URL = "http://wiki.tsingj.local/pages/viewpage.action?pageId="
def get_page_info(auth, pageid):

    url = '{base}/{pageid}'.format(
        base = BASE_URL,
        pageid = pageid)

    r = requests.get(url, auth = auth)

    r.raise_for_status()

    return r.json()

def get_page_body(auth,pageid):
    url = '{base}/{pageid}?expand=body.view'.format(
        base=BASE_URL,
        pageid=pageid)
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json()

def get_page_table(auth,pageid):
    logging.info("get page table")
    pagebody = get_page_body(auth=auth,pageid=pageid)
    #print(pagebody)
    value = (pagebody.get("body").get("view").get("value"))
    table_re = re.compile(r'(<table[\d\D]+<\/table>)')
    table = re.search(table_re,value)
    return (table.group(1))

def get_page_lable(auth,pageid):
    url = '{base}/{pageid}/labels'.format(
        base=BASE_URL,
        pageid=pageid)

    r = requests.get(url, auth=auth)

    r.raise_for_status()

    return r.json()

def get_page_ancestors(auth, pageid):

    # Get basic page information plus the ancestors property

    url = '{base}/{pageid}?expand=ancestors'.format(
        base = BASE_URL,
        pageid = pageid)

    r = requests.get(url, auth = auth)

    r.raise_for_status()

    return r.json()['ancestors']

def smoke_report_table():
    data = ["201909090909",10,1,2,3]

    table = PrettyTable(["执行时间","用例总数","成功","失败","异常"])
    table.add_row(data)
    print(table.attributes)
    return  table.get_html_string()

def write_table(auth,pageid,html,title=None):
    info = get_page_info(auth, pageid)

    ver = int(info['version']['number']) + 1

    ancestors = get_page_ancestors(auth, pageid)

    anc = ancestors[-1]
    del anc['_links']
    del anc['_expandable']
    del anc['extensions']

    if title is not None:
        info['title'] = title

    data = {
        'id': str(pageid),
        'type': 'page',
        'title': info['title'],
        'version': {'number': ver},
        'ancestors': [anc],
        'body': {
            'storage':
                {
                    'representation': 'storage',
                    'value': str(html),
                }
        }
    }

    data = json.dumps(data)

    url = '{base}/{pageid}'.format(base=BASE_URL, pageid=pageid)

    r = requests.put(
        url,
        data=data,
        auth=auth,
        headers={'Content-Type': 'application/json'}
    )
    r.raise_for_status()
    logging.info("Wrote '%s' version %d" % (info['title'], ver))
    logging.info("URL: %s%s" % (VIEW_URL, str(pageid)))

def runtime(runtime):
    tmptime = time.strptime(str(runtime),"%Y%m%d%H%M%S")
    #print(tmptime)
    return time.strftime("%Y-%m-%d %H:%M:%S",tmptime)

def main(argv):
    del argv
    if FLAGS.report == None:
        logging.info("need report dir")
    logging.info("add report")
    user   = 'zhenghongguang'
    passwd = 'Light1019'
    auth = (user,passwd)
    pageid = '18645695'
    table_html = get_page_table(auth=auth,pageid=pageid)
    table = from_html(table_html)
    report_xml = listxmlfile_dir(FLAGS.report)
    new_row = []
    #runtime = FLAGS.report.split('-')[-1]
    new_row.append(runtime(FLAGS.report.split('-')[-1]))
    new_row.append(report_xml.get("root").get("tests"))
    new_row.append(int(report_xml.get("root").get("tests")) - int(report_xml.get("root").get("failures"))-int(report_xml.get("root").get("errors")))
    new_row.append(report_xml.get("root").get("failures"))
    new_row.append(report_xml.get("root").get("errors"))
    new_row.append(report_xml.get("root").get("time"))
    new_row.append("http://jenkins.tsingj.local/job/QA/job/smoketest/%s/testReport/"%(str(FLAGS.buildid)))
    new_row.append("10.18.0.18::gfs/qa/smoketest/"+FLAGS.report)
    table[0].add_row(new_row)
    write_table(auth=auth,pageid=pageid,html=table[0].get_html_string(),title='报告汇总')

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    app.run(main)
    #print(runtime("20191023101010"))

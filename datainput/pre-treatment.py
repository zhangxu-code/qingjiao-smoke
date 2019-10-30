import os
import csv
import sys
import re
import logging
from absl import flags,app
FLAGS = flags.FLAGS
flags.DEFINE_string("type",None,'type = job or type = dbsql')
flags.DEFINE_string('env',None,'env = master or dev')
flags.DEFINE_string('key',None,'key = smoke or heartbeat')

def get_job_csv(env='master',key=None):
    #global env
    logging.info(env)
    value_rows = []
    csvfiles = os.listdir('./datainput/tm/')
    csvtype = re.compile(r'_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)\.csv')
    fw = open('./datainput/tm/jobs.csv','w')
    writer = csv.writer(fw)
    fw.write('title,datasource,result,code\n')
    for csv_name in csvfiles:
        print(csv_name)
        if not re.search(csvtype,csv_name):
            logging.info('skip '+csv_name)
            continue
        csvfind = re.search(csvtype,csv_name)
        if key == None:
            local_csv_name = csvfind.group(2)
            expect_csv_name = env
        else:
            local_csv_name = csvfind.group(1)+'_'+csvfind.group(2)
            expect_csv_name = key+'_'+env

        if local_csv_name != expect_csv_name:
            logging.info('skip '+csv_name)
            continue
        logging.info("read job info "+csv_name)
        fr = open('./datainput/tm/'+csv_name)
        csvfile = csv.reader(fr)
        next(csvfile)
        # csvfile = csv.DictReader(fr)
        for row in csvfile:
            writer.writerow(row)
        fr.close()
    fw.close()

def get_db_csv(env='master',key=None):
    logging.info("db sql csv %s:%s"%(env,key) )
    csvfiles = os.listdir('./datainput/db/')
    csvtype = re.compile(r'_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)\.csv')
    fw = open('./datainput/db/dbsql.csv','w')
    writer = csv.writer(fw)
    writer.writerow(['title','sql','expect'])
    for csv_name in csvfiles:
        if not re.search(csvtype,csv_name):
            logging.info('skip '+csv_name)
            continue
        csvfind = re.search(csvtype,csv_name)
        if key == None:
            local_csv_name = csvfind.group(2)
            expect_csv_name = env
        else:
            local_csv_name = csvfind.group(1) + '_' + csvfind.group(2)
            expect_csv_name = key + '_' + env

        if local_csv_name != expect_csv_name:
            logging.info('skip ' + csv_name)
            continue
        logging.info("read job info " + csv_name)
        fr = open('./datainput/db/' + csv_name)
        csvfile = csv.reader(fr)
        next(csvfile)
        # csvfile = csv.DictReader(fr)
        for row in csvfile:
            writer.writerow(row)
        fr.close()
    fw.close()
def main(argv):
    del argv
    print(FLAGS.env)
    print(FLAGS.type)
    print(FLAGS.key)
    if FLAGS.type =='job':
        get_job_csv(env=FLAGS.env,key=FLAGS.key)
    if FLAGS.type == 'dbsql':
        get_db_csv(env=FLAGS.env,key=FLAGS.key)

if __name__ == '__main__':
    app.run(main)
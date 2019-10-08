# -*- coding: UTF-8 -*-

from impala.dbapi import connect
from impala.util import log
import logging
#logging.basicConfig(level=logging.INFO)

class dbengine:
    cur = None
    conn = None

    def __init__(self,host = '10.18.0.19',port=21050):
        #logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        self.conn = connect(host=host,port=port,database='default')

        self.cur = self.conn.cursor()

    def describe_table(self,tablename):
        self.cur.execute("describe %s"%(tablename))
        print(self.cur.fetchall())

    def execut_sql(self,sql):
        self.cur.execute(sql)
        logging.info(sql)
        return self.cur.fetchall()

    def __del__(self):
        self.cur.close()
        self.conn.close()



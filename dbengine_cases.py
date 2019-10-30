#from .db.dbengine import dbengine
#from dbimpala.dbengine import dbengine
from dbimpala.dbengine import dbengine
import unittest
import HTMLReport
import logging
import csv
import ddt
# table gold info: {'len': 1940, 'price_Count': 7816861, 'price_Count_1000': 37}
# price_Count_1000： count(price < 1000) == 37

def select_count_csv_reader():
    value_rows = []
    fr = open("./datainput/db/dbsql.csv")
    csvfile = csv.reader(fr)
    next(csvfile)  # 忽略表头
    #csvfile = csv.DictReader(fr)
    for row in csvfile:
        value_rows.append((row))
    return  value_rows

@ddt.ddt
class dbengineCases(unittest.TestCase):
    db = None
    host = None
    port = None
    def setUp(self) -> None:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        global host
        self.host = host
        global port
        self.port = port
        self.db = dbengine(host=self.host,port=self.port)
    def tearDown(self) -> None:
        self.db.__del__()

    def describe_gold_case(self):
        data = self.db.execut_sql("describe gold")
        logging.info(data)
        self.assertIsInstance(data,list)
        self.assertEquals(len(data),4)
        for col in data:
            self.assertIsInstance(col,tuple)
    def hdfs_gold_case(self):
        sql = "select c_code from gold limit 10"
        data = self.db.execut_sql(sql=sql)
        logging.info(data)
        self.assertEquals(len(data),0)

    def hdfs_filter_gold_case(self):
        sql = "SELECT c_code from gold WHERE price > 5 LIMIT 10;"
        data = self.db.execut_sql(sql=sql)
        logging.info(data)
        self.assertEquals(len(data),0)


    @ddt.data(*select_count_csv_reader())
    @ddt.unpack
    def test_agg_case(self,name,sql,expect):
        data = self.db.execut_sql(sql=sql)
        logging.info(data)
        self.assertEquals(data[0][0], int(expect),msg=sql)


    def create_tabel_gold_case(self):
        sql = "create table gold_10(day int,time int,price int,c_code int) tblproperties('DS.Dataset'='70:data:tablea.txt')"

def db_init(ihost,iport=21050):
    global host
    host = ihost
    global port
    port = iport

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    cases = (select_count_csv_reader())
    #for c in  cases:
    #    print(c)
    db_init(ihost='10.18.0.19',iport=21050)
    fw = open('test.txt','w')
    runner = unittest.TextTestRunner(stream=fw,verbosity=2)
    logging.debug("deubg")

    unittest.main()
    
    suit1 = unittest.TestLoader().loadTestsFromTestCase(dbengineCases)
    suit = unittest.TestSuite(suit1)
    suit.addTest(dbengineCases("agg_gold_case"))
    runner.run(suit)
    '''
    #suit.addTest(dbengineCases("agg_case"))
    runner = HTMLReport.TestRunner(report_file_name='test',
                                   output_path='./',
                                   description='login test suite',
                                   thread_count=1,
                                   thread_start_wait=3,
                                   sequential_execution=False,
                                   lang='cn')
    runner.run(suit)
'''
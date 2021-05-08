#from .db.dbengine import dbengine
#from dbimpala.dbengine import dbengine
from dbimpala.dbengine import dbengine
import unittest
import HTMLReport
from loguru import logger
import csv
import ddt
import numpy.testing as npt

def select_count_csv_reader():
    value_rows = []
    fr = open("./datainput/db/dbsql.csv")
    csvfile = csv.reader(fr)
    next(csvfile)  # 忽略表头
    #csvfile = csv.DictReader(fr)
    for row in csvfile:
        print(len(row))
        print(row)
        value_rows.append((row))
    return  value_rows

@ddt.ddt
class dbengineCases(unittest.TestCase):
    db = None
    host = None
    port = None
    @classmethod
    def setUpClass(cls) -> None:
        #logger = logging.getLogger()
        #logger.setLevel(logging.INFO)
        global host
        cls.host = host
        global port
        cls.port = port
        cls.db = dbengine(host=cls.host,port=cls.port)
        cls.create_tabel_gold_case(cls)
    @classmethod
    def tearDownClass(cls) -> None:
        cls.drop_table(cls)
        cls.db.__del__()

    def describe_gold_case(self):
        data = self.db.execut_sql("describe gold")
        logger.info(data)
        self.assertIsInstance(data,list)
        self.assertEquals(len(data),4)
        for col in data:
            self.assertIsInstance(col,tuple)
    def hdfs_gold_case(self):
        sql = "select c_code from gold limit 10"
        data = self.db.execut_sql(sql=sql)
        logger.info(data)
        self.assertEquals(len(data),0)

    def hdfs_filter_gold_case(self):
        sql = "SELECT c_code from gold WHERE price > 5 LIMIT 10;"
        data = self.db.execut_sql(sql=sql)
        logger.info(data)
        self.assertEquals(len(data),0)


    @ddt.data(*select_count_csv_reader())
    @ddt.unpack
    def test_agg_case(self,name,sql,expect):
        res = self.db.execut_sql(sql=sql)
        logger.info(res)
        data = []
        for tmp in res:
            data.extend(tmp)
        logger.info(data)
        #self.assertAlmostEqual(data, eval(expect),msg="校验result与预期不一致 res:%s expect:%s sql:%s"%(str(data),str(expect),sql))
        npt.assert_almost_equal(data, eval(expect),err_msg="校验result与预期不一致 res:%s expect:%s sql:%s"%(str(data),str(expect),sql))
        #npt.assert_almost_equal()

    def create_tabel_gold_case(self):
        try:
            sql = "create table golda_int(day integer , time integer ,price integer ,c_code integer) TBLPROPERTIES('DS.dataset'='1:goldA_upload_int_100:goldA_upload_int_100')"
            #logger.info("create table "+sql)
            logger.info(self.db.execut_sql(sql))
            sql = "create table goldb_int(day integer , time integer ,price integer ,c_code integer) TBLPROPERTIES('DS.dataset'='1:goldb_upload_int_140:goldb_upload_int_140')"
            logger.info(self.db.execut_sql(sql))
        except Exception as err:
            logger.error(err)

    def drop_table(self):
        sql = "drop table golda_int"
        logger.info("drop table "+sql)
        #logger.info(self.db.execut_sql(sql))
        sql = "drop table goldb_int"
        logger.info("drop table " + sql)
        #logger.info(self.db.execut_sql(sql))

def db_init(ihost,iport=21050):
    global host
    host = ihost
    global port
    port = iport

if __name__ == '__main__':
    #logger = logging.getLogger()
    #logger.setLevel(logging.INFO)
    cases = (select_count_csv_reader())
    #for c in  cases:
    #    print(c)
    db_init(ihost='10.18.0.80',iport=21050)
    fw = open('test.txt','w')
    runner = unittest.TextTestRunner(stream=fw,verbosity=2)
    #logging.debug("deubg")
    #select_count_csv_reader()
    #raise 1

    unittest.main()
    
    suit1 = unittest.TestLoader().loadTestsFromTestCase(dbengineCases)
    '''
    suit = unittest.TestSuite(suit1)
    #suit.addTest(dbengineCases("agg_gold_case"))
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
    runner.run(suit1)

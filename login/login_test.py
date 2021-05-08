import unittest
import HTMLReport
import requests
import json
from stability_py.login.login import login_c
import  logging

class loginTestCase(unittest.TestCase):
    site = ''
    login_ = None
    def setUp(self) -> None:
        self.site = 'debugsaas-inspection.tsingj.local'
        self.login_ = login_c(site=self.site,user='user',passwd='123456')
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
    def login_ok(self):
        ret_j = self.login_.login_sso()
        self.assertEqual(ret_j.get("code"),0,"expect recode code")
        self.assertIsInstance(ret_j.get("data"),dict)
        self.assertIsInstance(ret_j.get("data").get("access_token"),str)
        #self.assertEqual(True, False)
        ret = self.login_.logout_sso(ret_j.get("data").get("access_token"))
        print(ret)
        self.assertEquals(ret.get("code"),0)
        self.assertEquals(ret.get("msg"),"成功")



def addlogsuite(suite,fun):
    suite.addTest(loginTestCase(fun))



if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(loginTestCase("login_ok"))
    addlogsuite(suite,fun="login_ok")
    #runner = unittest.TextTestRunner()
    runner = HTMLReport.TestRunner(report_file_name='test',
                                   output_path='./',
                                   description='login test suite',
                                   thread_count=1,
                                   thread_start_wait=3,
                                   sequential_execution=False,
                                   lang='cn')
    runner.run(suite)

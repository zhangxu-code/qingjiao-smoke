import unittest
from login.login import login_c


class loginTestcases(unittest.TestCase):
    _login = None
    tmp_user = None
    tmp_site = None
    tmp_passwd = None
    def setUp(self) -> None:

        global site
        self.tmp_site = site
        global user
        self.tmp_user = user
        global passwd
        self.tmp_passwd = passwd
        self._login = login_c(site=self.tmp_site,user=self.tmp_user,passwd=self.tmp_passwd)
    def logincase_ok(self):
        ret_j = self._login.login_sso()
        self.assertEqual(ret_j.get("code"), 0, "expect recode code")
        self.assertIsInstance(ret_j.get("data"), dict)
        self.assertIsInstance(ret_j.get("data").get("access_token"), str)
        ret = self._login.logout_sso(ret_j.get("data").get("access_token"))
        self.assertEquals(ret.get("code"), 0)
        self.assertEquals(ret.get("msg"), "成功")

    def logincase_wrong_user(self):
        login_obj = login_c(site = self.tmp_site,user = "user_not_exist",passwd = self.tmp_passwd)
        ret = login_obj.login_sso()
        self.assertEquals(ret.get("code"),1)

    def logincase_wrong_passwd(self):
        login_obj = login_c(site=self.tmp_site, user=self.tmp_user, passwd="&wrong&password&")
        ret = login_obj.login_sso()
        self.assertEquals(ret.get("code"), 1)


def login_init(insite,inuser,inpasswd):
    global site
    site = insite
    global user
    user = inuser
    global passwd
    passwd = inpasswd
import requests
#import logging
from loguru import logger
from util.redis_producer import redis_producer
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class login_c:
    site   = ''
    user   =''
    passwd = ''
    token  = ''
    MQ     = None
    def __init__(self,site='console.tsingj.local',user='smoketest',passwd='qwer1234',queue = None):
        self.site = site
        self.user = user
        self.passwd = passwd
        self.MQ = redis_producer()

    def produces(self,api,time=None,length=None,isOK=True):
        tmp = {}
        tmp["api"] = api
        if isOK == False:
            tmp["isOK"] = False
        else:
            tmp["isOK"] = True
            tmp["time"] = time
            #tmp["length"] = length
        self.MQ.producer(json.dumps(tmp))

    def login_sso(self):
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        data = {
            "username":self.user,
            "password": self.passwd
        }
        try:
            req = requests.post(url=url, data=data, verify=False)
            print(req.text)
            logger.info(req.elapsed.total_seconds()*1000)

            self.token = req.json().get("data").get("access_token")
            self.produces(api="/api/api-sso/token/simpleLogin",time=req.elapsed.total_seconds()*1000,isOK=True)
        except Exception as err:
            logger.error(err)
            self.produces(api="/api/api-sso/token/simpleLogin",isOK=False)
            return False
        return req.json()

    def logout_sso(self,token):
        url = 'http://%s/api/api-sso/token/logout'%(self.site)
        head = {
            "Authorization": "bearer %s" % (token),
            "Content-Type": "application/json"
        }
        req = requests.delete(url=url,headers = head,verify = False)
        return req.json()

    def create_user(self,username,passwd=None,token=None):
        url = 'https://%s/api/api-user/account' %(self.site)
        if passwd == None:
            passwd = 'qwer1234'
        body = {
            "password":passwd,
            "username":username,
            "status":1,
            "role":"user",
            "telephone":"13477743782"
        }
        if token:
            self.token = token
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }

        try:
            req = requests.post(url=url,headers = head,data=json.dumps(body),verify=False)
            self.produces(api="/api/api-user/account", time=req.elapsed.total_seconds() * 1000, isOK=True)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="/api/api-user/account",  isOK=False)
            return False

    def reset_passwd(self,username,newpasswd=None,token=None):
        if newpasswd == None:
            newpasswd = '1234qwer'
        if token:
            self.token = token
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        url = 'https://%s/api/api-user/account/resetPassword' % (self.site)
        body = {
            "password":newpasswd,
            "username":username
        }
        try:
            req = requests.post(url=url,data=json.dumps(body),headers=head,verify=False)
            self.produces(api="/api/api-user/account/resetPassword", time=req.elapsed.total_seconds() * 1000, isOK=True)
            return req.json()
        except Exception as err:
            logger.error(err)
            self.produces(api="/api/api-user/account/resetPassword",  isOK=False)
            return False

    def getuser_byName(self,username,token=None):
        if token:
            self.token = token
        head = {
            "Authorization": "bearer %s" % (self.token),
            "Content-Type": "application/json"
        }
        url = 'https://%s/api/api-user/account/getUserInfoByUsername' % (self.site)
        try:
            req = requests.get(url=url+'/'+username,headers=head,verify=False)
            self.produces(api="/api/api-user/account/getUserInfoByUsername", time=req.elapsed.total_seconds() * 1000, isOK=True)
            return req.json()
        except Exception as err:
            self.produces(api="/api/api-user/account/getUserInfoByUsername",isOK=False)
            logger.error(err)
            logger.error(url+"/"+username)
            logger.error(head)
            return False

    def simplelogin(self,user = None,passwd = None):
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        if user != None:
            self.user = user
        if passwd != None:
            self.passwd = passwd

        data = {
            "username":self.user,
            "password": self.passwd
        }
        try:
            logger.info(url)
            logger.info(data)
            req = requests.post(url=url, params=data, verify=False)
            print(req.text)
            self.token = req.json().get("data").get("access_token")
            self.produces(api="/api/api-sso/token/simpleLogin", time=req.elapsed.total_seconds() * 1000,length=req.headers.get("Content-Length"), isOK=True)
        except Exception as err:
            logger.error(err)
            self.produces(api="/api/api-sso/token/simpleLogin", isOK=False)
            return False
        return req.json()


if __name__ == '__main__':
    print("login")
    login = login_c()
    login.login_sso()
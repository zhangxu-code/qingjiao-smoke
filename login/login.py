import requests
import logging
class login_c:
    site = ''
    user=''
    passwd = ''
    def __init__(self,site='debugsaas.tsingj.local',user='autotest',passwd='qwer1234'):
        self.site = site
        self.user = user
        self.passwd = passwd
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

    def login_sso(self):
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        data = "username=%s&password=%s" % (self.user, self.passwd)
        req = requests.post(url=url, params=data, verify=False)
        print(req.text)
        return req.json()

    def logout_sso(self,token):
        url = 'http://%s/api/api-sso/token/logout'%(self.site)
        head = {
            "Authorization": "bearer %s" % (token),
            "Content-Type": "application/json"
        }
        req = requests.delete(url=url,headers = head,verify = False)
        return req.json()

if __name__ == '__main__':
    print("login")
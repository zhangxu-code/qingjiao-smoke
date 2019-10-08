import requests
import os
import sys
import json

class MetaData:
    token = ''
    site = ''
    def __init__(self,site,user,passwd):
        self.site = site
        url = 'https://%s/api/api-sso/token/simpleLogin' % (self.site)
        data = "username=%s&password=%s" % (user, passwd)
        req = requests.post(url=url, params=data, verify=False)
        print(req.text)
        self.token = req.json().get("data").get("access_token")

    def list_metadata_page(self,page=1,token=None):
        url = 'https://%s/api/api-tm/dataSourceMetadata' % (self.site)
        if token == None:
            token = self.token
        head = {
            "Authorization": "bearer %s" % (token)
        }
        data = "page=%d" % (page)
        req = requests.get(url=url, headers=head, params=data, verify=False)
        return req.json()

    def add_data(self,setname,dsid,key,filename='../data.csv',token = None):
        url = "https://%s/api/api-tm/dataSourceMetadata" %(self.site)
        if token == None:
            token = self.token
        head = {
            "authorization":"bearer %s" %(token),
            "Content-Type":"binary"
        }
        body = {
            "dataSetName":setname,
            "dsId":dsid,
            "key":key
        }
        file = {"file":open(filename,'rb')}
        res = requests.post(url=url,data=json.dumps(body),files = file,headers = head,verify=False)
        return res.json()


if __name__ == '__main__':
    metadata = MetaData(user="autotest",passwd='qwert12345',site='debugsaasuat.tsingj.local')
    #print(metadata.list_metadata_page())
    print(metadata.add_data(setname="data_matrix",dsid=1,key='two_matrix'))

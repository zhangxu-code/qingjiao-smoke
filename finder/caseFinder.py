# -*- coding: utf-8 -*-
"""
Project smoketestbak
Author  zhenghongguang
Date    2020-02-03
"""

import os
import re
import sys
import inspect
import importlib
import unittest
import re
from loguru import logger


class casefinder:

    def find_py(self, path):
        logger.info(path)
        files_py = []
        for tmp_file in os.listdir(path):
            if os.path.isfile(path + "/" + tmp_file):
                if ".py" == os.path.splitext(path + "/" + tmp_file)[1]:
                    files_py.append(path + "/" + tmp_file)
            if os.path.isdir(path + "/" + tmp_file):
                files_py.extend(self.find_py(path + "/" + tmp_file))
        return files_py

    def find_cls(self, cls):
        """
        查找类中已 "test"开头的方法
        :param cls:
        :return:
        """
        test_methods = []
        for fun in dir(cls):
            if re.search(re.compile(r'^test'), fun.lower()):
                tmp_fun = {fun: self.method_comment(getattr(cls, fun).__doc__)}
                test_methods.append(tmp_fun)
        return test_methods

    def method_comment(self, comment):
        """
        已 [\w+] 开头的认为是注释
        :param comment:
        :return:
        """
        if comment == None:
            return None
        for _ in comment.split("\n"):
            if re.search(r'^\[[\w]+\]', _.strip()):
                return _.strip()
        return None

    def find_cases(self, files_py):
        sys.path.append(os.getcwd())
        cases_dict = {}
        casespath_dict = {}
        for _py in files_py:
            try:
                modle = _py[len(os.getcwd()):].lstrip('/')
                tmp_mod = importlib.import_module(modle[:-3].replace('/', '.'))
                for name, obj in inspect.getmembers(tmp_mod):
                    if inspect.isclass(obj):
                        if obj.__base__ == unittest.TestCase:
                            '''文件路径作为一级key， 类名作为二级key'''
                            if modle[:-3].replace('/', '.') in casespath_dict.keys():
                                casespath_dict[modle[:-3].replace('/', '.')][name] = []
                            else:
                                casespath_dict[modle[:-3].replace('/', '.')] = {}
                            logger.info(modle[:-3].replace('/', '.'))
                            tmp_dict = cases_dict
                            # 如果为unittest.TestCase 子类，则按文件路径组织字典结构
                            path_list = modle.split("/")
                            for i in range(len(path_list)):
                                if i == len(path_list) - 1:
                                    tmp_dict[os.path.splitext(path_list[i])[0]] = {}
                                    tmp_dict = tmp_dict.get(os.path.splitext(path_list[i])[0])
                                else:
                                    tmp_dict[path_list[i]] = {}
                                    tmp_dict = tmp_dict.get(path_list[i])
                            casespath_dict[modle[:-3].replace('/', '.')][name] = self.find_cls(obj)
                            tmp_dict = casespath_dict[modle[:-3].replace('/', '.')][name]
                            #cases_dict[modle[:-3].replace('/', '.')] = tmp_dict
            except Exception as err:
                logger.error(err)
                logger.info(_py)
        return cases_dict, casespath_dict

    def find_bypath(self, path):
        pyfiles = self.find_py(path)
        cases = []
        logger.info(pyfiles)
        # for _ in pyfiles:
        cases_dict, casespath_dict = self.find_cases(pyfiles)
        logger.info(cases_dict)
        logger.info(casespath_dict)
        return cases

    def key_match(self, key, comment):
        if comment == None:
            return True
        method_key = re.findall(r'\[[\w]+\]', comment)
        method_key_f = []
        for _ in method_key:
            method_key_f.append(_[1:-1])
        logger.info(method_key_f)
        if key is None:
            return True
        if isinstance(key, list):
            if set(key).intersection(set(method_key_f)):
                return True
            else:
                return False
        if key in method_key_f:
            return True
        else:
            return False

    def findcases_bypath(self, path, key=None):
        if os.path.isdir(path):
            pyfiles = self.find_py(path)
        elif os.path.isfile(path):
            pyfiles = path
        cases_dict, casespath_dict = self.find_cases(pyfiles)
        suite = unittest.TestSuite()
        for pathkey in casespath_dict.keys():
            filename = pathkey.split('.')[-1]
            logger.info(pathkey)
            #logger.info(cls)
            module_packge = importlib.import_module(pathkey)
            for cls in casespath_dict.get(pathkey):
                for name, obj in inspect.getmembers(module_packge):
                    if inspect.isclass(obj):
                        if name == cls:
                            for method_case in casespath_dict.get(pathkey).get(cls):
                                method, comment = method_case.popitem()
                                #logger.info(method)
                                #logger.info(comment)
                                if self.key_match(key, comment):
                                    suite.addTest(obj(method))
                        #for fun in dir(obj):
                        #    if re.search(re.compile(r'^test'), fun.lower()):
                        #        suite.addTest(obj(fun))
        return suite


def main():
    '''
    fr = open("./finder/conf.yaml")
    conf = yaml.load(fr)
    fr.close()
    logger.info(conf)
    file_py = []
    for _ in conf.get("path"):
        #sys.path.append(os.getcwd()+_)
        file_py = find_py(os.getcwd() + "/" + _)
        print(file_py)
    logger.info(find_cases(file_py))
    '''
    sys.path.append(os.getcwd())
    module_path = 'privpy_library.privpy_lib.tests.test_pnumpy.test_array_creation'
    module_privpy = importlib.import_module(module_path)
    print(type(module_privpy))
    for name, obj in inspect.getmembers(module_privpy):
        if inspect.isclass(obj):
            print(name)
            print(obj)
            print(obj.__base__.__name__)
            if obj.__base__.__name__ == "TestCase":
                for fun in dir(obj):
                    print(fun)
            print('-' * 100)


if __name__ == '__main__':
    main()

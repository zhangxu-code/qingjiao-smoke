#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-28
"""

import os
import importlib
import inspect

if __name__ == '__main__':

    module = importlib.import_module("testddt")
    print(module)
    for name,obj in inspect.getmembers(module):
        print(name)
        print(obj)
        if name == "test":
            print(dir(obj))

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketest
Author  zhenghongguang
Date    2020-02-28
"""

import os
import ddt
import unittest
import csv

def job_csv():
    value_rows = []
    fr = open('./test/data.csv')
    csvfile = csv.reader(fr)
    next(csvfile)
    # csvfile = csv.DictReader(fr)
    for row in csvfile:
        value_rows.append((row))
        print(row)
    fr.close()
    print(len(value_rows))
    return value_rows

@ddt.ddt
class test(unittest.TestCase):

    @ddt.data(*job_csv())
    @ddt.unpack
    def test_jobrun(self, data):
        print(data)


#if __name__ == '__main__':
#    unittest.main()
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketestbak
Author  zhenghongguang
Date    2020-02-19
"""

import yaml
from loguru import  logger
#from tm.consoleapi import ConsoleAPI

def getmetaId(client, ds_name, data_set_name,meta_key):
    logger.info("get meta Id")

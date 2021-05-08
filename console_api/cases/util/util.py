#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Project smoketestbak
Author  zhenghongguang
Date    2020-02-19
"""

from loguru import logger
import random


def getmetaId(client):
    logger.info("get meta id")
    dspage = 0
    while 1:
        try:
            response = client.query_ds(query="page=%d" % dspage)
            datasetpage = 0
            for ds in response.get("data").get("data"):
                dsId = ds.get("id")
                response_dataset = client.query_dataset(query="dataServerId=%d&page=%d" % (dsId, datasetpage))
                for dataset in response_dataset.get("data").get("data"):
                    datasetid = dataset.get("id")
                    response_metadata = client.query_metadata(query="dataServerId=%d&dataSetId=%d" % (dsId, datasetid))
                    randint = random.randint(1, len(response_metadata.get("data").get("data"))) - 1
                    return response_metadata.get("data").get("data")[randint].get("metaId")
        except Exception as err:
            logger.error(err)
            return False
    return False

def getdsid(client):
    response = client.query_ds()
    try:
        randint = random.randint(1, len(response.get("data").get("data"))) - 1
        return response.get("data").get("data")[randint].get("id"), \
               response.get("data").get("data")[randint].get("name")
    except Exception as err:
        logger.error(err)
        return False
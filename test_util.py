#import numpy as np
#from client import TaskRunner
from taskrunner.taskRunner_api import TaskRunnerAPI
#import logging
from loguru import logger

'''
def run_task(code):
    task_runner = TaskRunner()
    task_runner.set_sources([])
    task_runner.set_dests([])
    task_runner.set_code(code)
    debug_result, _ = task_runner.run()

    res = {}
    for x in debug_result:
        varname = x['varname']
        res[varname] = {}
        if len(x['shape']) == 0:
            res[varname]['val'] = x['data']
        else:
            res[varname]['val'] = np.reshape(x['data'], x['shape'])
        res[varname]['type'] = x['type']

    return res
'''

def run_task(code):
    logger.info(code)
    runner = TaskRunnerAPI()
    result = runner.run(code='import sys\n'+code)
    #print('----------')
    logger.info(result)
    for key in result.keys():
        logger.info(result.get(key).get("val"))
        logger.info(type(result.get(key).get('val')))
    #print('+++++++++++')
    return result
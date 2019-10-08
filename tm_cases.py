import unittest
from tm.job_1M import tmJob
import logging
import time
#from tm import job_conf
import re


class tmTestcases(unittest.TestCase):
    _tm = None

    def setUp(self) -> None:
        global conf_args
        global site
        tmp_site = site
        global user
        tmp_user = user
        global passwd
        tmp_passwd = passwd
        logging.info("%s : %s : %s "%(tmp_site,tmp_user,tmp_passwd))
        self._tm = tmJob(site = tmp_site ,user = tmp_user, passwd=tmp_passwd)
    def jobstartcase(self):
        jobid_l = conf_args.get("jobid").split(',')
        for jobid in jobid_l:
            ret = self._tm.job_start(jobid)
            self.assertEqual(ret.get("code"),0)
            self.assertIsInstance(ret.get("data"),str)
    def jobstart_pipline_case(self):
        # 由用户指定输入jobid
        jobid_l = conf_args.get("jobid").split(',')
        logging.info("start job %s"%(conf_args.get("jobid")))
        for jobid in jobid_l:
            ret = self._tm.job_start(jobid)
            self.assertEqual(ret.get("code"),0)
            self.assertIsInstance(ret.get("data"),str)
            finished = False
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=jobid)
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)

    def jobgetConfig_case(self):
        jobid_L = conf_args.get("jobid").split(',')
        for jobid in jobid_L:
            ret = self._tm.job_getconfig(jobid)
            self.assertEquals(ret.get("code"),0)

    #创建任务的结果校验
    def createjob_ret_check(self,key,ret,code,datasource,result):
        self.assertEquals(ret.get("code"), 0)
        self.assertIsInstance(ret.get("data").get("id"), int)
        self.assertEquals(ret.get("data").get("name"),
                          "autotest_%s" % (key))
        self.assertEquals(ret.get("data").get("code"), code)
        datasource_ret = []
        for data in ret.get("data").get("taskDataSourceVOList"):
            tmp = {}
            tmp["dataSourceId"] = data.get("dataSourceId")
            tmp["dataSourceMetadataId"] = data.get("dataSourceMetadataId")
            tmp["varName"] = data.get("varName")
            tmp["dataServerId"] = data.get("dataServerId")
            datasource_ret.append(tmp)
        result_ret = []
        for res in ret.get("data").get("taskResultVOList"):
            tmp = {}
            tmp["resultVarName"] = res.get("resultVarName")
            tmp["resultDest"]    = res.get("resultDest")
            result_ret.append(tmp)
        logging.debug(datasource)
        logging.debug(datasource_ret)
        logging.debug(result)
        logging.debug(result_ret)
        self.assertEquals(datasource_ret,datasource)
        self.assertEquals(result_ret,result)
    def createjob_pipline(self,key,datasource,result,code):
        ret = self._tm.job_create(key=key, result=result,
                                  datasource=datasource,
                                  code=code)
        self.createjob_ret_check(key=key, ret=ret, code=code, datasource=datasource, result=result)
        ret_start = self._tm.job_start(ret.get("data").get("id"))
        self.assertEqual(ret_start.get("code"), 0)
        self.assertIsInstance(ret_start.get("data"), str)
        finished = False
        # get jobinfo and check job status
        while not finished:
            jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
            if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                finished = True
            time.sleep(1)
        self.assertEquals(jobinfo.get("code"), 0)
        self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)

    #下面是六个基准算法测试case和二维数组的加、乘、比较
    def jobcreate_cnnface_pipline(self):
        datasource = [
            {
                "dataSourceId": 386,
                 "dataSourceMetadataId": 370,
                 "dataServerId": 69,
                 "varName": "face_people"
             },
            {
                "dataSourceId": 386,
                "dataSourceMetadataId": 371,
                "dataServerId": 69,
                "varName": "face_table"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import privpy as pp\nimport pnumpy as pnp\n\nTABLE = pnp.reshape(pp.ss(\"face_table\"), (105, 512))\nPEOPLE = pnp.reshape(pp.ss(\"face_people\"), (5, 512))\n\nRES = []\nfor vec in PEOPLE:\n    n_persons = TABLE.shape[0]\n    dist = pnp.tile(vec, (n_persons, 1)) - TABLE\n    dist = pnp.sum(dist * dist, axis=1)  # square distance\n    index = pnp.argmin(dist)\n    RES.append(index)\n    \npp.reveal(pp.farr(RES), 'result')"

        self.createjob_pipline(key="cnnface",datasource=datasource,result=result,code=code)


    def jobcreate_matrix_factor_pipline(self):
        datasource = [
            {
                "dataSourceId": 447,
                 "dataSourceMetadataId": 517,
                 "dataServerId": 70,
                 "varName": "movie"
             },
            {
                "dataSourceId": 218,
                "dataSourceMetadataId": 80,
                "dataServerId": -1,
                "varName": "movie2"
            }
        ]
        result = [{
            "resultVarName": "result1",
            "resultDest": "-1"
        },{
            "resultVarName": "result2",
            "resultDest": "-1"
        }]
        code = "import numpy as np\nimport privpy as pp\nimport pnumpy as pnp\n\ndef funk_svd(d_tmp, k_tmp, iter_times=60, alpha=0.01, learn_rate=0.001):\n    '''\n    funk_svd\n    '''\n    scalar = 0.001\n    a, b = d_tmp.shape  # D size = m * n\n    np.random.seed(2018)\n    u = pp.farr(np.random.rand(a, k_tmp) * scalar)\n    v = pp.farr(np.random.rand(k_tmp, b) * scalar)\n    err_tmp = None\n    for i in range(iter_times):\n        print('round:', i)\n        d_est = pnp.dot(u, v)\n        err_tmp = d_tmp - d_est\n        print('computed Err')\n        u_grad = -2 * pnp.dot(err_tmp, v.trans()) + 2 * alpha * u\n        v_grad = -2 * pnp.dot(u.trans(), err_tmp) + 2 * alpha * v\n        u = u - learn_rate * u_grad\n        v = v - learn_rate * v_grad\n    print('updated u and v')\n    return u, v\n\nDATA1 = pnp.reshape(pp.ss(\"movie1\"), (50, 9724))\nDATA2 = pnp.reshape(pp.ss(\"movie2\"), (50, 9724))\nDATA = pnp.concatenate([DATA1, DATA2])\n\nk = 10\nITER_NUM = 2\n\nU_RES, V_RES = funk_svd(DATA, k, iter_times=ITER_NUM, learn_rate=0.0005, alpha=0.001)\n\npp.reveal(U_RES, \"result1\")\npp.reveal(V_RES, \"result2\")"
        self.createjob_pipline(key="matrix_factor", datasource=datasource, result=result, code=code)


    def jobcreate_logistic_pipline(self):
        datasource = [
            {
                "dataSourceId": 402,
                 "dataSourceMetadataId": 407,
                 "dataServerId": 70,
                 "varName": "adult1"
             },
            {
                "dataSourceId": 212,
                "dataSourceMetadataId": 63,
                "dataServerId": -1,
                "varName": "adult2"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import numpy as np\nimport privpy as pp\nimport pnumpy as pnp\nimport ptorch as pt \n\nclass LogisticRegressionS(object):\n    '''\n    the logistic regression class, including msub functions.\n    '''\n    def __init__(self, q=0.01, num_iter=100000):\n        self.q = q\n        self.num_iter = num_iter\n        self.theta = pnp.zeros([])\n\n    def __add_intercept(self, x):\n        '''\n        add x shape ones matrix with x\n        '''\n        intercept = pnp.ones((x.shape[0], 1))\n        return pnp.concatenate((intercept, x), axis=1)\n\n    def __sigmoid(self, x):\n        '''\n        return 1 / (1 + np.exp(-x))\n        '''\n        tmp_result = pt.relu(x + 0.5)\n        tmp_result = tmp_result - pt.relu(tmp_result - 1)\n        return tmp_result\n\n    def fit(self, x, y, batch=-1):\n        '''\n        fit\n        '''\n        x = self.__add_intercept(x)\n\n        if batch < 0:\n            batch = len(x)\n\n        # weights initialization\n        self.theta = pnp.zeros(x.shape[1])\n\n        for i in range(self.num_iter):\n            for j in range(0, len(x), batch):\n                tmp_x = x[j:j+batch]\n                tmp_y = y[j:j+batch]\n                z = pnp.dot(tmp_x, self.theta)\n                d = self.__sigmoid(z)\n                gradient = pnp.dot(tmp_x.trans(), (d - tmp_y)) / len(tmp_y)\n                self.theta -= self.q * gradient / np.sqrt(i + 1)\n\n    def predict_prob(self, x):\n        '''\n        predice prob\n        '''\n        x = pp.farr(x)\n        x = self.__add_intercept(x)\n        return self.__sigmoid(pnp.dot(x, self.theta))\n\n    def predict(self, x):\n        '''\n        predict\n        '''\n        return self.predict_prob(x)\n\nNUM_ITER = 20\nBATCH_SIZE = 100\n\nDATA1 = pnp.reshape(pp.ss(\"adult1\"), (200, 124))\nDATA2 = pnp.reshape(pp.ss(\"adult2\"), (200, 124))\nX1, Y1 = DATA1[:, :-1], DATA1[:, -1]\nX2, Y2 = DATA2[:, :-1], DATA2[:, -1]\n\nX_S = pnp.concatenate([X1, X2])\nY_S = pnp.concatenate([Y1, Y2])\n\nMODEL_S = LogisticRegressionS(q=0.1, num_iter=NUM_ITER)\nMODEL_S.fit(X_S, Y_S, batch=BATCH_SIZE)\n\nPRED_S = MODEL_S.predict(X_S)\npp.reveal(PRED_S, \"result\")"

        self.createjob_pipline(key="logistic", datasource=datasource, result=result, code=code)


    def jobcreate_query_gold_pipline(self):
        datasource = [
            {
                "dataSourceId": 443,
                 "dataSourceMetadataId": 499,
                 "dataServerId": 70,
                 "varName": "table"
             },
            {
                "dataSourceId": 387,
                "dataSourceMetadataId": 375,
                "dataServerId": 69,
                "varName": "query"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import numpy as np\nimport privpy as pp\nimport pnumpy as pnp\n\ndef queryf(table_que, condition_que):\n    '''\n    query function\n    '''\n    commodity_code, date_low, date_up, time_low, time_up, price_low, price_up = condition_que\n    date_que, time_que, price_que, code_que = table_que.trans()\n    cdate_1 = date_low < date_que\n    cdate_2 = (date_low == date_que) * (time_low * 1000 < time_que + 0.5)\n    cdate_3 = date_que < date_up\n    cdate_4 = (date_up == date_que) * (time_que - 0.5 < time_up * 1000)\n    cdate_que = (1 - (1 - cdate_1) * (1 - cdate_2)) * (1 - (1 - cdate_3) * (1 - cdate_4))\n    cprice_que = (1 - (price_que < price_low)) * (1 - (price_que > price_up))\n    ccomid_que = (commodity_code == code_que)\n    que = cdate_que * cprice_que * ccomid_que\n    count_que = pnp.sum(que)\n    return count_que\n\nTABLE = pnp.reshape(pp.ss(\"table\"), (1940, 4))\nQUERY = pnp.reshape(pp.ss(\"query\"), (2, 7))\n\nres = []\nfor i in range(len(QUERY)):\n    tmp_res = queryf(TABLE, QUERY[i])\n    res.append(tmp_res)\n\npp.reveal(pp.farr(res), \"result\")"

        self.createjob_pipline(key="query_gold", datasource=datasource, result=result, code=code)


    def jobcreate_lstm_hsi_pipline(self):
        datasource = [
            {
                "dataSourceId": 393,
                 "dataSourceMetadataId": 387,
                 "dataServerId": 69,
                 "varName": "data"
             },
            {
                "dataSourceId": 393,
                "dataSourceMetadataId": 386,
                "dataServerId": 69,
                "varName": "model"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import numpy as np\nimport privpy as pp\nimport pnumpy as pnp\n\nclass LSTM(object):\n    '''\n    LSTM class\n    '''\n\n    def sigmoid(self, x, step_count=10):\n        '''\n        sigmoid function\n        '''\n        tmp_result = 0.5\n        delta_x = x / step_count\n        for i in range(step_count):\n            derivate = tmp_result * (1 - tmp_result)\n            tmp_result += delta_x * derivate\n            i += 1\n        return tmp_result\n\n    def tanh(self, x):\n        '''\n        2sigmoid(2x) - 1 = tanh(x)\n        '''\n        return 2.0 * self.sigmoid(2.0 * x) - 1.0\n\n    def __init__(self, input_size, hidden_size, w_ih, w_hh, b_ih, b_hh):\n        self.input_size = input_size\n        self.hidden_size = hidden_size\n\n        # weights for current time series x_t. There are four gates:\n        # ii for input gate (update the choice of sigmoid)\n        # if for forgive gate\n        # ig for input gate (update to the value of tanh)\n        # io for output gate\n        self.w_ii = w_ih[:hidden_size]\n        self.w_if = w_ih[hidden_size:2 * hidden_size]\n        self.w_ig = w_ih[2 * hidden_size:3 * hidden_size]\n        self.w_io = w_ih[3 * hidden_size:4 * hidden_size]\n\n        self.b_ii = b_ih[:hidden_size]\n        self.b_if = b_ih[hidden_size:2 * hidden_size]\n        self.b_ig = b_ih[2 * hidden_size:3 * hidden_size]\n        self.b_io = b_ih[3 * hidden_size:4 * hidden_size]\n\n        # weights for previous hidden h_(t_1), concatenent to x_t\n        self.w_hi = w_hh[:hidden_size]\n        self.w_hf = w_hh[hidden_size:2 * hidden_size]\n        self.w_hg = w_hh[2 * hidden_size:3 * hidden_size]\n        self.w_ho = w_hh[3 * hidden_size:4 * hidden_size]\n\n        self.b_hi = b_hh[:hidden_size]\n        self.b_hf = b_hh[hidden_size:2 * hidden_size]\n        self.b_hg = b_hh[2 * hidden_size:3 * hidden_size]\n        self.b_ho = b_hh[3 * hidden_size:4 * hidden_size]\n\n    def pass_cell(self, x_c, h_p, c_p):  # c - current, p - previous\n        '''\n        pass cell\n        '''\n        i_t = self.sigmoid(\n            pnp.dot(self.w_ii, x_c) + self.b_ii + pnp.dot(self.w_hi, h_p) +\n            self.b_hi)\n\n        f_t = self.sigmoid(\n            pnp.dot(self.w_if, x_c) + self.b_if + pnp.dot(self.w_hf, h_p) +\n            self.b_hf)\n\n        g_t = self.tanh(\n            pnp.dot(self.w_ig, x_c) + self.b_ig + pnp.dot(self.w_hg, h_p) +\n            self.b_hg)\n\n        o_t = self.sigmoid(\n            pnp.dot(self.w_io, x_c) + self.b_io + pnp.dot(self.w_ho, h_p) +\n            self.b_ho)\n\n        c_c = f_t * c_p + i_t * g_t\n        h_c = o_t * self.tanh(c_c)\n\n        return h_c, c_c\n\n    def predict(self, input_t, h_0, c_0):\n        '''\n        predict\n        '''\n        (seq_len, input_size) = input_t.shape\n        if input_size != self.input_size:\n            raise Exception('The input_size of the input is incorrect!')\n\n        h_p, c_p = h_0, c_0\n        output = []\n\n        for k in range(seq_len):\n            x_c = input_t[k]\n            h_p, c_p = self.pass_cell(x_c, h_p, c_p)\n            output.append(h_p)\n\n        return pp.farr(output)\n\n# The model hyper-parameters\n\nglobals()['input_size'] = 24\nglobals()['hidden_size'] = 24\nglobals()['seq_len'] = 10\n\nMODEL = pp.ss(\"model\")\n\nglobals()['weight_emb'] = pnp.reshape(MODEL[:24], (24,))\nglobals()['bias_emb'] = pnp.reshape(MODEL[24:48], (24,))\nglobals()['weight_out'] = pnp.reshape(MODEL[48:72], (24,))\nglobals()['bias_out'] = MODEL[72]\nglobals()['weight_ih'] = pnp.reshape(MODEL[73:2377], (96, 24))\nglobals()['bias_ih'] = pnp.reshape(MODEL[2377:2473], (96,))\nglobals()['weight_hh'] = pnp.reshape(MODEL[2473:4777], (96, 24))\nglobals()['bias_hh'] = pnp.reshape(MODEL[4777:4873], (96,))\n\nX_TEST = pnp.reshape(pp.ss(\"data\"), (3, 10))\n\nglobals()['lstm'] = LSTM(input_size, hidden_size, weight_ih, weight_hh, bias_ih, bias_hh)\n\ndef compute(x, g_tmp, c_tmp):  # X is of shape (1, 10)\n    '''\n    compute\n    '''\n    x_mat = pnp.tile(pnp.reshape(x, (seq_len, 1)), (\n        1,\n        input_size,\n    ))  # 10 x 24\n    emb_weight_mat = pnp.tile(weight_emb, (seq_len, 1))  # 10 x 24\n    emb_bias_mat = pnp.tile(bias_emb, (seq_len, 1))  # 10 x 24\n    pass_emb_layer = x_mat * emb_weight_mat + emb_bias_mat  # of shape (10, 24)\n\n    # predictions = []\n    y_lstm_pred = lstm.predict(pass_emb_layer, g_tmp, c_tmp)[-1]\n\n    y_out = pnp.dot(weight_out, y_lstm_pred) + bias_out\n\n    return y_out\n\ndef restore(array):\n    '''\n    restore array\n    '''\n    mean_arr = 24706.462699077954\n    std_arr = 3113.3302180457677\n    return array * std_arr + mean_arr\n\ng = pnp.zeros(hidden_size)\nc = pnp.zeros(hidden_size)\nY_PREDS = []\nX_LEN = len(X_TEST)\nfor j in range(X_LEN):\n    y_pred = restore(compute(X_TEST[j], g, c))\n    Y_PREDS.append(y_pred)\nY_PREDS = pp.farr(Y_PREDS)\n\npp.reveal(Y_PREDS, \"result\")"
        self.createjob_pipline(key="lstm_hsi", datasource=datasource, result=result, code=code)


    def jobcreate_nn_mnist_pipline(self):
        datasource = [
            {
                "dataSourceId": 217,
                 "dataSourceMetadataId": 127,
                 "dataServerId": -1,
                 "varName": "mnist_weight"
             },
            {
                "dataSourceId": 217,
                "dataSourceMetadataId": 132,
                "dataServerId": -1,
                "varName": "mnist_image"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import numpy as np\nimport privpy as pp\nimport pnumpy as pnp\nimport ptorch as pt\n\ndef parse_weight(weight_par):\n    '''\n    parse weight\n    '''\n    result = []\n    shape = [784, 625, 625, 10]\n    start = 0\n    for i in range(1, len(shape)):\n        step = shape[i-1] * shape[i]\n        result.append(pnp.reshape(weight_par[start:start+step], (shape[i], shape[i-1])))\n        start += step\n    return result\n\ndef inference(img_inf, w_inf):\n    '''\n    inference\n    '''\n    tmp_x = img_inf\n    w_len = len(w_inf)\n    for i in range(w_len):\n        tmp_x = pnp.dot(tmp_x, w_inf[i].trans())\n        if i < len(w_inf) - 1:\n            tmp_x = pt.relu(tmp_x)\n    return pnp.argmax(tmp_x, axis=1)\n\nWEIGHT = pp.ss(\"mnist_weight\") # shape = 784 * 625 + 625 * 625 + 625 * 10\nIMG = pnp.reshape(pp.ss(\"mnist_image\"), (10, 784))\nw = parse_weight(WEIGHT)\n\nPREDS = inference(IMG, w)\n\npp.reveal(PREDS, \"result\")"
        self.createjob_pipline(key="nn_mnist", datasource=datasource, result=result, code=code)

    def jobcreate_100M_pipline(self):
        datasource = [
            {
                "dataSourceId": 1278,
                "dataSourceMetadataId": 2521,
                "dataServerId": 70,
                "varName": "data1"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import privpy as pp\na1=pp.ss(\"data1\")\nresult = a1*a1\npp.reveal(result, \"result\")"
        self.createjob_pipline(key="100M", datasource=datasource, result=result, code=code)

    def jobcreate_10M_pipline(self):
        datasource = [
            {
                "dataSourceId": 1277,
                "dataSourceMetadataId": 2520,
                "dataServerId": 69,
                "varName": "data1"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import privpy as pp\na1=pp.ss(\"data1\")\nresult = a1*a1\npp.reveal(result, \"result\")"
        self.createjob_pipline(key="10M", datasource=datasource, result=result, code=code)

    def jobcreate_1M_pipline(self):
        datasource = [
            {
                "dataSourceId": 499,
                "dataSourceMetadataId": 746,
                "dataServerId": 70,
                "varName": "data1"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import privpy as pp\na1=pp.ss(\"data1\")\nresult = a1*a1\npp.reveal(result, \"result\")"
        self.createjob_pipline(key="1M", datasource=datasource, result=result, code=code)

    def jobcreate_500M_pipline(self):
        datasource = [
            {
                "dataSourceId": 1374,
                "dataSourceMetadataId": 2739,
                "dataServerId": 70,
                "varName": "data1"
            }
        ]
        result = [{
            "resultVarName": "result",
            "resultDest": "-1"
        }]
        code = "import privpy as pp\n\na = pp.ss(\"data1\")\n\n#result = pp.sint(1)\n#result = a \nresult = a * a\n    \npp.reveal(result,'result')\n"
        self.createjob_pipline(key="500M", datasource=datasource, result=result, code=code)

    def jobcreate_multiplication_pipline(self):
        logging.info("create job")
        #create job
        key = conf_args.get("key")
        jobCount = int(conf_args.get("Num"))
        datasource = [{
            "dataServeId":322,
            "dataSourceId":520,
            "dataSourceMetadataId":761,
            "varName":"data"
        }]
        result = [{
            "resultDest":"322",
            "resultVarName":"result"
        }]
        var_re = re.compile('pp\.ss\(\"[\w]+\"\)')
        varname = 'data'
        code = "import privpy as pp\ndd=pp.ss(\"%s\")\nresult = dd*dd \npp.reveal(result,\"result\")" %(varname)
        index = 0
        while index < jobCount:
            ret = self._tm.job_create(key=key,result=result,
                                datasource=datasource,
                                code=code)
            #check create job response
            self.createjob_ret_check(key=key,ret=ret,code=code)
            #start job and check code
            ret_start = self._tm.job_start(ret.get("data").get("id"))
            self.assertEqual(ret_start.get("code"), 0)
            self.assertIsInstance(ret_start.get("data"), str)
            finished = False
            #get jobinfo and check job status
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
            index = index + 1
    def jobcreate_addition_pipline(self):
        logging.info("create job")
        #create job
        key = conf_args.get("key")
        jobCount = int(conf_args.get("Num"))
        datasource = [{
            "dataServeId": 322,
            "dataSourceId": 520,
            "dataSourceMetadataId": 761,
            "varName": "data"
        }]
        result = [{
            "resultDest": "322",
            "resultVarName": "result"
        }]
        var_re = re.compile('pp\.ss\(\"[\w]+\"\)')
        varname = 'data'
        code = "import privpy as pp\ndd=pp.ss(\"%s\")\nresult = dd + dd \npp.reveal(result,\"result\")" %(varname)
        index = 0
        while index < jobCount:
            ret = self._tm.job_create(key=key,result=result,
                                datasource=datasource,
                                code=code)
            #check create job response
            self.createjob_ret_check(key=key,ret=ret,code=code)
            #start job and check code
            ret_start = self._tm.job_start(ret.get("data").get("id"))
            self.assertEqual(ret_start.get("code"), 0)
            self.assertIsInstance(ret_start.get("data"), str)
            finished = False
            #get jobinfo and check job status
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
            index = index + 1
    def jobcreate_comparison_pipline(self):
        logging.info("create job")
        #create job
        key = conf_args.get("key")
        jobCount = int(conf_args.get("Num"))
        datasource = [{
            "dataServeId": 322,
            "dataSourceId": 520,
            "dataSourceMetadataId": 761,
            "varName": "data"
        }]
        result = [{
            "resultDest": "322",
            "resultVarName": "result"
        }]
        varname = 'data'
        code = "import privpy as pp\ndd=pp.ss(\"%s\")\nresult = dd < dd \npp.reveal(result,\"result\")" %(varname)
        index = 0
        while index < jobCount:
            ret = self._tm.job_create(key=key,result=result,
                                datasource=datasource,
                                code=code)
            #check create job response
            self.createjob_ret_check(key=key,ret=ret,code=code)
            #start job and check code
            ret_start = self._tm.job_start(ret.get("data").get("id"))
            self.assertEqual(ret_start.get("code"), 0)
            self.assertIsInstance(ret_start.get("data"), str)
            finished = False
            #get jobinfo and check job status
            while not finished:
                jobinfo = self._tm.job_getinfo(jobid=ret.get("data").get("id"))
                if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                    finished = True
                time.sleep(1)
            self.assertEquals(jobinfo.get("code"), 0)
            self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
            index = index + 1
    def listjob_check(self,ret,curpage):
        if ret.get("data").get("totalPages") > curpage:
            self.assertEquals(ret.get("data").get("nextPageNo"), curpage + 1)
        self.assertEquals(ret.get("data").get("previousPageNo"), curpage if curpage == 1 else curpage + 1)
        self.assertEquals(ret.get("code"), 0)
        self.assertIsInstance(ret.get("data"), dict)
        self.assertIsInstance(ret.get("data").get("pageNo"), int)
        self.assertIsInstance(ret.get("data").get("pageSize"), int)
        self.assertIsInstance(ret.get("data").get("totalRows"), int)
        self.assertIsInstance(ret.get("data").get("data"), list)
    def listjob_case(self):
        curpage = 1
        hasmorejob = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret,curpage=curpage)
            if ret.get("data").get("totalPages") == curpage:
                hasmorejob == False
            curpage =  curpage + 1
    def listjob_case_wrongtoken(self):
        ret = self._tm.list_job(token="123456")
        self.assertEquals(ret.get("code"),1)
        self.assertNotEquals(ret.get("msg"),"成功")
    def jobstartbylist_pipline_case(self):
        # list job，然后对第一个已完成的job 重新开始，如没有已完成job assert(false)
        curpage = 1
        hasmorejob = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 6:
                    ret_j = self._tm.job_start(job.get("id"))
                    self.assertEqual(ret_j.get("code"), 0)
                    self.assertIsInstance(ret_j.get("data"), str)
                    finished = False
                    while not finished:
                        jobinfo = self._tm.job_getinfo(jobid=job.get("id"))
                        if jobinfo.get("data").get("queueStatus") == 6 or jobinfo.get("data").get("queueStatus") == 7:
                            finished = True
                        time.sleep(1)
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("queueStatus"), 6)
                    hasmorejob = False
                    break
    # 获取job result，三种状态的job类型
    def jobgetresult_job_success_pipline_case(self):
        curpage = 1
        hasmorejob = True
        nosuccess = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 6:
                    jobinfo = self._tm.get_job_result(jobid=job.get("id"))
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("msg"), "成功")
                    self.assertIsInstance(jobinfo.get("data"),dict)
                    hasmorejob = False
                    nosuccess = False
                    break

        self.assertFalse(nosuccess)

    def jobgetresult_job_failed_pipline_case(self):
        curpage = 1
        hasmorejob = True
        nofailed = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 7:
                    jobinfo = self._tm.get_job_result(jobid=job.get("id"))
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("msg"), "成功")
                    self.assertIsInstance(jobinfo.get("data"),dict)
                    hasmorejob = False
                    nofailed = False
                    break
        self.assertFalse(nofailed)

    def jobgetresult_job_notrun_pipline_case(self):
        curpage = 1
        hasmorejob = True
        norun = True
        while hasmorejob:
            ret = self._tm.list_job(page=curpage)
            self.listjob_check(ret=ret, curpage=curpage)
            for job in ret.get("data").get("data"):
                if job.get("queueStatus") == 0:
                    jobinfo = self._tm.get_job_result(jobid=job.get("id"))
                    self.assertEquals(jobinfo.get("code"), 0)
                    self.assertEquals(jobinfo.get("data").get("msg"), "成功")
                    self.assertIsInstance(jobinfo.get("data"),dict)
                    hasmorejob = False
                    norun = False
                    break
        self.assertFalse(norun)


def tm_init(insite,inuser,inpasswd):
    global site
    site = insite
    global user
    user = inuser
    global passwd
    passwd = inpasswd
    logging.info("%s : %s :%s "%(site,user,passwd))
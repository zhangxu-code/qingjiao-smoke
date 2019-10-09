### PrivPy冒烟测试
* 执行方式
<pre>python3 ./runTestsuit.py --env='env192' --key='smoke'</pre>
* 参数配置，conf.yml中配置测试环境等参数
<pre>env192:
  user: autotest
  passwd: qwer1234
  site: console-dev.tsingj.local
  dbhost: 10.18.0.19
  dbport: 21050</pre>
* 输出 Junit格式的xml测试报告

### 已集成smoke cases
* 6个基准算法测试
* 不同大小数据集任务测试
* 单表查询sql测试

### 下步计划
* jenkins测试报告持久化
* 多表join测试
* 集成privpy_library

### 运行环境
依赖python3.6
<br>第三方库依赖，详见 requirements.txt
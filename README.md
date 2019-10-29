### PrivPy冒烟测试
* 中台smoke执行方式
<pre>python3 ./runTestsuit.py --env='env192' --key='smoke'</pre>
* library执行方式
<pre>python3 runPrivpy-library.py --package=$package --module=$module --env=$env</pre>
* 参数配置，conf.yml中配置测试环境等参数
<br>由于同一个用户只允许一个登录（不支持多连接同时在线），故不同的测试场景区分不同的用户，使用env-key的方式区分
<pre>master-smoke:
  user: autotest
  passwd: qwer1234
  site: console-dev.tsingj.local
  dbhost: 10.18.0.19
  dbport: 21050</pre>
* 输出 Junit格式的xml测试报告
#### privpy-library
privpy-library测试使用算法组提供的测试代码，重构test_util.run_code方法，连接中台下发任务执行测试case

### 已集成smoke cases
* 6个基准算法测试
* 不同大小数据集任务测试
* 单表查询sql测试
* library测试
* 数据持久化

### 下步计划
* xmlrunner重构
* 数据市场case集成
* 测试结果展现页面

### 运行环境
依赖python3.6
<br>第三方库依赖，详见 requirements.txt
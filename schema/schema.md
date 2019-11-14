[toc]
# schema 语法整理

## 类型
### 常用类型
type | 描述
--- | ---
string  | 字符串
number  | 数字
integer | 整型
boolean | 布尔
object  | 对象
array   | 数组

### string 验证关键字
* maxLength 必须是非负整数，标识字符串最大长度
* minLength 必须是非负整数，标识字符串最小长度
* pattern   正则表达式，匹配字符串规则

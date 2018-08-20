
# crawler端
从eastmoney爬数据；错误的话发出告警，通知管理人员！
转换成gson；
通过eventbus发布数据；

# db端
1、通过eventbus接收数据
2、验证数据，错误的话，发出告警，通知管理人员；
3、保存数据

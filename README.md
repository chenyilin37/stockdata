

## 安装说明
1、mvn clean install
2、feature:repo-add mvn:vegoo.newstock/vegoo.stockdata.features/1.0.0-SNAPSHOT/xml/features
3、feature:install stockdata

##查看MySQL数据库各表记录数
select table_name,table_rows from information_schema.tables where TABLE_SCHEMA = 'stockdata' order by table_rows desc;

##
select * from information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA = 'stockdata'  order by TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION

## MAC安装redis
brew services start redis@3.2
redis-server /usr/local/etc/redis.conf

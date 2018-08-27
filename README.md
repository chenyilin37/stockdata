

## 安装说明
1、mvn clean install
2、feature:repo-add mvn:vegoo.newstock/vegoo.stockdata.features/1.0.0-SNAPSHOT/xml/features
3、feature:install stockdata

## 离线安装
1、制作KAR文件
    kar:create vegoo.stockdata.features-1.0.0-SNAPSHOT
2、将KAR文件复制目标安装环境；    
3、安装KAR文件
   kar:install vegoo.stockdata.features-1.0.0-SNAPSHOT
4、feature:install stockdata


##查看MySQL数据库各表记录数
select table_name,table_rows from information_schema.tables where TABLE_SCHEMA = 'stockdata' order by table_rows desc;


## Docker安装redis
docker pull redis
docker run --name redis -p 6379:6379 -v /Users/Shared/redis/data:/data -d redis

redis-cli -h 192.168.1.8 --raw

## 构建Docker镜像
 下载：https://github.com/chenyilin37/redis-udf/blob/master/Dockerfile    
 	docker build -t goas/mysql-with-redis-udf:5.7 .(.不可少)   

	docker login   
	docker push goas/mysql-with-redis-udf:5.7     

## Docker安装MySQL
 docker pull goas/mysql-with-redis-udf:5.7   
  
 docker run -d -p 3306:3306 --privileged=true -v /Users/Shared/mysql/data:/var/lib/mysql \   
	 -e MYSQL_ROOT_PASSWORD=123456 \   
	 -e MYSQL_USER=chenyl \   
	 -e MYSQL_PASSWORD=123456 \   
	 -e REDIS_HOST=192.168.1.8 \   
	 --name macmysql goas/mysql-with-redis-udf:5.7   
 
 docker run -it --link macmysql:mysql --rm goas/mysql-with-redis-udf:5.7 sh -c 'bash'   


## 建立MySQL函数：
    执行以下脚本：    
  DROP FUNCTION IF EXISTS `redis_set`; create function redis_set returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_get`; create function redis_get returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_del`; create function redis_del returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_getset`; create function redis_getset returns string soname 'mysqludf-redis.so';
  DROP FUNCTION IF EXISTS `redis_hset`; create function redis_hset returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_hget`; create function redis_hget returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_hmget`; create function redis_hmget returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_hmset`; create function redis_hmset returns string soname 'mysqludf-redis.so';   
  DROP FUNCTION IF EXISTS `redis_hdel`; create function redis_hdel returns string soname 'mysqludf-redis.so';
  DROP FUNCTION IF EXISTS `redis_sadd`; create function redis_sadd returns string soname 'mysqludf-redis.so';
  DROP FUNCTION IF EXISTS `redis_srem`; create function redis_srem returns string soname 'mysqludf-redis.so';
  DROP FUNCTION IF EXISTS `redis_zadd`; create function redis_zadd returns string soname 'mysqludf-redis.so';
  DROP FUNCTION IF EXISTS `redis_zrem`; create function redis_zrem returns string soname 'mysqludf-redis.so';

## Docker安装Karaf
	docker pull mkroli/karaf   
	
	docker run -d -t --name karaf \   
	  -p 1099:1099 \   
	  -p 8101:8101 \   
	  -p 44444:44444 \   
	  -v /Users/Shared/karaf/deploy:/deploy \   
	  mkroli/karaf   
	
	运行karaf
	  ssh -p8101 karaf@localhost   


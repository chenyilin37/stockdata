
安装Redis、MySQL见redis-udf项目说明！
  https://github.com/chenyilin37/redis-udf/blob/master/README.md

## Docker安装Karaf
### 制作镜像
    docker build -t goas/karaf:4.2.0 .

### 安装Karaf
	docker pull goas/karaf:4.2.0   
	
	docker run -d -t --name mackaraf \   
	  -p 1099:1099 \
	  -p 8101:8101 \
	  -p 44444:44444 \
	  -v /Users/Shared/karaf/deploy:/deploy \
	  --restart=always \
	  goas/karaf:4.2.0   
	
	运行karaf
	  ssh -p8101 karaf@localhost   
	  
	  
## 应用安装说明
### commons
1、mvn clean install
2、feature:repo-add mvn:vegoo.commons/vegoo.commons.features/1.0.0-SNAPSHOT/xml/features
3、feature:install vegoo-commons

### stockdata
1、mvn clean install
2、feature:repo-add mvn:vegoo.stockdata/vegoo.stockdata.features/1.0.0-SNAPSHOT/xml/features
3、feature:install stockdata

### stockserver
1、mvn clean install
2、feature:repo-add mvn:vegoo.stockserver/vegoo.stockserver.features/1.0.0-SNAPSHOT/xml/features
3、feature:install stockserver

## 离线安装
1、制作KAR文件
    kar:create vegoo.stockdata.features-1.0.0-SNAPSHOT
2、将KAR文件复制目标安装环境；    
3、安装KAR文件
   kar:install file:/vegoo.stockdata.features-1.0.0-SNAPSHOT
4、feature:install stockdata


## Q&A

### Failed to obtain JDBC Connection
	java.net.NoRouteToHostException: Can't assign requested address
	原因：使用Karaf数据源，JDBC连接不能自动回收，端口占满导致
	查看动态端口范围，MAC OSX约有15000
	sysctl -a | grep port
	
	临时修改
	sudo sysctl -w net.inet.ip.portrange.hifirst=10240 
	sudo sysctl -w net.inet.ip.portrange.hilast=65500
	sudo sysctl -w net.inet.ip.portrange.first=10240
	sudo sysctl -w net.inet.ip.portrange.last=65500

	永久修改
	sudo nano /etc/sysctl.conf
	把参数写到文件里
	
	kern.maxfiles=1048600
	kern.maxfilesperproc=1048576
	net.inet.ip.portrange.first=10240  
	net.inet.ip.portrange.last=65500
	
	重启系统即可。不要小于1024（那是有root所用），
	查看效果：
	netstat -n | awk '/^tcp/ {++S[$NF]} END {for(a in S) print a, S[a]}'


## 查看MySQL数据库各表记录数
select table_name,table_rows from information_schema.tables where TABLE_SCHEMA = 'stockdata' order by table_rows desc;


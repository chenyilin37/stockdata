DROP TRIGGER IF EXISTS `stock_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `stock_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `stock_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `stock_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `stock_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `stock_INIT_REDIS`;



DELIMITER $

CREATE TRIGGER `stock_AFTER_INSERT` AFTER INSERT ON `stock` FOR EACH ROW
BEGIN
  call stock_UPDATE_REDIS(new.stockCode,new.stockName,new.marketid,new.stockUcode,new.isNew);
END $

CREATE TRIGGER `stock_AFTER_UPDATE` AFTER UPDATE ON `stock` FOR EACH ROW
BEGIN
   call stock_REMOVE_REDIS(old.stockCode,old.stockName,old.marketid,old.stockUcode,old.isNew);
   call stock_UPDATE_REDIS(new.stockCode,new.stockName,new.marketid,new.stockUcode,new.isNew);
END $


CREATE TRIGGER `stock_AFTER_DELETE` AFTER DELETE ON `stock` FOR EACH ROW
BEGIN
  call stock_REMOVE_REDIS(old.stockCode,old.stockName,old.marketid,old.stockUcode,old.isNew);
END $


CREATE PROCEDURE stock_UPDATE_REDIS
(
 in stockCode VARCHAR(10),
 in stockName VARCHAR(45),
 in marketid int,
 in stockUcode VARCHAR(10),
 in isNew int
) 
BEGIN
	 SELECT redis_sadd('stocks', stockCode) INTO @tmp;   

	 SET @tableId = 'stock';

    -- 更新缓存
	 SELECT CONCAT_WS('_', @tableId, stockCode) into @NID;

    -- 字段
	 SELECT redis_hset(@NID, 'stockCode', stockCode) INTO @tmp;   
	 SELECT redis_hset(@NID, 'stockName', stockName) INTO @tmp;   
	 SELECT redis_hset(@NID, 'marketid', CAST(marketid AS CHAR)) INTO @tmp;   
	 SELECT redis_hset(@NID, 'stockUcode', stockUcode) INTO @tmp;   
	 SELECT redis_hset(@NID, 'isNew', CAST(isNew AS CHAR)) INTO @tmp;   

    -- 索引

END $


CREATE PROCEDURE stock_REMOVE_REDIS
(
 in stockCode VARCHAR(10),
 in stockName VARCHAR(45),
 in marketid int,
 in stockUcode VARCHAR(10),
 in isNew int
) 
BEGIN
	SELECT redis_srem('stocks', stockCode) INTO @tmp;   

  SET @tableId = 'stock';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, stockCode)) INTO @tmp ;   

END $



CREATE PROCEDURE `stock_INIT_REDIS`()
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
	 DECLARE stockCode VARCHAR(10);
	 DECLARE stockName VARCHAR(45);
	 DECLARE marketid int;
	 DECLARE stockUcode VARCHAR(10);
	 DECLARE isNew int;
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.stockCode,t.stockName,t.marketid,t.stockUcode,t.isNew
 	  FROM stock t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO stockCode,stockName,marketid,stockUcode,isNew;
		
	   IF done !=1 THEN
	     -- 调用另一个存储过程获取结果
			 CALL stock_UPDATE_REDIS(stockCode,stockName,marketid,stockUcode,isNew);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'stock',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $



DELIMITER ;
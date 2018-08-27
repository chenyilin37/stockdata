DROP TRIGGER IF EXISTS `stocksOfBlock_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `stocksOfBlock_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `stocksOfBlock_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `stocksOfBlock_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `stocksOfBlock_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `stocksOfBlock_INIT_REDIS`;



DELIMITER $

CREATE TRIGGER `stocksOfBlock_AFTER_INSERT` AFTER INSERT ON `stocksOfBlock` FOR EACH ROW
BEGIN
  call stocksOfBlock_UPDATE_REDIS(new.blkUcode,new.stockCode,new.marketid);
END $

CREATE TRIGGER `stocksOfBlock_AFTER_UPDATE` AFTER UPDATE ON `stocksOfBlock` FOR EACH ROW
BEGIN
   call stocksOfBlock_REMOVE_REDIS(old.blkUcode,old.stockCode,old.marketid);
   call stocksOfBlock_UPDATE_REDIS(new.blkUcode,new.stockCode,new.marketid);
END $


CREATE TRIGGER `stocksOfBlock_AFTER_DELETE` AFTER DELETE ON `stocksOfBlock` FOR EACH ROW
BEGIN
  call stocksOfBlock_REMOVE_REDIS(old.blkUcode,old.stockCode,old.marketid);
END $


CREATE PROCEDURE stocksOfBlock_UPDATE_REDIS
(
	in blkUcode VARCHAR(10),
	in stockCode VARCHAR(10),
	in marketid VARCHAR(2)
) 
BEGIN

 	SELECT redis_sadd(CONCAT_WS('_', 'blkstk', blkucode), stockCode) INTO @tmp;   
 	SELECT redis_sadd(CONCAT_WS('_', 'stkblk', stockCode), blkucode) INTO @tmp;   

END $


CREATE PROCEDURE stocksOfBlock_REMOVE_REDIS
(
	in blkUcode VARCHAR(10),
	in stockCode VARCHAR(10),
	in marketid VARCHAR(2)
) 
BEGIN
 	SELECT redis_srem(CONCAT_WS('_', 'blkstk', blkucode), stockCode) INTO @tmp;   
 	SELECT redis_srem(CONCAT_WS('_', 'stkblk', stockCode), blkucode) INTO @tmp;   
END $



CREATE PROCEDURE `stocksOfBlock_INIT_REDIS`()
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  
	DECLARE blkUcode VARCHAR(10);
	DECLARE stockCode VARCHAR(10);
	DECLARE marketid VARCHAR(2);
   
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.blkUcode, t.stockCode, t.marketid
 	  FROM stocksOfBlock t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO blkUcode, stockCode, marketid;
		
	   IF done !=1 THEN
	     -- 调用另一个存储过程获取结果
			 CALL stocksOfBlock_UPDATE_REDIS(blkUcode, stockCode, marketid);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'stocksOfBlock',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $



DELIMITER ;
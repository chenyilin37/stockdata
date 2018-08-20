DROP TRIGGER IF EXISTS `block_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `block_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `block_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `block_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `block_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `block_INIT_REDIS`;



DELIMITER $

CREATE TRIGGER `block_AFTER_INSERT` AFTER INSERT ON `block` FOR EACH ROW
BEGIN
  call block_UPDATE_REDIS(new.blkucode, new.marketid, new.blkcode, new.blkname, new.blktype);
END $

CREATE TRIGGER `block_AFTER_UPDATE` AFTER UPDATE ON `block` FOR EACH ROW
BEGIN
   call block_REMOVE_REDIS(old.blkucode, old.marketid, old.blkcode, old.blkname, old.blktype);
   call block_UPDATE_REDIS(new.blkucode, new.marketid, new.blkcode, new.blkname, new.blktype);
END $


CREATE TRIGGER `block_AFTER_DELETE` AFTER DELETE ON `block` FOR EACH ROW
BEGIN
  call block_REMOVE_REDIS(old.blkucode, old.marketid, old.blkcode, old.blkname, old.blktype);
END $


CREATE PROCEDURE block_UPDATE_REDIS
(
	in blkucode VARCHAR(10), 
	in marketid VARCHAR(1), 
	in blkcode VARCHAR(10), 
	in blkname VARCHAR(45), 
	in blktype VARCHAR(10)
) 
BEGIN
	SELECT redis_sadd('blocks', blkucode) INTO @tmp;   

	SET @tableId = 'block';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, blkucode) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'blkucode', blkucode) INTO @tmp;   
	SELECT redis_hset(@NID, 'marketid', marketid) INTO @tmp;   
	SELECT redis_hset(@NID, 'blkcode', blkcode) INTO @tmp;   
	SELECT redis_hset(@NID, 'blkname', blkname) INTO @tmp;   
	SELECT redis_hset(@NID, 'blktype', blktype) INTO @tmp;   

    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, blkcode), blkucode) INTO @tmp ;

END $


CREATE PROCEDURE block_REMOVE_REDIS
(
	in blkucode VARCHAR(10), 
	in marketid VARCHAR(1), 
	in blkcode VARCHAR(10), 
	in blkname VARCHAR(45), 
	in blktype VARCHAR(10)
) 
BEGIN
	SELECT redis_srem('blocks', blkucode) INTO @tmp;   

  SET @tableId = 'block';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, blkucode)) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, blkcode)) INTO @tmp ;

END $



CREATE PROCEDURE `block_INIT_REDIS`()
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
   DECLARE done boolean default 0;
  
   
	DECLARE blkucode VARCHAR(10); 
	DECLARE marketid VARCHAR(1); 
	DECLARE blkcode VARCHAR(10); 
	DECLARE blkname VARCHAR(45); 
	DECLARE blktype VARCHAR(10);
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.blkucode, t.marketid, t.blkcode, t.blkname, t.blktype
 	  FROM block t;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO blkucode, marketid, blkcode, blkname, blktype;
		
	   IF done !=1 THEN
	     -- 调用另一个存储过程获取结果
			 CALL block_UPDATE_REDIS(blkucode, marketid, blkcode, blkname, blktype);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'block',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $



DELIMITER ;
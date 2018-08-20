DROP TRIGGER IF EXISTS `kdaydata_AFTER_INSERT`;
DROP TRIGGER IF EXISTS `kdaydata_AFTER_UPDATE`;
DROP TRIGGER IF EXISTS `kdaydata_AFTER_DELETE`;

DROP PROCEDURE IF EXISTS `kdaydata_UPDATE_REDIS`;
DROP PROCEDURE IF EXISTS `kdaydata_REMOVE_REDIS`;
DROP PROCEDURE IF EXISTS `kdaydata_INIT_REDIS`;
DROP PROCEDURE IF EXISTS `kdaydata_INIT_BYDATE`;


DELIMITER $

CREATE TRIGGER `kdaydata_AFTER_INSERT` AFTER INSERT ON `kdaydata` FOR EACH ROW
BEGIN
  call kdaydata_UPDATE_REDIS( new.id,
															new.SCode,
															new.transDate,
															new.open,
															new.close,
															new.high,
															new.low,
															new.volume,
															new.amount,
															new.LClose,
															new.changeRate,
															new.amplitude,
															new.turnoverRate);
END $

CREATE TRIGGER `kdaydata_AFTER_UPDATE` AFTER UPDATE ON `kdaydata` FOR EACH ROW
BEGIN
   call kdaydata_REMOVE_REDIS(old.id,
															old.SCode,
															old.transDate,
															old.open,
															old.close,
															old.high,
															old.low,
															old.volume,
															old.amount,
															old.LClose,
															old.changeRate,
															old.amplitude,
															old.turnoverRate);
	
   call kdaydata_UPDATE_REDIS(new.id,
															new.SCode,
															new.transDate,
															new.open,
															new.close,
															new.high,
															new.low,
															new.volume,
															new.amount,
															new.LClose,
															new.changeRate,
															new.amplitude,
															new.turnoverRate);
END $


CREATE TRIGGER `kdaydata_AFTER_DELETE` AFTER DELETE ON `kdaydata` FOR EACH ROW
BEGIN
  call kdaydata_REMOVE_REDIS( old.id,
															old.SCode,
															old.transDate,
															old.open,
															old.close,
															old.high,
															old.low,
															old.volume,
															old.amount,
															old.LClose,
															old.changeRate,
															old.amplitude,
															old.turnoverRate);
END $


CREATE PROCEDURE kdaydata_UPDATE_REDIS
(
  in id int,
	in SCode VARCHAR(10),
	in transDate DATE,
	in open DECIMAL(10,2),
	in close DECIMAL(10,2),
	in high DECIMAL(10,2),
	in low DECIMAL(10,2),
	in volume DECIMAL(13,0),
	in amount DECIMAL(15,2),
	in LClose DECIMAL(10,2),
	in changeRate DECIMAL(10,2),
	in amplitude DECIMAL(10,2),
	in turnoverRate DECIMAL(10,2)
) 
BEGIN
	SET @tableId = 'kdaydata';

    -- 更新缓存
	SELECT CONCAT_WS('_', @tableId, CAST(id AS CHAR)) into @NID;

    -- 字段
	SELECT redis_hset(@NID, 'id', CAST(id AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'SCode', SCode) INTO @tmp;   
	SELECT redis_hset(@NID, 'transDate', DATE_FORMAT(transDate, '%Y-%m-%d') )  INTO @tmp;    
	SELECT redis_hset(@NID, 'open', CAST(open AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'close', CAST(close AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'high', CAST(high AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'low', CAST(low AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'volume', CAST(volume AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'amount', CAST(amount AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'LClose', CAST(LClose AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'changeRate', CAST(changeRate AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'amplitude', CAST(amplitude AS CHAR)) INTO @tmp;   
	SELECT redis_hset(@NID, 'turnoverRate', CAST(turnoverRate AS CHAR)) INTO @tmp;   


    -- 索引
	SELECT redis_set( CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(transDate, '%Y-%m-%d')), CAST(id AS CHAR)) INTO @tmp ;
	SELECT redis_set( CONCAT_WS('_', @tableId, DATE_FORMAT(transDate, '%Y-%m-%d'), SCode), CAST(id AS CHAR)) INTO @tmp ;

END $


CREATE PROCEDURE kdaydata_REMOVE_REDIS
(
  in id int,
	in SCode VARCHAR(10),
	in transDate DATE,
	in open DECIMAL(10,2),
	in close DECIMAL(10,2),
	in high DECIMAL(10,2),
	in low DECIMAL(10,2),
	in volume DECIMAL(13,0),
	in amount DECIMAL(15,2),
	in LClose DECIMAL(10,2),
	in changeRate DECIMAL(10,2),
	in amplitude DECIMAL(10,2),
	in turnoverRate DECIMAL(10,2)
) 
BEGIN
  SET @tableId = 'kdaydata';
	
  SELECT redis_del(CONCAT_WS('_', @tableId, CAST(id AS CHAR))) INTO @tmp ;   
  SELECT redis_del(CONCAT_WS('_', @tableId, SCode, DATE_FORMAT(transDate, '%Y-%m-%d'))) INTO @tmp ;
  SELECT redis_del(CONCAT_WS('_', @tableId, DATE_FORMAT(transDate, '%Y-%m-%d'), SCode)) INTO @tmp ;

END $

CREATE PROCEDURE `kdaydata_INIT_REDIS`(
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  DECLARE mRDate DATE;
	
	DECLARE records CURSOR
	FOR
		SELECT distinct t.transDate FROM kdaydata t where t.transDate is not null;
		
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    
	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO mRDate;
		
	   IF done != 1 THEN
	     -- 调用另一个存储过程获取结果
		 	 CALL kdaydata_INIT_BYDATE(mRDate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	-- 在Redis中，表示该表已经缓存；日期时间数据用T分隔，用空格无法缓存
	SELECT redis_hset('CACHED_TABLES', 'kdaydata',  DATE_FORMAT(NOW(), '%Y-%m-%d %T')) INTO @tmp;  		 
	
END $


CREATE PROCEDURE `kdaydata_INIT_BYDATE`(
	in mRDate Date
)
    COMMENT '将表中的数据全部同步到Redis'
BEGIN
	 -- 声明局部变量
  DECLARE done boolean default 0;
  
   
  DECLARE id int;
	DECLARE SCode VARCHAR(10);
	DECLARE transDate DATE;
	DECLARE open DECIMAL(10,2);
	DECLARE close DECIMAL(10,2);
	DECLARE high DECIMAL(10,2);
	DECLARE low DECIMAL(10,2);
	DECLARE volume DECIMAL(13,0);
	DECLARE amount DECIMAL(15,2);
	DECLARE LClose DECIMAL(10,2);
	DECLARE changeRate DECIMAL(10,2);
	DECLARE amplitude DECIMAL(10,2);
	DECLARE turnoverRate DECIMAL(10,2)
    
	-- 声明游标
	-- cursor定义时,如果取出列中包含主键id，必须为表定义别名，否则fetch出值为0；非主键列未发现此问题。特此记录以备忘~ 
	DECLARE records CURSOR
	FOR
		SELECT t.id,t.SCode,t.transDate,t.open,t.close,t.high,t.low,
					 t.volume,t.amount,t.LClose,t.changeRate,t.amplitude,t.turnoverRate
    FROM kdaydata t where  t.transDate=mRDate;
	
	-- 声明循环结束条件
	DECLARE continue handler for not FOUND  SET done=1; 
	
    -- 打开游标
	OPEN records;
    

	-- 循环所有行
	REPEAT
		 -- 获得当前循环的数据
		 FETCH records INTO id,SCode,transDate,open,close,high,low,volume,amount,LClose,changeRate,amplitude,turnoverRate;
		
	   IF done !=1 THEN
	    -- 调用另一个存储过程获取结果
			CALL kdaydata_UPDATE_REDIS(id,SCode,transDate,open,close,high,low,volume,amount,LClose,changeRate,amplitude,turnoverRate);
	   END IF;

	-- 结束循环
	UNTIL done END REPEAT;
	
    -- 关闭游标
	CLOSE records;
	
	
END $



DELIMITER ;